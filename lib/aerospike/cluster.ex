defmodule Aerospike.Cluster do
  @moduledoc """
  Read-side helpers over published cluster state.

  `ready?/1`, routing, retry policy, and active-node helpers read the
  cluster's ETS tables directly when the caller passes the cluster atom
  or a pid registered under that atom. Transport sockets, tend-cycle state,
  and node handles remain implementation details. `warm_up/2` is the one
  explicit operator helper this module aggregates over the published
  active-node view.
  """

  alias Aerospike.Cluster.NodePool
  alias Aerospike.Cluster.Router
  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.RetryPolicy

  @typedoc """
  Cluster identity accepted by read-side helpers.

  Pass the cluster registration atom used at startup, or the pid of a
  process registered under that atom. Unregistered pids raise
  `ArgumentError`.
  """
  @type cluster :: atom() | pid()

  @typedoc """
  ETS table names published for one cluster runtime.

  These names are derived from the cluster identity and are used by the
  read-side helpers to inspect partition ownership, node generation,
  runtime metadata, and transaction tracking state.
  """
  @type tables :: %{
          owners: atom(),
          node_gens: atom(),
          meta: atom(),
          txn_tracking: atom()
        }

  @typedoc """
  Replica selection policy used by read routing.

  `:master` always routes to the partition master. `:sequence` walks the
  available replica list using the retry attempt index.
  """
  @type replica_policy :: :master | :sequence

  @typedoc """
  Result returned by cluster routing helpers.

  Successful routes return the selected node name. `:cluster_not_ready`
  means the namespace has no complete partition map yet; `:no_master`
  means the map is present but the target partition has no eligible owner.
  """
  @type route_result :: {:ok, String.t()} | {:error, :cluster_not_ready | :no_master}

  @typedoc """
  Active node metadata returned by `nodes/1`.

  The host and port are the direct-connect endpoint captured from the
  Tender's current node handle. Unknown handles are represented with an
  empty host and port `0`.
  """
  @type node_info :: %{name: String.t(), host: String.t(), port: :inet.port_number()}

  @typedoc """
  Warm-up option accepted by `warm_up/2`.

    * `:count` — number of connections to check out per active node.
      `0` or omitted means the configured pool size. Values above the
      pool size are capped to the pool size.
    * `:pool_checkout_timeout` — timeout in milliseconds for each
      individual pool checkout. Defaults to `5_000`.
  """
  @type warm_up_option ::
          {:count, non_neg_integer()}
          | {:pool_checkout_timeout, non_neg_integer()}

  @typedoc """
  Per-node warm-up result returned inside `t:warm_up_result/0`.

  `:status` is `:ok`, `:partial`, or `:error`. `:requested` is the capped
  per-node target and `:warmed` is the number of successful checkouts.
  `:error` is `nil` on full success or the checkout failure returned by
  the pool.
  """
  @type warm_up_node_result :: %{
          required(:host) => String.t(),
          required(:port) => :inet.port_number() | 0,
          required(:status) => :ok | :partial | :error,
          required(:requested) => non_neg_integer(),
          required(:warmed) => non_neg_integer(),
          required(:error) => Error.t() | nil
        }

  @typedoc """
  Aggregate warm-up report returned by `warm_up/2`.

  The `:nodes` map is keyed by node name and contains one
  `t:warm_up_node_result/0` per active node.
  """
  @type warm_up_result :: %{
          status: :ok | :partial | :error,
          requested_per_node: non_neg_integer(),
          total_requested: non_neg_integer(),
          total_warmed: non_neg_integer(),
          nodes_total: non_neg_integer(),
          nodes_ok: non_neg_integer(),
          nodes_partial: non_neg_integer(),
          nodes_error: non_neg_integer(),
          nodes: %{String.t() => warm_up_node_result()}
        }

  @doc """
  Returns the published ETS tables for `cluster`.
  """
  @spec tables(cluster()) :: tables()
  def tables(cluster)

  def tables(cluster) when is_atom(cluster) do
    %{
      owners: :"#{cluster}_partition_map_owners",
      node_gens: :"#{cluster}_partition_map_node_gens",
      meta: :"#{cluster}_meta",
      txn_tracking: :"#{cluster}_txn_tracking"
    }
  end

  def tables(cluster) when is_pid(cluster), do: cluster |> registered_name!() |> tables()

  def tables(_cluster),
    do: raise(ArgumentError, "cluster identity must be an atom or a pid registered under one")

  @doc """
  Returns whether every configured namespace has a complete partition map.
  """
  @spec ready?(cluster()) :: boolean()
  def ready?(cluster) do
    cluster
    |> tables()
    |> Map.fetch!(:meta)
    |> read_meta(:ready, false)
  end

  @doc """
  Returns the cluster-default retry policy published in `meta.:retry_opts`.
  """
  @spec retry_policy(cluster()) :: RetryPolicy.t()
  def retry_policy(cluster) do
    cluster
    |> tables()
    |> Map.fetch!(:meta)
    |> load_retry_policy()
  end

  @doc """
  Returns the published active node-name snapshot.
  """
  @spec active_nodes(cluster()) :: [String.t()]
  def active_nodes(cluster) do
    cluster
    |> tables()
    |> Map.fetch!(:meta)
    |> read_meta(:active_nodes, [])
  end

  @doc """
  Returns whether `node_name` appears in the published active-node snapshot.
  """
  @spec active_node?(cluster(), String.t()) :: boolean()
  def active_node?(cluster, node_name) when is_binary(node_name) do
    node_name in active_nodes(cluster)
  end

  @doc """
  Returns the published active node-name snapshot.
  """
  @spec node_names(cluster()) :: [String.t()]
  def node_names(cluster) do
    active_nodes(cluster)
  end

  @doc """
  Returns the published active nodes with their direct-connect host and port.
  """
  @spec nodes(cluster()) :: [node_info()]
  def nodes(cluster) do
    cluster
    |> node_names()
    |> Enum.map(&node_info(cluster, &1))
  end

  @doc """
  Verifies that the active node pools can serve checkouts through the normal path.

  `:count` defaults to the configured pool size for the cluster and is capped
  at that size. `:pool_checkout_timeout` controls each checkout attempt.
  """
  @spec warm_up(cluster(), [warm_up_option()]) :: {:ok, warm_up_result()} | {:error, Error.t()}
  def warm_up(cluster, opts \\ []) when is_list(opts) do
    with true <- ready?(cluster) || {:error, Error.from_result_code(:cluster_not_ready)},
         {:ok, pool_size} <- configured_pool_size(cluster),
         {:ok, requested_count} <-
           normalize_warm_up_count(Keyword.get(opts, :count, 0), pool_size),
         node_names when is_list(node_names) and node_names != [] <- node_names(cluster) do
      {:ok,
       warm_up_result(
         cluster,
         node_names,
         requested_count,
         Keyword.get(opts, :pool_checkout_timeout, 5_000)
       )}
    else
      [] -> {:error, Error.from_result_code(:cluster_not_ready)}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  @doc """
  Routes a read for `key` under `policy` and `attempt`.
  """
  @spec route_for_read(cluster(), Key.t(), replica_policy(), non_neg_integer()) :: route_result()
  def route_for_read(cluster, %Key{} = key, policy, attempt)
      when policy in [:master, :sequence] and is_integer(attempt) and attempt >= 0 do
    Router.pick_for_read(tables(cluster), key, policy, attempt)
  end

  @doc """
  Routes a write for `key` to its master node.
  """
  @spec route_for_write(cluster(), Key.t()) :: route_result()
  def route_for_write(cluster, %Key{} = key) do
    Router.pick_for_write(tables(cluster), key)
  end

  defp registered_name!(cluster) when is_pid(cluster) do
    case Process.info(cluster, :registered_name) do
      {:registered_name, name} when is_atom(name) -> name
      _ -> raise ArgumentError, "cluster identity must be an atom or a pid registered under one"
    end
  end

  defp node_info(cluster, node_name) do
    case Tender.node_handle(cluster, node_name) do
      {:ok, %{host: host, port: port}} ->
        %{name: node_name, host: host, port: port}

      {:error, :unknown_node} ->
        %{name: node_name, host: "", port: 0}
    end
  end

  defp configured_pool_size(cluster) do
    case :ets.lookup(tables(cluster).meta, {:runtime, :config, :pool_size}) do
      [{{:runtime, :config, :pool_size}, pool_size}]
      when is_integer(pool_size) and pool_size > 0 ->
        {:ok, pool_size}

      _ ->
        {:error, Error.from_result_code(:cluster_not_ready)}
    end
  catch
    :error, :badarg ->
      {:error, Error.from_result_code(:cluster_not_ready)}
  end

  defp normalize_warm_up_count(count, pool_size) when is_integer(count) and count >= 0 do
    {:ok, if(count == 0, do: pool_size, else: min(count, pool_size))}
  end

  defp normalize_warm_up_count(other, _pool_size) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: ":count must be a non-negative integer, got: #{inspect(other)}"
     )}
  end

  defp warm_up_result(cluster, node_names, requested_count, checkout_timeout) do
    node_results =
      Map.new(node_names, fn node_name ->
        warm_up_node(cluster, node_name, requested_count, checkout_timeout)
      end)

    nodes_total = map_size(node_results)
    total_requested = requested_count * nodes_total
    total_warmed = Enum.reduce(node_results, 0, &sum_warmed_connections/2)
    nodes_ok = count_node_results(node_results, :ok)
    nodes_partial = count_node_results(node_results, :partial)
    nodes_error = count_node_results(node_results, :error)

    %{
      status: warm_up_status(nodes_ok, nodes_partial, nodes_error),
      requested_per_node: requested_count,
      total_requested: total_requested,
      total_warmed: total_warmed,
      nodes_total: nodes_total,
      nodes_ok: nodes_ok,
      nodes_partial: nodes_partial,
      nodes_error: nodes_error,
      nodes: node_results
    }
  end

  defp warm_up_node(cluster, node_name, requested_count, checkout_timeout) do
    case Tender.node_handle(cluster, node_name) do
      {:ok, %{pool: pool, host: host, port: port}} ->
        case NodePool.warm_up(pool, requested_count, checkout_timeout) do
          {:ok, warmed} ->
            {node_name,
             %{
               host: host,
               port: port,
               requested: requested_count,
               warmed: warmed,
               status: :ok,
               error: nil
             }}

          {:error, %Error{} = error, warmed} ->
            {node_name,
             %{
               host: host,
               port: port,
               requested: requested_count,
               warmed: warmed,
               status: warm_up_node_status(warmed),
               error: error
             }}
        end

      {:error, :unknown_node} ->
        {node_name,
         %{
           host: "",
           port: 0,
           requested: requested_count,
           warmed: 0,
           status: :error,
           error: Error.from_result_code(:invalid_node, node: node_name)
         }}
    end
  end

  defp warm_up_node_status(0), do: :error
  defp warm_up_node_status(_warmed), do: :partial

  defp sum_warmed_connections({_node_name, %{warmed: warmed}}, acc), do: acc + warmed

  defp count_node_results(node_results, status) do
    Enum.count(node_results, fn {_node_name, result} -> result.status == status end)
  end

  defp warm_up_status(nodes_ok, 0, 0) when nodes_ok > 0, do: :ok
  defp warm_up_status(0, 0, nodes_error) when nodes_error > 0, do: :error
  defp warm_up_status(_nodes_ok, _nodes_partial, _nodes_error), do: :partial

  defp read_meta(meta_tab, key, default) do
    case :ets.lookup(meta_tab, key) do
      [{^key, value}] -> value
      _ -> default
    end
  catch
    :error, :badarg -> default
  end

  defp load_retry_policy(meta_tab) do
    RetryPolicy.load(meta_tab)
  catch
    :error, :badarg -> RetryPolicy.defaults()
  end
end
