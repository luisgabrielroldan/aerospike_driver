defmodule Aerospike.Router do
  @moduledoc false
  # Routes a wire-encoded request to the correct node based on the key's partition.
  #
  # Flow: key → partition_id → ETS partition table → node_name → ETS nodes table
  # → pool_pid → NimblePool.checkout! → send wire + receive response.

  require Logger

  alias Aerospike.Connection
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Tables

  @doc """
  Resolves routing for `key`, checks out a pooled connection, sends `wire`, and returns the AS_MSG body.

  Returns `{:ok, body, node_name}` on success or `{:error, %Aerospike.Error{}}`.

  `node_name` is the Aerospike cluster node string (e.g. from the `node` info field).
  """
  @spec run(atom(), Key.t(), iodata(), keyword()) ::
          {:ok, binary(), String.t()} | {:error, Error.t()}
  def run(conn_name, %Key{} = key, wire, opts \\ []) when is_atom(conn_name) do
    ro = routing_opts(opts)
    checkout_timeout = Keyword.get(ro, :pool_checkout_timeout, 5_000)
    replica_index = Keyword.get(ro, :replica_index, 0)

    with :ok <- check_ready(conn_name),
         {:ok, pool_pid, node_name} <- resolve_pool(conn_name, key, replica_index),
         {:ok, body} <- checkout_and_request(pool_pid, wire, checkout_timeout) do
      {:ok, body, node_name}
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, other} ->
        {:error, Error.from_result_code(:parse_error, message: inspect(other))}
    end
  end

  @doc """
  Groups `keys` by the cluster node that owns each key's partition (for batch fan-out).

  Returns `{:ok, %{node_name => [{original_index, key}, ...]}}` preserving list order
  via `original_index` (0-based), or `{:error, %Aerospike.Error{}}`.
  """
  @typedoc """
  Keys grouped per node with the `NimblePool` pid used to reach that node.
  """
  @type node_batch_group :: %{
          pool_pid: pid(),
          entries: [{non_neg_integer(), Key.t()}]
        }

  @spec group_by_node(atom(), [Key.t()], keyword()) ::
          {:ok, %{String.t() => node_batch_group()}} | {:error, Error.t()}
  def group_by_node(conn_name, keys, opts \\ []) when is_atom(conn_name) and is_list(keys) do
    replica_index = Keyword.get(routing_opts(opts), :replica_index, 0)

    with :ok <- check_ready(conn_name),
         {:ok, acc} <- fold_keys_into_node_groups(conn_name, keys, replica_index) do
      {:ok, reverse_group_entries(acc)}
    end
  end

  defp fold_keys_into_node_groups(conn_name, keys, replica_index) do
    keys
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, %{}}, fn {key, idx}, {:ok, acc} ->
      case resolve_pool(conn_name, key, replica_index) do
        {:ok, pool_pid, node_name} ->
          {:cont, {:ok, insert_batch_group_entry(acc, node_name, pool_pid, {idx, key})}}

        {:error, _} = err ->
          {:halt, err}
      end
    end)
  end

  defp insert_batch_group_entry(acc, node_name, pool_pid, entry) do
    Map.update(acc, node_name, %{pool_pid: pool_pid, entries: [entry]}, fn group ->
      %{group | entries: [entry | group.entries]}
    end)
  end

  defp reverse_group_entries(acc) do
    Map.new(acc, fn {node, %{pool_pid: p, entries: es}} ->
      {node, %{pool_pid: p, entries: Enum.reverse(es)}}
    end)
  end

  @doc """
  Checks out a connection from `pool_pid`, sends `wire`, reads one response body.

  For single-record commands (put, get, delete, exists, touch, operate).
  """
  @spec checkout_and_request(pid(), iodata(), non_neg_integer()) ::
          {:ok, binary()} | {:error, Error.t()}
  def checkout_and_request(pool_pid, wire, checkout_timeout)
      when is_pid(pool_pid) and is_integer(checkout_timeout) and checkout_timeout >= 0 do
    do_checkout(pool_pid, checkout_timeout, fn conn ->
      case Connection.request(conn, wire) do
        {:ok, conn2, _v, _t, body} ->
          {{:ok, body}, conn2}

        {:error, reason} ->
          e = Error.from_result_code(:network_error, message: inspect(reason))
          {{:error, e}, :close}
      end
    end)
  end

  @doc """
  Like `checkout_and_request/3` but reads a multi-frame (streaming) response.

  Batch, scan, and query commands return multiple proto frames per request.
  All frames are read until the INFO3_LAST sentinel and concatenated.
  """
  @spec checkout_and_request_stream(pid(), iodata(), non_neg_integer()) ::
          {:ok, binary()} | {:error, Error.t()}
  def checkout_and_request_stream(pool_pid, wire, checkout_timeout)
      when is_pid(pool_pid) and is_integer(checkout_timeout) and checkout_timeout >= 0 do
    do_checkout(pool_pid, checkout_timeout, fn conn ->
      case Connection.request_stream(conn, wire) do
        {:ok, conn2, body} ->
          {{:ok, body}, conn2}

        {:error, reason} ->
          e = Error.from_result_code(:network_error, message: inspect(reason))
          {{:error, e}, :close}
      end
    end)
  end

  defp do_checkout(pool_pid, checkout_timeout, fun) do
    NimblePool.checkout!(
      pool_pid,
      :checkout,
      fn _from, conn -> fun.(conn) end,
      checkout_timeout
    )
  catch
    :exit, {:timeout, {NimblePool, :checkout, _}} ->
      {:error, Error.from_result_code(:pool_timeout)}

    :exit, {:noproc, {NimblePool, :checkout, _}} ->
      {:error, Error.from_result_code(:invalid_node)}

    :exit, reason ->
      {:error, Error.from_result_code(:network_error, message: inspect(reason))}
  end

  defp routing_opts(opts) when is_list(opts) do
    base = Keyword.take(opts, [:pool_checkout_timeout])
    ri = Keyword.get(opts, :replica_index)

    if is_integer(ri) and ri >= 0 do
      Keyword.put(base, :replica_index, ri)
    else
      put_replica_index(base, Keyword.get(opts, :replica))
    end
  end

  defp put_replica_index(kw, nil), do: kw

  defp put_replica_index(kw, r) when is_integer(r) and r >= 0 do
    Keyword.put(kw, :replica_index, r)
  end

  defp put_replica_index(kw, a) when a in [:master, :sequence, :any] do
    Keyword.put(kw, :replica_index, replica_atom_to_index(a))
  end

  defp replica_atom_to_index(:master), do: 0

  defp replica_atom_to_index(:sequence) do
    Logger.warning("replica: :sequence not yet implemented, routing to master")
    0
  end

  defp replica_atom_to_index(:any) do
    Logger.warning("replica: :any not yet implemented, routing to master")
    0
  end

  # Guards against requests before the cluster has finished its initial tend.
  defp check_ready(name) do
    case :ets.lookup(Tables.meta(name), Tables.ready_key()) do
      [{_, true}] -> :ok
      _ -> {:error, Error.from_result_code(:cluster_not_ready)}
    end
  end

  # Looks up which node owns {namespace, partition_id, replica_index} in the
  # partitions ETS table, then finds that node's pool PID.
  defp resolve_pool(name, key, replica_index) do
    partition_id = Key.partition_id(key)
    ns = key.namespace

    case :ets.lookup(Tables.partitions(name), {ns, partition_id, replica_index}) do
      [] ->
        {:error, Error.from_result_code(:invalid_cluster_partition_map)}

      [{_, node_name}] ->
        lookup_pool(name, node_name)
    end
  end

  defp lookup_pool(name, node_name) do
    case :ets.lookup(Tables.nodes(name), node_name) do
      [] ->
        {:error, Error.from_result_code(:invalid_node)}

      [{_, %{pool_pid: pid}}] when is_pid(pid) ->
        {:ok, pid, node_name}

      _ ->
        {:error, Error.from_result_code(:invalid_node)}
    end
  end
end
