defmodule Aerospike.Cluster.PartitionMapWriter do
  @moduledoc false

  use GenServer

  alias Aerospike.Cluster.PartitionMap
  alias Aerospike.Cluster.PartitionMapMerge
  alias Aerospike.RetryPolicy

  @type tables :: %{
          optional(atom()) => atom(),
          owners: atom(),
          node_gens: atom(),
          meta: atom()
        }

  @type option :: {:name, atom()} | {:tables, tables()}

  @doc false
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @doc """
  Starts the writer.

  Options:

    * `:name` — cluster name atom. The writer registers itself under
      `via/1`.
    * `:tables` — `%{owners: atom(), node_gens: atom(), meta: atom()}`
      map of ETS table names as returned by
      `Aerospike.Cluster.TableOwner.tables/1`.
  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: via(name))
  end

  @doc """
  Returns the registered name atom used by `start_link/1` for `name`.
  """
  @spec via(atom()) :: atom()
  def via(name) when is_atom(name), do: :"#{name}_partition_map_writer"

  @doc """
  Applies `segments` reported by `node_name` against the `owners` table
  via `Aerospike.Cluster.PartitionMapMerge.apply_segments/4`.

  Returns `true` when every in-scope segment was accepted by the regime
  guard, `false` when any segment was rejected as stale. The caller uses
  that boolean to decide whether to advance the node's `applied_gen`.
  """
  @spec apply_segments(
          GenServer.server(),
          PartitionMap.node_name(),
          [PartitionMapMerge.segment()],
          [PartitionMap.namespace()]
        ) :: boolean()
  def apply_segments(server, node_name, segments, namespaces)
      when is_binary(node_name) and is_list(segments) and is_list(namespaces) do
    GenServer.call(server, {:apply_segments, node_name, segments, namespaces})
  end

  @doc """
  Stores the latest `partition-generation` value for `node_name`.
  """
  @spec put_node_gen(GenServer.server(), PartitionMap.node_name(), non_neg_integer()) :: :ok
  def put_node_gen(server, node_name, gen)
      when is_binary(node_name) and is_integer(gen) and gen >= 0 do
    GenServer.call(server, {:put_node_gen, node_name, gen})
  end

  @doc """
  Removes every `owners` entry that lists `node_name` as a replica.
  """
  @spec drop_node(GenServer.server(), PartitionMap.node_name()) :: :ok
  def drop_node(server, node_name) when is_binary(node_name) do
    GenServer.call(server, {:drop_node, node_name})
  end

  @doc """
  Removes the per-node `partition-generation` entry for `node_name`.
  """
  @spec delete_node_gen(GenServer.server(), PartitionMap.node_name()) :: :ok
  def delete_node_gen(server, node_name) when is_binary(node_name) do
    GenServer.call(server, {:delete_node_gen, node_name})
  end

  @doc """
  Recomputes the `meta.:ready` flag from the current `owners` table.

  `:ready` flips to `true` only when every configured namespace has a
  complete partition map (`PartitionMap.complete?/2`). Returns the new
  ready value so callers can cache it without re-reading the table.
  """
  @spec recompute_ready(GenServer.server(), [PartitionMap.namespace(), ...]) :: boolean()
  def recompute_ready(server, namespaces) when is_list(namespaces) and namespaces != [] do
    GenServer.call(server, {:recompute_ready, namespaces})
  end

  @doc """
  Publishes the cluster-default retry policy to `meta.:retry_opts`.
  """
  @spec publish_retry_policy(GenServer.server(), RetryPolicy.t()) :: :ok
  def publish_retry_policy(
        server,
        %{
          max_retries: _,
          sleep_between_retries_ms: _,
          replica_policy: _
        } = policy
      ) do
    GenServer.call(server, {:publish_retry_policy, policy})
  end

  @doc """
  Publishes the sorted active node-name snapshot to `meta.:active_nodes`.
  """
  @spec publish_active_nodes(GenServer.server(), [PartitionMap.node_name()]) :: :ok
  def publish_active_nodes(server, node_names) when is_list(node_names) do
    GenServer.call(server, {:publish_active_nodes, node_names})
  end

  @impl GenServer
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    tables = Keyword.fetch!(opts, :tables)

    is_atom(name) or
      raise ArgumentError, "Aerospike.Cluster.PartitionMapWriter: :name must be an atom"

    %{owners: owners_tab, node_gens: node_gens_tab, meta: meta_tab} = tables

    {:ok,
     %{
       name: name,
       owners_tab: owners_tab,
       node_gens_tab: node_gens_tab,
       meta_tab: meta_tab
     }}
  end

  @impl GenServer
  def handle_call({:apply_segments, node_name, segments, namespaces}, _from, state) do
    applied? = PartitionMapMerge.apply_segments(state.owners_tab, node_name, segments, namespaces)
    {:reply, applied?, state}
  end

  def handle_call({:put_node_gen, node_name, gen}, _from, state) do
    :ok = PartitionMap.put_node_gen(state.node_gens_tab, node_name, gen)
    {:reply, :ok, state}
  end

  def handle_call({:drop_node, node_name}, _from, state) do
    :ok = PartitionMap.drop_node(state.owners_tab, node_name)
    {:reply, :ok, state}
  end

  def handle_call({:delete_node_gen, node_name}, _from, state) do
    :ok = PartitionMap.delete_node_gen(state.node_gens_tab, node_name)
    {:reply, :ok, state}
  end

  def handle_call({:recompute_ready, namespaces}, _from, state) do
    ready? =
      Enum.all?(namespaces, fn namespace ->
        PartitionMap.complete?(state.owners_tab, namespace)
      end)

    :ets.insert(state.meta_tab, {:ready, ready?})
    {:reply, ready?, state}
  end

  def handle_call({:publish_retry_policy, policy}, _from, state) do
    true = RetryPolicy.put(state.meta_tab, policy)
    {:reply, :ok, state}
  end

  def handle_call({:publish_active_nodes, node_names}, _from, state) do
    snapshot =
      node_names
      |> Enum.uniq()
      |> Enum.sort()

    :ets.insert(state.meta_tab, {:active_nodes, snapshot})
    {:reply, :ok, state}
  end
end
