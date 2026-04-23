defmodule Aerospike.Cluster.PartitionMap do
  @moduledoc false
  # ETS-backed partition ownership map with regime guard and per-node
  # partition-generation tracking.
  #
  # Two ETS tables back this module:
  #
  #   * owners table — `{namespace, partition_id}` ⇒ `%PartitionOwners{}`.
  #     Holds the current best known replica list for each partition. An
  #     update with a *strictly lower* regime than the stored value is
  #     rejected (newer regime wins; equal regime overwrites idempotently).
  #
  #   * node-generation table — `node_name` ⇒ `non_neg_integer()`.
  #     The last `partition-generation` value seen for each node. The
  #     tender consults this to decide whether a node's partition map
  #     needs refetching.
  #
  # All functions take table names and are pure with respect to the
  # BEAM process model: any caller may *read*, but per plan Task 8 the
  # Tender is the sole writer by convention.

  @partitions 4096

  defmodule PartitionOwners do
    @moduledoc false

    @enforce_keys [:regime, :replicas]
    defstruct [:regime, :replicas]

    @type t :: %__MODULE__{
            regime: non_neg_integer(),
            replicas: [String.t() | nil, ...]
          }
  end

  @type table :: :ets.tab()
  @type namespace :: String.t()
  @type partition_id :: 0..4095
  @type regime :: non_neg_integer()
  @type node_name :: String.t()

  @doc """
  Creates the two ETS tables and returns their names.

  `name` is used as a prefix so multiple clusters can coexist in the same VM.

  Caller (the Tender) owns these tables — they inherit its lifetime.
  """
  @spec create_tables(atom()) :: {owners :: atom(), node_gens :: atom()}
  def create_tables(name) when is_atom(name) do
    owners = table_name(name, :owners)
    node_gens = table_name(name, :node_gens)

    :ets.new(owners, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(node_gens, [:set, :public, :named_table, read_concurrency: true])

    {owners, node_gens}
  end

  @doc """
  Returns the owners of `{namespace, partition_id}` or `:unknown_partition`.
  """
  @spec owners(table(), namespace(), partition_id()) ::
          {:ok, PartitionOwners.t()} | {:error, :unknown_partition}
  def owners(tab, namespace, partition_id)
      when is_binary(namespace) and partition_id in 0..(@partitions - 1)//1 do
    case :ets.lookup(tab, {namespace, partition_id}) do
      [{_, %PartitionOwners{} = po}] -> {:ok, po}
      [] -> {:error, :unknown_partition}
    end
  end

  @doc """
  Stores `replicas` for `{namespace, partition_id}` at `regime`.

  Returns `{:error, :stale_regime}` if the currently stored entry has a
  strictly higher regime. Equal regime overwrites idempotently.
  """
  @spec update(table(), namespace(), partition_id(), regime(), [node_name() | nil, ...]) ::
          :ok | {:error, :stale_regime}
  def update(tab, namespace, partition_id, regime, [_ | _] = replicas)
      when is_binary(namespace) and partition_id in 0..(@partitions - 1)//1 and
             is_integer(regime) and regime >= 0 do
    key = {namespace, partition_id}

    case :ets.lookup(tab, key) do
      [{_, %PartitionOwners{regime: current}}] when current > regime ->
        {:error, :stale_regime}

      _ ->
        :ets.insert(tab, {key, %PartitionOwners{regime: regime, replicas: replicas}})
        :ok
    end
  end

  @doc """
  Returns `true` iff every partition id 0..4095 has an entry with at least
  one replica for `namespace`.
  """
  @spec complete?(table(), namespace()) :: boolean()
  def complete?(tab, namespace) when is_binary(namespace) do
    complete?(tab, namespace, 0)
  end

  defp complete?(_tab, _namespace, @partitions), do: true

  defp complete?(tab, namespace, partition_id) do
    case :ets.lookup(tab, {namespace, partition_id}) do
      [{_, %PartitionOwners{replicas: [_ | _]}}] ->
        complete?(tab, namespace, partition_id + 1)

      _ ->
        false
    end
  end

  @doc """
  Removes every owners entry for every partition across every namespace.
  """
  @spec clear(table()) :: :ok
  def clear(tab) do
    :ets.delete_all_objects(tab)
    :ok
  end

  @doc """
  Removes every owners entry where `node_name` appears as a replica.

  Used by the Tender when a node is dropped after the failure threshold.
  An entry whose last remaining replica is removed is deleted outright.
  """
  @spec drop_node(table(), node_name()) :: :ok
  def drop_node(tab, node_name) when is_binary(node_name) do
    :ets.foldl(
      fn {key, %PartitionOwners{regime: regime, replicas: replicas}}, :ok ->
        case Enum.reject(replicas, &(&1 == node_name)) do
          ^replicas ->
            :ok

          [] ->
            :ets.delete(tab, key)
            :ok

          remaining ->
            :ets.insert(tab, {key, %PartitionOwners{regime: regime, replicas: remaining}})
            :ok
        end
      end,
      :ok,
      tab
    )
  end

  @doc """
  Reads the last seen `partition-generation` value for `node_name`.
  """
  @spec get_node_gen(table(), node_name()) :: {:ok, non_neg_integer()} | {:error, :unknown_node}
  def get_node_gen(tab, node_name) when is_binary(node_name) do
    case :ets.lookup(tab, node_name) do
      [{_, gen}] -> {:ok, gen}
      [] -> {:error, :unknown_node}
    end
  end

  @doc """
  Stores the latest `partition-generation` for `node_name`.
  """
  @spec put_node_gen(table(), node_name(), non_neg_integer()) :: :ok
  def put_node_gen(tab, node_name, gen)
      when is_binary(node_name) and is_integer(gen) and gen >= 0 do
    :ets.insert(tab, {node_name, gen})
    :ok
  end

  @doc """
  Removes the per-node partition-generation entry for `node_name`.
  """
  @spec delete_node_gen(table(), node_name()) :: :ok
  def delete_node_gen(tab, node_name) when is_binary(node_name) do
    :ets.delete(tab, node_name)
    :ok
  end

  @doc """
  Returns the fixed partition count (4096).
  """
  @spec partition_count() :: pos_integer()
  def partition_count, do: @partitions

  defp table_name(prefix, suffix) do
    :"#{prefix}_partition_map_#{suffix}"
  end
end
