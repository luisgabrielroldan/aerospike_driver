defmodule Aerospike.Cluster.Router do
  @moduledoc false

  alias Aerospike.Cluster.PartitionMap
  alias Aerospike.Cluster.PartitionMap.PartitionOwners
  alias Aerospike.Key

  @type replica_policy :: :master | :sequence
  @type node_name :: String.t()
  @type reason :: :cluster_not_ready | :no_master

  @typedoc """
  ETS table names published through `Aerospike.Cluster.tables/1`. The router reads
  `:owners` for partition ownership and `:meta` for the `:ready` flag.
  `:node_gens` is accepted for API symmetry with the cluster tables map and is currently
  unused.
  """
  @type tables :: %{owners: atom(), node_gens: atom(), meta: atom(), txn_tracking: atom()}

  @doc """
  Returns the master node name for writing `key`.

  Refuses with `{:error, :cluster_not_ready}` while the cluster has not
  filled every configured namespace's partition map, and with
  `{:error, :no_master}` if the specific partition has no master replica.
  """
  @spec pick_for_write(tables(), Key.t()) :: {:ok, node_name()} | {:error, reason()}
  def pick_for_write(tables, %Key{} = key) do
    if ready?(tables) do
      pick_for_write_ready(tables, key)
    else
      {:error, :cluster_not_ready}
    end
  end

  @doc """
  Returns the node name for reading `key` under `policy` with the
  caller-supplied `attempt` counter.
  """
  @spec pick_for_read(tables(), Key.t(), replica_policy(), non_neg_integer()) ::
          {:ok, node_name()} | {:error, reason()}
  def pick_for_read(tables, %Key{} = key, policy, attempt)
      when policy in [:master, :sequence] and is_integer(attempt) and attempt >= 0 do
    if ready?(tables) do
      pick_for_read_ready(tables, key, policy, attempt)
    else
      {:error, :cluster_not_ready}
    end
  end

  @doc false
  @spec pick_for_write_ready(tables(), Key.t()) :: {:ok, node_name()} | {:error, :no_master}
  def pick_for_write_ready(%{owners: owners}, %Key{} = key) do
    resolve_master(owners, key)
  end

  @doc false
  @spec pick_for_read_ready(tables(), Key.t(), replica_policy(), non_neg_integer()) ::
          {:ok, node_name()} | {:error, :no_master}
  def pick_for_read_ready(%{owners: owners}, %Key{} = key, policy, attempt)
      when policy in [:master, :sequence] and is_integer(attempt) and attempt >= 0 do
    resolve_read(owners, key, policy, attempt)
  end

  @doc false
  @spec ready?(tables()) :: boolean()
  def ready?(%{meta: meta}) do
    case :ets.lookup(meta, :ready) do
      [{:ready, true}] -> true
      _ -> false
    end
  end

  defp resolve_master(owners, key) do
    case PartitionMap.owners(owners, key.namespace, Key.partition_id(key)) do
      {:ok, %PartitionOwners{replicas: [master | _]}} when is_binary(master) ->
        {:ok, master}

      {:ok, %PartitionOwners{}} ->
        {:error, :no_master}

      {:error, :unknown_partition} ->
        {:error, :no_master}
    end
  end

  defp resolve_read(owners, key, :master, _attempt), do: resolve_master(owners, key)

  defp resolve_read(owners, key, :sequence, attempt) do
    case PartitionMap.owners(owners, key.namespace, Key.partition_id(key)) do
      {:ok, %PartitionOwners{replicas: replicas}} ->
        pick_sequence(replicas, attempt)

      {:error, :unknown_partition} ->
        {:error, :no_master}
    end
  end

  defp pick_sequence(replicas, attempt) do
    case available_count(replicas, 0) do
      0 ->
        {:error, :no_master}

      count ->
        {:ok, pick_available(replicas, rem(attempt, count))}
    end
  end

  defp available_count([], count), do: count
  defp available_count([nil | rest], count), do: available_count(rest, count)

  defp available_count([node_name | rest], count) when is_binary(node_name),
    do: available_count(rest, count + 1)

  defp pick_available([nil | rest], index), do: pick_available(rest, index)
  defp pick_available([node_name | _rest], 0) when is_binary(node_name), do: node_name

  defp pick_available([node_name | rest], index)
       when is_binary(node_name) and index > 0 do
    pick_available(rest, index - 1)
  end
end
