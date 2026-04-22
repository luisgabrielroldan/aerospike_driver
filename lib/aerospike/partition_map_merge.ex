defmodule Aerospike.PartitionMapMerge do
  @moduledoc """
  Pure accumulation for partition-map segments reported by a single node.

  This module is the merge half of the partition-map refresh stage. It
  consumes the `[{namespace, regime, ownership}]` segments a node reports
  (see `Aerospike.Node.refresh_partitions/2`) and folds them into the
  owners ETS table using the regime guard: a stored entry with a strictly
  higher regime rejects the incoming segment, an equal regime overwrites
  idempotently, a strictly lower (or absent) regime is replaced.

  It also owns the cluster-stable agreement check: given a per-cycle
  `%{node_name => hash}` map of `cluster-stable` values captured this
  cycle, `verify_cluster_stable/1` decides whether the cluster view is
  consistent enough to merge partition maps onto. The caller (the tend
  orchestrator) is responsible for filtering the map down to active
  nodes that produced a hash this cycle.

  The module is pure with respect to BEAM process model: it reads from
  and writes to the supplied ETS table but owns no process. Writes run
  in whatever process called in; the single-writer invariant for the
  owners table is an invariant of the caller, not of this module.
  """

  alias Aerospike.PartitionMap

  @type segment ::
          {PartitionMap.namespace(), PartitionMap.regime(), [{0..4095, non_neg_integer()}]}

  @doc """
  Applies every segment the node reported for `node_name` against `owners_tab`.

  `namespaces` is the configured namespace allow-list; segments for any
  other namespace are ignored (they cannot contribute to `ready?` anyway).

  Returns `true` when every in-scope segment was accepted by the regime
  guard, `false` when any segment was rejected as stale. The caller uses
  that boolean to decide whether to advance the node's `applied_gen`.
  """
  @spec apply_segments(PartitionMap.table(), PartitionMap.node_name(), [segment()], [
          PartitionMap.namespace()
        ]) :: boolean()
  def apply_segments(owners_tab, node_name, segments, namespaces)
      when is_binary(node_name) and is_list(segments) and is_list(namespaces) do
    Enum.reduce(segments, true, fn {namespace, regime, ownership}, acc ->
      apply_ownership(owners_tab, node_name, namespace, regime, ownership, namespaces) and acc
    end)
  end

  @doc """
  Checks whether every node that produced a `cluster-stable` hash this
  cycle agrees.

  Input is a `%{node_name => hash}` map containing only nodes the caller
  considers contributors (i.e. `:active` with a hash captured this cycle).
  Inactive or hash-less nodes must be filtered out by the caller so the
  `:empty` return is distinguishable from "full outage".

  Returns:

    * `{:ok, hash}` — every contributor reports the same hash; safe to
      fetch partition maps.
    * `{:ok, :empty}` — no contributors. Either the cluster is fully
      unreachable this cycle or no node has been probed yet; safe to
      skip partition-map refresh.
    * `{:error, :disagreement, contributors}` — contributors disagree;
      the cluster is mid-transition. Partition-map refresh is unsafe
      until a later cycle re-verifies.
  """
  @spec verify_cluster_stable(%{optional(PartitionMap.node_name()) => String.t()}) ::
          {:ok, String.t() | :empty}
          | {:error, :disagreement, %{optional(PartitionMap.node_name()) => String.t()}}
  def verify_cluster_stable(contributors) when is_map(contributors) do
    case contributors |> Map.values() |> Enum.uniq() do
      [] -> {:ok, :empty}
      [hash] -> {:ok, hash}
      _ -> {:error, :disagreement, contributors}
    end
  end

  defp apply_ownership(owners_tab, node_name, namespace, regime, ownership, namespaces) do
    if namespace in namespaces do
      Enum.reduce(ownership, true, fn {partition_id, replica_index}, acc ->
        merge_replica(owners_tab, namespace, partition_id, regime, replica_index, node_name) and
          acc
      end)
    else
      true
    end
  end

  defp merge_replica(tab, namespace, partition_id, regime, replica_index, node_name) do
    replicas =
      case PartitionMap.owners(tab, namespace, partition_id) do
        {:ok, %{regime: current_regime, replicas: current}} when current_regime == regime ->
          place_replica(current, replica_index, node_name)

        {:ok, %{regime: current_regime}} when current_regime > regime ->
          :stale

        _ ->
          place_replica([], replica_index, node_name)
      end

    case replicas do
      :stale ->
        false

      replicas ->
        PartitionMap.update(tab, namespace, partition_id, regime, replicas)
        true
    end
  end

  defp place_replica(replicas, replica_index, node_name) do
    padded = pad_replicas(replicas, replica_index)
    List.replace_at(padded, replica_index, node_name)
  end

  defp pad_replicas(replicas, replica_index) do
    needed = replica_index + 1 - length(replicas)

    if needed > 0 do
      replicas ++ List.duplicate(nil, needed)
    else
      replicas
    end
  end
end
