defmodule Aerospike.Tables do
  @moduledoc false
  # Canonical ETS table name helpers for one named connection.
  # All cluster state lives in these four tables, owned by TableOwner.

  # {node_name, row} — `row` is `t:Aerospike.Cluster.node_row/0` (see `insert_node_registry/5`).
  @doc false
  def nodes(name) when is_atom(name), do: :"#{name}_nodes"

  # {{namespace, partition_id, replica_index}, node_name}
  @doc false
  def partitions(name) when is_atom(name), do: :"#{name}_partitions"

  # Transaction tracking (future use).
  @doc false
  def txn_tracking(name) when is_atom(name), do: :"#{name}_txn_tracking"

  # Cluster metadata: policy_defaults, cluster_ready flag, etc.
  @doc false
  def meta(name) when is_atom(name), do: :"#{name}_meta"

  # ETS key that the Router checks to confirm the cluster has completed initial tend.
  @doc false
  def ready_key, do: :cluster_ready

  # Task.Supervisor for batch fan-out (isolates task crashes from callers).
  @doc false
  def task_sup(name) when is_atom(name), do: :"#{name}_task_sup"
end
