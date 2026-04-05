defmodule Aerospike.Tables do
  @moduledoc false
  # Canonical ETS table name helpers for one named connection.
  # All cluster state lives in these four tables, owned by TableOwner.

  # {node_name, %{host, port, pool_pid, active, features, rack_id}}
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
end
