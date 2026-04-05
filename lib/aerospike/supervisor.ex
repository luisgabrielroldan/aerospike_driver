defmodule Aerospike.Supervisor do
  @moduledoc false
  # Top-level supervisor for one named Aerospike connection. Starts children in order:
  #   1. TableOwner  — creates and owns the ETS tables.
  #   2. NodeSupervisor — DynamicSupervisor for per-node connection pools.
  #   3. Cluster — GenServer that discovers nodes and builds the partition map.
  #
  # Dependency chain: Cluster and NodeSupervisor both rely on TableOwner's ETS tables.
  # `rest_for_one` restarts the crashed child and every child started after it, so if
  # TableOwner dies the whole subtree is rebuilt with fresh tables; if only Cluster
  # dies, TableOwner and pools survive (same as before under `one_for_one` for the
  # last child).

  alias Aerospike.Policy
  alias Aerospike.Tables

  @doc false
  @spec sup_name(atom()) :: atom()
  def sup_name(name) when is_atom(name), do: :"#{name}_sup"

  @doc false
  def start_link(opts) when is_list(opts) do
    validated = Policy.validate_start!(opts)
    name = Keyword.fetch!(validated, :name)

    children = [
      {Aerospike.TableOwner, name: name},
      {Aerospike.NodeSupervisor, name: name},
      {Task.Supervisor, name: Tables.task_sup(name)},
      {Aerospike.Cluster, validated}
    ]

    Supervisor.start_link(children,
      strategy: :rest_for_one,
      name: sup_name(name)
    )
  end

  @doc false
  def child_spec(opts) when is_list(opts) do
    # Validate to extract :name for the child spec id, but pass raw opts to start_link
    # so validation happens exactly once in start_link.
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: :infinity
    }
  end
end
