defmodule Aerospike.Supervisor do
  @moduledoc false
  # Top-level supervisor for one named Aerospike connection. Starts children in order:
  #   1. TableOwner  — creates and owns the ETS tables.
  #   2. NodeSupervisor — DynamicSupervisor for per-node connection pools.
  #   3. Cluster — GenServer that discovers nodes and builds the partition map.
  #
  # `one_for_one` strategy: each child is independent. If the Cluster crashes,
  # the ETS tables and pools survive so the restart can re-tend without data loss.

  alias Aerospike.Policy

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
      {Aerospike.Cluster, validated}
    ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: sup_name(name)
    )
  end

  @doc false
  def child_spec(opts) when is_list(opts) do
    validated = Policy.validate_start!(opts)
    name = Keyword.fetch!(validated, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [validated]},
      type: :supervisor,
      restart: :permanent,
      shutdown: :infinity
    }
  end
end
