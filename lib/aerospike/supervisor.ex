defmodule Aerospike.Supervisor do
  @moduledoc """
  Top-level supervisor for one named Aerospike cluster.

  Starts three children under `rest_for_one` so the blast radius of a
  crash matches which process owns what state:

    1. `Aerospike.TableOwner` — creates and owns the ETS tables backing
       the cluster. Must start first so every later child can read the
       table names.
    2. `Aerospike.NodeSupervisor` — `DynamicSupervisor` for per-node
       `NimblePool` children. Lives independently of the Tender so the
       Tender can restart without losing already-started pools — though
       Tier 1 does not yet take advantage of that (Task 7 wires pools
       to the Tender lifecycle with an orphan-cleanup sweep on restart).
    3. `Aerospike.Tender` — the single-writer cluster-state GenServer.
       Started last so its `init/1` can read the TableOwner's tables
       and reference the NodeSupervisor by name.

  Crash semantics under `rest_for_one`:

    * Tender crash → only the Tender restarts. ETS tables survive, the
      NodeSupervisor survives, and the restarted Tender rehydrates its
      `:ready` flag from the `:meta` table.
    * NodeSupervisor crash → Tender also restarts (it is after
      NodeSupervisor in the list). TableOwner survives.
    * TableOwner crash → the whole subtree restarts with fresh tables.

  This module only supervises. It does not start pools, does not run
  tend cycles, and does not own any application state.
  """

  alias Aerospike.NodeSupervisor
  alias Aerospike.TableOwner
  alias Aerospike.Tender

  @typedoc """
  Start options.

    * `:name` — atom used as the cluster identity (required). Becomes
      the Tender's registered name, the TableOwner's table prefix, and
      the NodeSupervisor's registered name.
    * `:transport` — module implementing `Aerospike.NodeTransport`
      (required).
    * `:seeds` — list of `{host, port}` tuples (required, non-empty).
    * `:namespaces` — list of namespace strings the cluster must serve
      before `Tender.ready?/1` returns `true` (required, non-empty).

  Every other option is forwarded verbatim to `Aerospike.Tender` (for
  example `:connect_opts`, `:failure_threshold`, `:tend_interval_ms`,
  `:tend_trigger`).
  """
  @type option ::
          {:name, atom()}
          | {:transport, module()}
          | {:seeds, [Tender.seed(), ...]}
          | {:namespaces, [Tender.namespace(), ...]}
          | {atom(), term()}

  @doc false
  def child_spec(opts) when is_list(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: :infinity
    }
  end

  @doc """
  Starts the top-level supervisor for one named cluster.

  Returns the `Supervisor` pid on success. The supervisor registers
  itself under `sup_name/1` so callers can reach it by name.
  """
  @spec start_link([option()]) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    validated = validate!(opts)
    name = Keyword.fetch!(validated, :name)

    tender_opts =
      validated
      |> Keyword.drop([:tables, :node_supervisor])
      |> Keyword.put(:node_supervisor, NodeSupervisor.sup_name(name))

    children = [
      {TableOwner, name: name},
      {NodeSupervisor, name: name},
      %{
        id: {Tender, name},
        start: {__MODULE__, :start_tender, [name, tender_opts]},
        type: :worker,
        restart: :permanent,
        shutdown: 5_000
      }
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: sup_name(name))
  end

  @doc false
  # Starts the Tender after resolving the TableOwner's tables at supervisor
  # init time. TableOwner is already up at this point (first child under
  # `rest_for_one`), so `tables/1` is a synchronous call on a live process.
  @spec start_tender(atom(), keyword()) :: GenServer.on_start()
  def start_tender(name, tender_opts) do
    tables = TableOwner.tables(TableOwner.via(name))

    tender_opts
    |> Keyword.put(:tables, tables)
    |> Tender.start_link()
  end

  @doc """
  Returns the registered name atom used by `start_link/1` for `name`.
  """
  @spec sup_name(atom()) :: atom()
  def sup_name(name) when is_atom(name), do: :"#{name}_sup"

  ## Validation

  @required_keys [:name, :transport, :seeds, :namespaces]

  defp validate!(opts) do
    Enum.each(@required_keys, fn key ->
      Keyword.has_key?(opts, key) or
        raise ArgumentError, "Aerospike.Supervisor: missing required option #{inspect(key)}"
    end)

    name = Keyword.fetch!(opts, :name)

    is_atom(name) or
      raise ArgumentError, "Aerospike.Supervisor: :name must be an atom, got #{inspect(name)}"

    transport = Keyword.fetch!(opts, :transport)

    is_atom(transport) or
      raise ArgumentError,
            "Aerospike.Supervisor: :transport must be a module, got #{inspect(transport)}"

    seeds = Keyword.fetch!(opts, :seeds)

    (is_list(seeds) and seeds != []) or
      raise ArgumentError,
            "Aerospike.Supervisor: :seeds must be a non-empty list of {host, port} tuples"

    namespaces = Keyword.fetch!(opts, :namespaces)

    (is_list(namespaces) and namespaces != []) or
      raise ArgumentError,
            "Aerospike.Supervisor: :namespaces must be a non-empty list of strings"

    opts
  end
end
