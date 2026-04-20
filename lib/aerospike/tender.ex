defmodule Aerospike.Tender do
  @moduledoc """
  Single-writer cluster-state GenServer.

  The Tender is the only process that writes to the cluster's ETS tables
  (partition owners, per-node partition-generation, meta/ready flag). Every
  other component reads directly from ETS for a lock-free hot path.

  Responsibilities:

    * Bootstrap the node set from a seed list.
    * Periodically discover peers from every known node and add any that
      show up.
    * Track each node's reported `partition-generation`; refetch the
      partition map for nodes whose generation advanced.
    * Apply the regime guard on every partition-map update so a late reply
      from a lagging node cannot overwrite a newer one.
    * Count consecutive refresh failures per node; drop a node only when
      the failure threshold is reached.
    * Flip the `:ready` meta flag only once every configured namespace has
      a complete partition map (every partition 0..4095 has a master).

  All I/O goes through the `Aerospike.NodeTransport` behaviour module
  supplied via the `:transport` option. Production code uses
  `Aerospike.Transport.Tcp`; tests substitute `Aerospike.Transport.Fake`
  with scripted replies.

  Tests drive the cycle deterministically with `tend_now/1`, which blocks
  until the cycle's ETS writes are visible.
  """

  use GenServer

  require Logger

  alias Aerospike.Error
  alias Aerospike.NodeCounters
  alias Aerospike.NodeSupervisor
  alias Aerospike.PartitionMap
  alias Aerospike.Protocol.Info, as: InfoParser
  alias Aerospike.Protocol.PartitionMap, as: PartitionMapParser
  alias Aerospike.Protocol.Peers
  alias Aerospike.RetryPolicy
  alias Aerospike.TableOwner
  alias Aerospike.Telemetry
  alias Aerospike.TendHistogram

  @default_tend_interval_ms 1_000
  @default_failure_threshold 5
  @default_pool_size 10
  # Default breaker thresholds. `:circuit_open_threshold` mirrors the
  # Tender's own `failure_threshold` semantics by default — once roughly
  # as many transport failures pile up as the tender would tolerate
  # before demoting the node, stop sending new work. `:max_concurrent_ops`
  # defaults to `pool_size * 10`: generous enough that ordinary
  # steady-state traffic never trips the cap, but tight enough that a
  # wedged pool cannot queue a thundering herd. Both are overridable
  # through `Aerospike.start_link/1`.
  @default_circuit_open_threshold 10
  @default_max_concurrent_multiplier 10
  @refresh_node_info_keys ["partition-generation", "cluster-stable"]

  @type seed :: {String.t(), :inet.port_number()}
  @type namespace :: String.t()

  @typedoc """
  Start options:

    * `:name` — registered name (required).
    * `:transport` — module implementing `Aerospike.NodeTransport` (required).
    * `:seeds` — list of `{host, port}` tuples (required, non-empty).
    * `:namespaces` — list of configured namespaces the cluster must serve
      before `:ready` flips to `true` (required, non-empty).
    * `:tables` — `%{owners: atom(), node_gens: atom(), meta: atom()}`
      map of ETS table names, as returned by `Aerospike.TableOwner.tables/1`
      (required). The Tender does not create tables; it reads and writes
      the tables created by the TableOwner so the partition map survives
      a Tender restart.
    * `:connect_opts` — keyword forwarded verbatim to
      `transport.connect/3`. Default `[]`.
    * `:failure_threshold` — consecutive refresh failures before a node is
      dropped. Default `#{@default_failure_threshold}`.
    * `:tend_interval_ms` — period between automatic tend cycles.
      Default `#{@default_tend_interval_ms}`.
    * `:tend_trigger` — `:timer` (default) or `:manual`. In `:manual`
      mode no timer is started and tests drive cycles with `tend_now/1`.
    * `:node_supervisor` — registered name (atom) or pid of the
      `Aerospike.NodeSupervisor` used to start per-node connection
      pools. When absent, pool lifecycle is skipped entirely — the
      Tender runs with info sockets only, which is the mode the
      cluster-state invariant tests rely on.
    * `:pool_size` — pool workers per node. Default
      `#{@default_pool_size}`. Ignored when `:node_supervisor` is
      absent.
    * `:idle_timeout_ms` — milliseconds a pooled worker may sit idle
      before NimblePool evicts it via `handle_ping/2`. Forwarded to
      `Aerospike.NodeSupervisor.start_pool/2`. Defaults to the value
      chosen by the supervisor (stays below Aerospike's
      `proto-fd-idle-ms` of 60_000 ms). Ignored when `:node_supervisor`
      is absent.
    * `:max_idle_pings` — positive integer bounding how many idle
      workers NimblePool may drop per verification cycle. Forwarded to
      `Aerospike.NodeSupervisor.start_pool/2`. Defaults to the value
      chosen by the supervisor. Ignored when `:node_supervisor` is
      absent.
    * `:circuit_open_threshold` — non-negative integer. The
      `Aerospike.CircuitBreaker` refuses a command when the node's
      `:failed` counter has reached this value. The Tender zeroes the
      `:failed` slot on every successful tend cycle for the node so
      the failure window decays with cluster-state health. Default
      `#{@default_circuit_open_threshold}`.
    * `:max_concurrent_ops_per_node` — positive integer. The breaker
      refuses a command when `in_flight + queued` has reached this
      value. Default `pool_size * #{@default_max_concurrent_multiplier}`.
    * `:max_retries` — non-negative integer. Default retry attempts
      after the initial send before `Aerospike.Get` gives up. `0`
      disables retry. Default `2`. Per-command overrides via
      `Aerospike.get/3` opts.
    * `:sleep_between_retries_ms` — non-negative integer. Fixed sleep
      between retry attempts. Default `0` (no backoff; jittered backoff
      is not implemented).
    * `:replica_policy` — `:master` or `:sequence`. Default `:sequence`
      so retries walk the replica list; set to `:master` to pin every
      attempt to the master replica.
    * `:use_compression` — boolean, default `false`. Cluster-wide opt-in
      for outbound AS_MSG compression. When `true`, command dispatch
      asks the underlying transport to compress requests above the
      reference threshold — but only against nodes whose `features`
      capability set advertises `:compression`. The per-node gating
      happens inside the Tender, not at the call site, so a cluster
      with mixed-capability nodes never sends a compressed frame to a
      node that cannot decode it.
    * `:use_services_alternate` — boolean, default `false`. Cluster-wide
      toggle between the `peers-clear-std` and `peers-clear-alt`
      info keys during peer discovery. `peers-clear-alt` surfaces the
      server's alternate-access endpoints (configured via
      `alternate-access-address`), which is the route a client on a
      different subnet than the server's primary NIC has to take to
      reach every node. The toggle applies only to peer discovery — the
      seed list in `:seeds` is dialled verbatim — and is static for the
      lifetime of the cluster, matching Go
      `ClientPolicy.UseServicesAlternate`.
  """
  @type option ::
          {:name, GenServer.name()}
          | {:transport, module()}
          | {:seeds, [seed(), ...]}
          | {:namespaces, [namespace(), ...]}
          | {:tables, TableOwner.tables()}
          | {:connect_opts, keyword()}
          | {:failure_threshold, pos_integer()}
          | {:tend_interval_ms, pos_integer()}
          | {:tend_trigger, :timer | :manual}
          | {:node_supervisor, atom() | pid()}
          | {:pool_size, pos_integer()}
          | {:idle_timeout_ms, pos_integer()}
          | {:max_idle_pings, pos_integer()}
          | {:circuit_open_threshold, non_neg_integer()}
          | {:max_concurrent_ops_per_node, pos_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:replica_policy, :master | :sequence}
          | {:use_compression, boolean()}
          | {:use_services_alternate, boolean()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Runs one tend cycle synchronously. Returns once every ETS write from
  the cycle has been committed.
  """
  @spec tend_now(GenServer.server()) :: :ok
  def tend_now(server) do
    GenServer.call(server, :tend_now, 30_000)
  end

  @doc """
  Returns whether every configured namespace has a complete partition map.
  """
  @spec ready?(GenServer.server()) :: boolean()
  def ready?(server) do
    GenServer.call(server, :ready?)
  end

  @doc """
  Returns the names of the Tender's ETS tables, intended for Router reads.

  The `:meta` table holds lock-free cluster state flags (currently only
  `:ready`) so hot-path readers can consult cluster state without a
  `GenServer.call` into the Tender.
  """
  @spec tables(GenServer.server()) :: %{owners: atom(), node_gens: atom(), meta: atom()}
  def tables(server) do
    GenServer.call(server, :tables)
  end

  @doc """
  Returns the pid of the `Aerospike.NodePool` serving `node_name`.

  Returns `{:error, :unknown_node}` if the node is not in the cluster's
  current view, is currently in the `:inactive` lifecycle state (pool has
  been stopped pending recovery), or if the Tender was started without a
  `:node_supervisor` (i.e. cluster-state-only mode used by some tests).
  Command modules use this to check out a pooled connection without
  re-opening a socket per call.
  """
  @spec pool_pid(GenServer.server(), String.t()) ::
          {:ok, pid()} | {:error, :unknown_node}
  def pool_pid(server, node_name) when is_binary(node_name) do
    GenServer.call(server, {:pool_pid, node_name})
  end

  @doc """
  Returns the `Aerospike.NodeCounters` reference allocated for
  `node_name`, or `{:error, :unknown_node}` if the node is not in the
  cluster's current view or is currently `:inactive`. The circuit
  breaker reads counter slots through this reference on the hot path;
  this call pays a GenServer hop only once per node, not per command.

  Returns `{:error, :unknown_node}` when the Tender was started without
  a `:node_supervisor` (no pool lifecycle is tracked).
  """
  @spec node_counters(GenServer.server(), String.t()) ::
          {:ok, NodeCounters.t()} | {:error, :unknown_node}
  def node_counters(server, node_name) when is_binary(node_name) do
    GenServer.call(server, {:node_counters, node_name})
  end

  @doc """
  Returns the `Aerospike.TendHistogram` reference for `node_name`.

  The histogram accumulates per-node partition-map-refresh latency
  samples (one sample per tend cycle that fetches `replicas` for the
  node). Readers call `Aerospike.TendHistogram.percentile/2` or
  `count/1` directly against the returned reference; the GenServer hop
  is one-shot per node, not per query.

  Returns `{:error, :unknown_node}` if the node is not in the cluster's
  current view, is currently `:inactive`, or when the Tender was
  started without a `:node_supervisor` (cluster-state-only mode still
  allocates histograms — see the head of `allocate_histogram/1`).
  """
  @spec tend_histogram(GenServer.server(), String.t()) ::
          {:ok, TendHistogram.t()} | {:error, :unknown_node}
  def tend_histogram(server, node_name) when is_binary(node_name) do
    GenServer.call(server, {:tend_histogram, node_name})
  end

  @typedoc """
  Everything a command path needs to dispatch one attempt against a node:

    * `:pool` — pid of the node's `Aerospike.NodePool`.
    * `:counters` — the node's `Aerospike.NodeCounters` reference.
    * `:breaker` — breaker thresholds the caller passes to
      `Aerospike.CircuitBreaker.allow?/2`.
    * `:use_compression` — whether the command path should ask the
      transport to compress the request for this attempt. Computed as
      `cluster_use_compression and node_supports_compression?` at
      handle time, so a single cluster flag composes with the node's
      live `features` set without per-call policy state.
  """
  @type node_handle :: %{
          pool: pid(),
          counters: NodeCounters.t(),
          breaker: %{
            circuit_open_threshold: non_neg_integer(),
            max_concurrent_ops_per_node: pos_integer()
          },
          use_compression: boolean()
        }

  @doc """
  Returns the pool pid, counters reference, and breaker thresholds for
  `node_name` in one GenServer hop.

  Replaces the sequence of `pool_pid/2` + `node_counters/2` that the
  command path used in Task 5; commands now pay one round trip per op
  instead of two. Returns `{:error, :unknown_node}` under the same
  conditions as `pool_pid/2` and `node_counters/2` (unknown node,
  `:inactive` node, or cluster-state-only mode with no pool).
  """
  @spec node_handle(GenServer.server(), String.t()) ::
          {:ok, node_handle()} | {:error, :unknown_node}
  def node_handle(server, node_name) when is_binary(node_name) do
    GenServer.call(server, {:node_handle, node_name})
  end

  @doc """
  Returns a snapshot of every known node's lifecycle state.

  The map is keyed by node name and exposes the fields the retry and
  circuit-breaker layers consume:

    * `:status` — `:active | :inactive`.
    * `:failures` — consecutive refresh-failure counter.
    * `:recoveries` — number of times the node has flipped from
      `:inactive` back to `:active` across the Tender's lifetime.
    * `:last_tend_at` — monotonic milliseconds of the most recent tend
      stage that touched this node, or `nil` if none has run.
    * `:last_tend_result` — `:ok | :error | nil` classification of the
      outcome of that stage.
    * `:generation_seen` — most recent `partition-generation` value
      observed for the node, or `nil`.
    * `:counters` — the node's `Aerospike.NodeCounters` reference, or
      `nil` when the Tender was started without a `:node_supervisor`
      (cluster-state-only mode).
    * `:tend_histogram` — the node's `Aerospike.TendHistogram`
      reference, allocated at registration and nilled on lifecycle
      demotion. Callers that only want percentiles should prefer
      `tend_histogram/2`; the field is exposed here so a diagnostic
      caller can inspect the raw slot counts.
    * `:features` — `MapSet` of capability tokens captured from the
      node's `features` info-key reply at registration. Recognised
      tokens (e.g. `:compression`, `:pipelining`) are atoms;
      unrecognised tokens are preserved as `{:unknown, raw_string}`
      tuples. An empty set means either a probe failure or a server
      that advertises no capabilities the client knows about.

  This call is intended for tests and diagnostics; hot-path readers use
  ETS (`owners`, `node_gens`, `meta`) or `node_counters/2` instead.
  """
  @spec nodes_status(GenServer.server()) :: %{optional(String.t()) => map()}
  def nodes_status(server) do
    GenServer.call(server, :nodes_status)
  end

  @doc """
  Returns the transport module this Tender dispatches I/O through.

  Command modules pair this with `pool_pid/2`: checkout a pool worker,
  then call `transport.command/2` on the checked-out connection. The
  value is fixed at `start_link/1` time and does not change across
  tend cycles.
  """
  @spec transport(GenServer.server()) :: module()
  def transport(server) do
    GenServer.call(server, :transport)
  end

  @impl GenServer
  def init(opts) do
    transport = Keyword.fetch!(opts, :transport)
    seeds = Keyword.fetch!(opts, :seeds)
    namespaces = Keyword.fetch!(opts, :namespaces)
    tables = Keyword.fetch!(opts, :tables)

    seeds != [] or raise ArgumentError, "Aerospike.Tender: :seeds must be non-empty"

    namespaces != [] or
      raise ArgumentError, "Aerospike.Tender: :namespaces must be non-empty"

    %{owners: owners_tab, node_gens: node_gens_tab, meta: meta_tab} = tables

    pool_size = Keyword.get(opts, :pool_size, @default_pool_size)

    breaker_opts = %{
      circuit_open_threshold:
        Keyword.get(opts, :circuit_open_threshold, @default_circuit_open_threshold),
      max_concurrent_ops_per_node:
        Keyword.get(
          opts,
          :max_concurrent_ops_per_node,
          pool_size * @default_max_concurrent_multiplier
        )
    }

    retry_opts = RetryPolicy.from_opts(opts)
    RetryPolicy.put(meta_tab, retry_opts)

    state = %{
      transport: transport,
      connect_opts: Keyword.get(opts, :connect_opts, []),
      seeds: seeds,
      namespaces: namespaces,
      failure_threshold: Keyword.get(opts, :failure_threshold, @default_failure_threshold),
      tend_interval_ms: Keyword.get(opts, :tend_interval_ms, @default_tend_interval_ms),
      tend_trigger: Keyword.get(opts, :tend_trigger, :timer),
      node_supervisor: Keyword.get(opts, :node_supervisor),
      pool_size: pool_size,
      idle_timeout_ms: Keyword.get(opts, :idle_timeout_ms),
      max_idle_pings: Keyword.get(opts, :max_idle_pings),
      breaker_opts: breaker_opts,
      retry_opts: retry_opts,
      use_compression: Keyword.get(opts, :use_compression, false),
      use_services_alternate: Keyword.get(opts, :use_services_alternate, false),
      owners_tab: owners_tab,
      node_gens_tab: node_gens_tab,
      meta_tab: meta_tab,
      nodes: %{},
      ready?: read_ready(meta_tab)
    }

    cleanup_orphan_pools(state)

    {:ok, maybe_schedule_tend(state)}
  end

  @impl GenServer
  def handle_call(:tend_now, _from, state) do
    {:reply, :ok, run_tend(state)}
  end

  def handle_call(:ready?, _from, state) do
    {:reply, state.ready?, state}
  end

  def handle_call(:tables, _from, state) do
    {:reply, %{owners: state.owners_tab, node_gens: state.node_gens_tab, meta: state.meta_tab},
     state}
  end

  def handle_call({:pool_pid, node_name}, _from, state) do
    case Map.fetch(state.nodes, node_name) do
      {:ok, %{status: :active, pool_pid: pid}} when is_pid(pid) ->
        {:reply, {:ok, pid}, state}

      _ ->
        {:reply, {:error, :unknown_node}, state}
    end
  end

  def handle_call({:node_counters, node_name}, _from, state) do
    case Map.fetch(state.nodes, node_name) do
      {:ok, %{status: :active, counters: ref}} when not is_nil(ref) ->
        {:reply, {:ok, ref}, state}

      _ ->
        {:reply, {:error, :unknown_node}, state}
    end
  end

  def handle_call({:tend_histogram, node_name}, _from, state) do
    case Map.fetch(state.nodes, node_name) do
      {:ok, %{status: :active, tend_histogram: ref}} when not is_nil(ref) ->
        {:reply, {:ok, ref}, state}

      _ ->
        {:reply, {:error, :unknown_node}, state}
    end
  end

  def handle_call({:node_handle, node_name}, _from, state) do
    case Map.fetch(state.nodes, node_name) do
      {:ok, %{status: :active, pool_pid: pool, counters: counters, features: features}}
      when is_pid(pool) and not is_nil(counters) ->
        handle = %{
          pool: pool,
          counters: counters,
          breaker: state.breaker_opts,
          use_compression: state.use_compression and MapSet.member?(features, :compression)
        }

        {:reply, {:ok, handle}, state}

      _ ->
        {:reply, {:error, :unknown_node}, state}
    end
  end

  def handle_call(:transport, _from, state) do
    {:reply, state.transport, state}
  end

  def handle_call(:nodes_status, _from, state) do
    snapshot =
      Map.new(state.nodes, fn {name, node} ->
        {name,
         %{
           status: node.status,
           failures: node.failures,
           recoveries: node.recoveries,
           last_tend_at: node.last_tend_at,
           last_tend_result: node.last_tend_result,
           generation_seen: node.generation_seen,
           counters: node.counters,
           tend_histogram: node.tend_histogram,
           features: node.features
         }}
      end)

    {:reply, snapshot, state}
  end

  @impl GenServer
  def handle_info(:tend, state) do
    state = run_tend(state)
    {:noreply, maybe_schedule_tend(state)}
  end

  @impl GenServer
  def terminate(_reason, state) do
    Enum.each(state.nodes, fn {_, node} -> safe_close(state.transport, node.conn) end)
    :ok
  end

  ## Tend cycle

  defp run_tend(state) do
    :telemetry.span(Telemetry.tend_cycle_span(), %{}, fn ->
      new_state =
        state
        |> bootstrap_if_needed()
        |> refresh_nodes()
        |> discover_peers()
        |> maybe_refresh_partition_maps()
        |> recompute_ready()

      {new_state, %{}}
    end)
  end

  # Only fetches `replicas` once every `:active` node that produced a
  # `cluster-stable` hash this cycle agrees on the same hash. Disagreement
  # means the cluster is mid-transition; writing partition-map entries on
  # top of that transition risks mixing replica lists from different
  # cluster views, which is exactly the poisoning the guard prevents.
  #
  # Wrapped in a partition-map-refresh span so operators can separate
  # the cost of fetching partition-generation (already inside
  # `refresh_nodes/1`) from the cost of fetching and decoding the full
  # `replicas` payload, which dominates wall-clock time in steady state.
  defp maybe_refresh_partition_maps(state) do
    :telemetry.span(Telemetry.partition_map_refresh_span(), %{}, fn ->
      new_state =
        case verify_cluster_stable(state) do
          {:ok, _hash_or_empty} ->
            refresh_partition_maps(state)

          {:error, :disagreement, per_node_hashes} ->
            Logger.warning(
              "Aerospike.Tender: cluster-stable disagreement; skipping partition-map refresh: " <>
                inspect(per_node_hashes)
            )

            state
        end

      {new_state, %{}}
    end)
  end

  # Returns `{:ok, hash}` if every `:active` node with a `cluster-stable`
  # value captured this cycle agrees, `{:ok, :empty}` if there is no node
  # to verify (full outage or all info calls failed this cycle), and
  # `{:error, :disagreement, per_node_hashes}` otherwise. Inactive nodes
  # are excluded by construction (they cannot serve traffic), as are
  # active nodes whose info call failed this cycle (they have nil hash).
  @spec verify_cluster_stable(map()) ::
          {:ok, String.t() | :empty}
          | {:error, :disagreement, %{optional(String.t()) => String.t()}}
  defp verify_cluster_stable(state) do
    contributors =
      state.nodes
      |> Enum.filter(fn {_, node} ->
        node.status == :active and is_binary(node.cluster_stable)
      end)
      |> Map.new(fn {name, node} -> {name, node.cluster_stable} end)

    case contributors |> Map.values() |> Enum.uniq() do
      [] -> {:ok, :empty}
      [hash] -> {:ok, hash}
      _ -> {:error, :disagreement, contributors}
    end
  end

  # Re-enters seed bootstrap whenever `state.nodes` is empty. In steady
  # state the map is non-empty and this is a `map_size == 0` check — the
  # same cost as a boolean latch. When every node has been dropped (full
  # outage followed by the grace-cycle drop), the next tend cycle re-runs
  # `bootstrap_seed/3` against the configured seeds. A seed that is still
  # dead fails the info probe inside `bootstrap_seed/3` and logs at
  # `:warning`; no state is written and the next cycle retries. The
  # `:seeds` option is validated non-empty in `init/1`, so the "no
  # configured seeds" edge case cannot occur here.
  defp bootstrap_if_needed(%{nodes: nodes} = state) when map_size(nodes) > 0, do: state

  defp bootstrap_if_needed(state) do
    Enum.reduce(state.seeds, state, fn {host, port}, acc ->
      bootstrap_seed(acc, host, port)
    end)
  end

  # Fetches `node` and `features` in one info round-trip. The seed list is
  # dialled verbatim from `connect_opts`, so `service-clear-alt` does not
  # belong here — peer discovery is the only place the alternate-services
  # toggle takes effect.
  defp bootstrap_seed(state, host, port) do
    with {:ok, conn} <- state.transport.connect(host, port, state.connect_opts),
         {:ok, %{"node" => node_name} = info} <-
           state.transport.info(conn, ["node", "features"]) do
      case Map.fetch(state.nodes, node_name) do
        {:ok, _existing} ->
          safe_close(state.transport, conn)
          state

        :error ->
          features = parse_bootstrap_features(node_name, info)
          register_new_node(state, node_name, host, port, conn, features)
      end
    else
      {:error, %Error{} = err} ->
        Logger.warning("Aerospike.Tender: seed #{host}:#{port} bootstrap failed: #{err.message}")
        state

      {:ok, _other} ->
        Logger.warning("Aerospike.Tender: seed #{host}:#{port} missing 'node' info")
        state
    end
  end

  # `features` is best-effort: a node that fails the probe still
  # registers, but with an empty feature set, which is the safe default
  # (every optional capability stays off). This matches the Go client's
  # behaviour — `node.go:refreshFeatures` swallows a missing reply and
  # leaves the feature set at zero.
  defp parse_bootstrap_features(node_name, info) do
    case Map.fetch(info, "features") do
      {:ok, value} when is_binary(value) ->
        InfoParser.parse_features(value)

      _ ->
        Logger.debug(fn ->
          "Aerospike.Tender: #{node_name} bootstrap features key absent; assuming none"
        end)

        MapSet.new()
    end
  end

  defp register_new_node(state, node_name, host, port, conn, features) do
    # Allocate counters *before* starting the pool so the pool's
    # callbacks see the reference from the first init_worker call.
    # In cluster-state-only mode (`:node_supervisor` absent) no pool
    # runs so no writer ever touches a counters ref; skipping the
    # allocation there keeps `:counters` consistently nil for that
    # mode and matches the "counters exist iff a pool exists"
    # invariant used by the breaker.
    counters = allocate_counters(state)
    # The tend-latency histogram is always allocated (cluster-state-only
    # mode still runs tend cycles and fetches partition maps, so the
    # sampling call site runs regardless of whether a pool exists).
    tend_histogram = TendHistogram.new()

    case ensure_pool(state, node_name, host, port, counters, features) do
      {:ok, pool_pid} ->
        node = %{
          name: node_name,
          host: host,
          port: port,
          conn: conn,
          failures: 0,
          recoveries: 0,
          status: :active,
          last_tend_at: nil,
          last_tend_result: nil,
          generation_seen: nil,
          applied_gen: nil,
          cluster_stable: nil,
          pool_pid: pool_pid,
          counters: counters,
          features: features,
          tend_histogram: tend_histogram
        }

        %{state | nodes: Map.put(state.nodes, node_name, node)}

      :error ->
        safe_close(state.transport, conn)
        state
    end
  end

  # Refreshes every known node's `partition-generation` and `cluster-stable`
  # values in a single combined info call per node. The cycle starts by
  # clearing the previous cycle's `cluster_stable` hash on every node so
  # that `verify_cluster_stable/1` only ever reads hashes captured in the
  # current cycle (a node that fails this cycle's info call cannot
  # contribute a stale hash to the agreement check).
  defp refresh_nodes(state) do
    state = clear_cluster_stable(state)

    Enum.reduce(state.nodes, state, fn {name, _}, acc ->
      case Map.fetch(acc.nodes, name) do
        {:ok, node} -> refresh_node(acc, node)
        :error -> acc
      end
    end)
  end

  defp clear_cluster_stable(state) do
    nodes =
      Map.new(state.nodes, fn {name, node} ->
        {name, %{node | cluster_stable: nil}}
      end)

    %{state | nodes: nodes}
  end

  defp refresh_node(state, node) do
    case state.transport.info(node.conn, @refresh_node_info_keys) do
      {:ok, info} ->
        handle_refresh_info(state, node, info)

      {:error, %Error{} = err} ->
        Logger.debug("Aerospike.Tender: #{node.name} refresh-node info failed: #{err.message}")

        register_failure(state, node.name)
    end
  end

  # Both keys must parse for the cycle to count as a success for this node.
  # Missing or malformed `partition-generation` is the canonical failure
  # signal (the rest of the tend cycle hangs off the generation value);
  # missing or malformed `cluster-stable` is also a failure because the
  # agreement guard cannot verify a node that did not report a hash.
  defp handle_refresh_info(state, node, %{
         "partition-generation" => gen_value,
         "cluster-stable" => cluster_stable
       })
       when is_binary(cluster_stable) and cluster_stable != "" do
    case PartitionMapParser.parse_partition_generation(gen_value) do
      {:ok, gen} ->
        store_node_gen_if_changed(state, node.name, gen)
        register_success(state, node.name, gen, cluster_stable)

      :error ->
        register_failure(state, node.name)
    end
  end

  defp handle_refresh_info(state, node, _info) do
    register_failure(state, node.name)
  end

  # Records a successful info reply against the node record. Clears the
  # `failures` counter; if the node was `:inactive`, flips it back to
  # `:active` and increments `:recoveries` so the recovery count survives
  # future transitions. Also stamps `last_tend_at`/`last_tend_result` and
  # the `cluster-stable` hash captured this cycle (read later by
  # `verify_cluster_stable/1`).
  #
  # Zeroes the per-node `:failed` counter as the tender-side "failure
  # window decay" for the Task 6 circuit breaker. The info socket just
  # answered cleanly, which is the Tender's strongest statement about
  # node health — any pool-path transport failures recorded before this
  # cycle are stale relative to that statement. The breaker in
  # `Aerospike.CircuitBreaker.allow?/2` re-reads `:failed` on every
  # command so the reset is observed immediately by the next caller.
  defp register_success(state, name, gen, cluster_stable) do
    now = monotonic_ms()

    update_node(state, name, fn node ->
      {status, recoveries} =
        case node.status do
          :active -> {:active, node.recoveries}
          :inactive -> {:active, node.recoveries + 1}
        end

      reset_failed_counter(node.counters)

      %{
        node
        | failures: 0,
          status: status,
          recoveries: recoveries,
          last_tend_at: now,
          last_tend_result: :ok,
          generation_seen: gen,
          cluster_stable: cluster_stable
      }
    end)
  end

  defp reset_failed_counter(nil), do: :ok
  defp reset_failed_counter(ref), do: NodeCounters.reset_failed(ref)

  defp register_peers_success(state, name) do
    now = monotonic_ms()

    update_node(state, name, fn node ->
      {status, recoveries} =
        case node.status do
          :active -> {:active, node.recoveries}
          :inactive -> {:active, node.recoveries + 1}
        end

      %{
        node
        | failures: 0,
          status: status,
          recoveries: recoveries,
          last_tend_at: now,
          last_tend_result: :ok
      }
    end)
  end

  defp register_replicas_success(state, name) do
    now = monotonic_ms()

    update_node(state, name, fn node ->
      %{node | failures: 0, last_tend_at: now, last_tend_result: :ok}
    end)
  end

  defp store_node_gen_if_changed(state, name, gen) do
    case PartitionMap.get_node_gen(state.node_gens_tab, name) do
      {:ok, ^gen} -> :ok
      _ -> PartitionMap.put_node_gen(state.node_gens_tab, name, gen)
    end
  end

  # Bumps the per-node failure counter and drives lifecycle transitions:
  #
  #   * `:active` ⇒ `:inactive` — on reaching `failure_threshold`. Stops
  #     the pool, clears the node's `node_gens` / `owners` rows, but
  #     leaves the node in `state.nodes` with its `:conn` intact so the
  #     next tend cycle can probe it via `partition-generation` and flip
  #     it back to `:active`. Resets `failures` to 0 so the grace-window
  #     accounting is distinct from the active-state breach budget.
  #
  #   * `:inactive` ⇒ removed — any failure while `:inactive` removes
  #     the node entry entirely (`drop_node/2`). The pool was already
  #     stopped and the node's ETS rows cleared by the earlier
  #     `:active` ⇒ `:inactive` transition; this path only drops the
  #     in-memory entry and closes `:conn`. A single grace cycle is
  #     enough: the only thing the Tender does for an `:inactive` node
  #     is probe `partition-generation` in `refresh_nodes/1`, so "any
  #     failure" and "second consecutive threshold breach" collapse to
  #     the same event.
  defp register_failure(state, name) do
    now = monotonic_ms()

    state =
      update_node(state, name, fn node ->
        %{
          node
          | failures: node.failures + 1,
            last_tend_at: now,
            last_tend_result: :error
        }
      end)

    case Map.fetch(state.nodes, name) do
      {:ok, %{status: :inactive}} ->
        drop_node(state, name)

      {:ok, %{failures: failures, status: :active}} when failures >= state.failure_threshold ->
        mark_inactive(state, name)

      _ ->
        state
    end
  end

  # Flips `name` from `:active` to `:inactive`. Stops the pool so inflight
  # checkouts see `{:error, :unknown_node}` on the next attempt, clears
  # the node's `owners`/`node_gens` rows so the Router stops routing to
  # it, resets the `failures` counter so the next tend cycle can either
  # recover or fall through to a full drop, and drops the node's
  # counters / tend-histogram references — the pool is gone, the breaker
  # cannot sensibly read stale slots, and a fresh recovery will allocate
  # new references via `register_new_node/6`. The `:atomics` term is
  # GC'd once no process retains it.
  defp mark_inactive(state, name) do
    case Map.fetch(state.nodes, name) do
      {:ok, node} ->
        drop_pool(state, node)
        PartitionMap.delete_node_gen(state.node_gens_tab, name)
        PartitionMap.drop_node(state.owners_tab, name)

        updated = %{
          node
          | status: :inactive,
            failures: 0,
            pool_pid: nil,
            generation_seen: nil,
            applied_gen: nil,
            counters: nil,
            tend_histogram: nil
        }

        %{state | nodes: Map.put(state.nodes, name, updated)}

      :error ->
        state
    end
  end

  # Fully removes `name` from `state.nodes` and closes its info socket.
  # Any pool has already been stopped by the earlier `mark_inactive/2`
  # transition; the ETS rows are already absent for the same reason.
  # Counters were cleared at the inactive transition, so dropping the
  # node-map entry here is enough — the GC reclaims the :counters ref
  # once no process retains it.
  defp drop_node(state, name) do
    case Map.pop(state.nodes, name) do
      {nil, _} ->
        state

      {node, nodes} ->
        drop_pool(state, node)
        safe_close(state.transport, node.conn)
        PartitionMap.delete_node_gen(state.node_gens_tab, name)
        PartitionMap.drop_node(state.owners_tab, name)
        %{state | nodes: nodes}
    end
  end

  defp update_node(state, name, fun) do
    case Map.fetch(state.nodes, name) do
      {:ok, node} -> %{state | nodes: Map.put(state.nodes, name, fun.(node))}
      :error -> state
    end
  end

  ## Peer discovery

  # Iterates over every known `:active` node (ordered by node name) asking
  # for the configured peer-discovery info key. Every parseable reply
  # contributes to the peer set — we do not stop at the first one. This
  # avoids the "first-by-term-order" failure mode where a stale or broken
  # lead node masks real topology. Nodes flipped to `:inactive` earlier in
  # the same cycle are skipped; their grace-cycle recovery probe happens
  # in `refresh_nodes/1` of the *next* cycle.
  defp discover_peers(state) do
    ordered = state.nodes |> sorted_nodes() |> Enum.filter(&(&1.status == :active))

    {state, peers_by_name} =
      Enum.reduce(ordered, {state, %{}}, &collect_peers/2)

    Enum.reduce(peers_by_name, state, fn {_name, peer}, acc ->
      ensure_peer_connected(acc, peer)
    end)
  end

  # `peers-clear-alt` surfaces the server's alternate-access addresses;
  # `peers-clear-std` surfaces its primary-access addresses. Go's
  # `ClientPolicy.UseServicesAlternate` picks between them; the reply
  # format is identical so `Peers.parse_peers_clear_std/1` handles both.
  defp peer_info_key(%{use_services_alternate: true}), do: "peers-clear-alt"
  defp peer_info_key(_state), do: "peers-clear-std"

  defp collect_peers(node, {state, peers_acc}) do
    key = peer_info_key(state)

    case state.transport.info(node.conn, [key]) do
      {:ok, %{^key => value}} ->
        handle_peers_value(state, node, value, peers_acc)

      {:ok, _other} ->
        {register_failure(state, node.name), peers_acc}

      {:error, %Error{} = err} ->
        Logger.debug("Aerospike.Tender: #{node.name} #{key} failed: #{err.message}")
        {register_failure(state, node.name), peers_acc}
    end
  end

  defp handle_peers_value(state, node, value, peers_acc) do
    case Peers.parse_peers_clear_std(value) do
      {:ok, %{peers: peers}} ->
        {register_peers_success(state, node.name), merge_peers(peers_acc, peers)}

      :error ->
        {register_failure(state, node.name), peers_acc}
    end
  end

  defp merge_peers(acc, peers) do
    Enum.reduce(peers, acc, fn peer, m -> Map.put_new(m, peer.node_name, peer) end)
  end

  defp ensure_peer_connected(state, %{node_name: name, host: host, port: port}) do
    case Map.has_key?(state.nodes, name) do
      true ->
        state

      false ->
        case state.transport.connect(host, port, state.connect_opts) do
          {:ok, conn} ->
            features = fetch_peer_features(state, name, conn)
            register_new_node(state, name, host, port, conn, features)

          {:error, %Error{} = err} ->
            Logger.warning(
              "Aerospike.Tender: could not connect to peer #{name} at #{host}:#{port}: #{err.message}"
            )

            state
        end
    end
  end

  # Peers are registered with the same best-effort feature handling as
  # seeds. The peer's `node_name` is already known from the
  # `peers-clear-std` reply, so only the `features` key is needed here;
  # an empty MapSet on probe failure keeps capability-gated paths off
  # for that node until the next time it is discovered.
  defp fetch_peer_features(state, name, conn) do
    case state.transport.info(conn, ["features"]) do
      {:ok, info} ->
        parse_bootstrap_features(name, info)

      {:error, %Error{} = err} ->
        Logger.debug(fn ->
          "Aerospike.Tender: peer #{name} features probe failed: #{err.message}"
        end)

        MapSet.new()
    end
  end

  ## Partition map refresh

  defp refresh_partition_maps(state) do
    state.nodes
    |> sorted_nodes()
    |> Enum.filter(&(&1.status == :active))
    |> Enum.reduce(state, fn node, acc ->
      case Map.fetch(acc.nodes, node.name) do
        {:ok, %{status: :active} = current} -> maybe_refresh_partition_map(acc, current)
        _ -> acc
      end
    end)
  end

  # Short-circuits the `replicas` info call when the node's last observed
  # `partition-generation` matches the one whose replicas reply was last
  # successfully applied. A newly-discovered peer has `applied_gen: nil`
  # (and typically `generation_seen: nil` since its `partition-generation`
  # has not been fetched yet) — both conditions force a fetch so the first
  # cycle always installs the node's ownership entries.
  defp maybe_refresh_partition_map(state, node) do
    if partition_map_fetch_needed?(node) do
      fetch_and_apply_replicas(state, node)
    else
      state
    end
  end

  defp partition_map_fetch_needed?(%{generation_seen: nil}), do: true
  defp partition_map_fetch_needed?(%{applied_gen: nil}), do: true
  defp partition_map_fetch_needed?(%{generation_seen: same, applied_gen: same}), do: false
  defp partition_map_fetch_needed?(_node), do: true

  defp fetch_and_apply_replicas(state, node) do
    # Measure the full replicas fetch + apply for this node. Per-node
    # latency is the useful operator signal (a single slow node shows up
    # here before it does in the cluster-wide tend_cycle span), so the
    # sample lands in the node's own histogram rather than a cluster
    # aggregate.
    start_native = System.monotonic_time()

    result =
      case state.transport.info(node.conn, ["replicas"]) do
        {:ok, %{"replicas" => value}} ->
          apply_replicas(state, node, value)

        {:ok, _other} ->
          register_failure(state, node.name)

        {:error, %Error{} = err} ->
          Logger.debug("Aerospike.Tender: #{node.name} replicas failed: #{err.message}")
          register_failure(state, node.name)
      end

    record_tend_sample(node.tend_histogram, System.monotonic_time() - start_native)

    result
  end

  defp record_tend_sample(nil, _duration_native), do: :ok
  defp record_tend_sample(ref, duration_native), do: TendHistogram.record(ref, duration_native)

  # Advances `applied_gen` to the node's `generation_seen` only when every
  # segment is accepted by the regime guard. If any segment is rejected as
  # stale the applied generation stays at its prior value so the next tend
  # cycle refetches `replicas` and retries the merge.
  defp apply_replicas(state, node, value) do
    segments = PartitionMapParser.parse_replicas_with_regime(value)

    applied? =
      Enum.reduce(segments, true, fn {namespace, regime, ownership}, acc ->
        apply_ownership(state, node.name, namespace, regime, ownership) and acc
      end)

    state = register_replicas_success(state, node.name)

    if applied? do
      mark_partition_map_applied(state, node.name, node.generation_seen)
    else
      state
    end
  end

  defp apply_ownership(state, node_name, namespace, regime, ownership) do
    if namespace in state.namespaces do
      Enum.reduce(ownership, true, fn {partition_id, replica_index}, acc ->
        merge_replica(state.owners_tab, namespace, partition_id, regime, replica_index, node_name) and
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

  defp mark_partition_map_applied(state, name, gen) do
    update_node(state, name, fn node -> %{node | applied_gen: gen} end)
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

  ## Ready flag

  defp recompute_ready(state) do
    ready? =
      Enum.all?(state.namespaces, fn namespace ->
        PartitionMap.complete?(state.owners_tab, namespace)
      end)

    :ets.insert(state.meta_tab, {:ready, ready?})
    %{state | ready?: ready?}
  end

  ## Pool lifecycle

  # Starts a NodePool for `node_name` under the configured NodeSupervisor.
  # Returns `{:ok, pool_pid}` on success. Returns `:error` on pool-start
  # failure: a warning is logged, the node is skipped for this cycle, and
  # the caller is expected to *not* register the node (per the Phase
  # decision: pool-start failure never bumps the per-node failure counter
  # because the node is never added to `state.nodes`). When the Tender
  # runs without a `:node_supervisor` (cluster-state-only mode), this
  # returns a sentinel `{:ok, nil}` so bootstrap/peer discovery still
  # register the node — that path is used by the invariant tests.
  defp allocate_counters(%{node_supervisor: nil}), do: nil
  defp allocate_counters(_state), do: NodeCounters.new()

  defp ensure_pool(%{node_supervisor: nil}, _node_name, _host, _port, _counters, _features),
    do: {:ok, nil}

  defp ensure_pool(state, node_name, host, port, counters, features) do
    opts =
      [
        node_name: node_name,
        transport: state.transport,
        host: host,
        port: port,
        connect_opts: state.connect_opts,
        pool_size: state.pool_size,
        counters: counters,
        features: features
      ]
      |> maybe_put_pool_opt(:idle_timeout_ms, state.idle_timeout_ms)
      |> maybe_put_pool_opt(:max_idle_pings, state.max_idle_pings)

    case NodeSupervisor.start_pool(state.node_supervisor, opts) do
      {:ok, pool_pid} ->
        {:ok, pool_pid}

      {:error, reason} ->
        Logger.warning(
          "Aerospike.Tender: could not start pool for #{node_name} at #{host}:#{port}: " <>
            "#{inspect(reason)}"
        )

        :error
    end
  end

  # Stops the pool for `node`. Absence of a pool (no supervisor or a nil
  # pool_pid) is a no-op. `{:error, :not_found}` from the NodeSupervisor
  # is logged at `:debug` — a pool that already exited is an acceptable
  # terminal state.
  defp drop_pool(%{node_supervisor: nil}, _node), do: :ok
  defp drop_pool(_state, %{pool_pid: nil}), do: :ok

  defp drop_pool(state, %{name: name, pool_pid: pool_pid}) do
    case NodeSupervisor.stop_pool(state.node_supervisor, pool_pid) do
      :ok ->
        :ok

      {:error, :not_found} ->
        Logger.debug("Aerospike.Tender: pool for #{name} already gone")
        :ok
    end
  end

  # On Tender (re)start under `rest_for_one`, the NodeSupervisor survives.
  # Pools started by a previous Tender incarnation are still children of
  # that supervisor but are no longer reachable via the new Tender's
  # `state.nodes`. Kill them before the first tend cycle so the Tender's
  # view remains the single source of truth.
  defp cleanup_orphan_pools(%{node_supervisor: nil}), do: :ok

  defp cleanup_orphan_pools(state) do
    case sup_pid(state.node_supervisor) do
      nil ->
        :ok

      sup ->
        sup
        |> DynamicSupervisor.which_children()
        |> Enum.each(fn
          {_id, pid, _type, _mods} when is_pid(pid) ->
            _ = NodeSupervisor.stop_pool(state.node_supervisor, pid)

          _ ->
            :ok
        end)
    end
  end

  defp sup_pid(name) when is_atom(name), do: Process.whereis(name)
  defp sup_pid(pid) when is_pid(pid), do: pid

  # Only forward pool opts the caller set explicitly — absent values let
  # `NodeSupervisor.start_pool/2` pick its own defaults (which stay
  # below Aerospike's `proto-fd-idle-ms` for `:idle_timeout_ms`).
  defp maybe_put_pool_opt(opts, _key, nil), do: opts
  defp maybe_put_pool_opt(opts, key, value), do: Keyword.put(opts, key, value)

  ## Helpers

  defp sorted_nodes(nodes) do
    nodes
    |> Map.values()
    |> Enum.sort_by(& &1.name)
  end

  defp maybe_schedule_tend(%{tend_trigger: :manual} = state), do: state

  defp maybe_schedule_tend(state) do
    Process.send_after(self(), :tend, state.tend_interval_ms)
    state
  end

  defp safe_close(transport, conn) do
    transport.close(conn)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp read_ready(meta_tab) do
    case :ets.lookup(meta_tab, :ready) do
      [{:ready, ready?}] -> ready?
      [] -> false
    end
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end
