defmodule Aerospike.Tender do
  @moduledoc """
  Tend-cycle orchestrator and cluster-state public query surface.

  Each cycle composes the following stages, one after the other, through a
  single process so every observable transition is serialised:

    1. Seed the node set from `:seeds` when the cluster view is empty.
    2. Refresh every known node's `partition-generation`, `cluster-stable`,
       and `peers-generation` in one combined info round trip via
       `Aerospike.Node.refresh/2`.
    3. Run peer discovery via `Aerospike.Node.refresh_peers/3` only when at
       least one node's `peers-generation` advanced this cycle (matches the
       reference C/Go/Java trigger).
    4. Verify `cluster-stable` agreement across contributing nodes via
       `Aerospike.PartitionMapMerge.verify_cluster_stable/1`. On
       disagreement, skip the partition-map refresh for this cycle.
    5. When the cluster is stable and a node's `partition-generation`
       advanced past its last applied value, fetch its `replicas` reply via
       `Aerospike.Node.refresh_partitions/2` and merge the segments via
       `Aerospike.PartitionMapMerge.apply_segments/4`.
    6. Recompute the `:ready` meta flag — `true` only when every configured
       namespace has a complete partition map.

  Per-node I/O (info-socket probes, login, peer discovery, partition-map
  fetch) runs through `Aerospike.Node`, which owns the `%Node{}` struct
  plus the pure (given a transport) operations. The Tender retains the
  lifecycle state around each node (status, failures, recoveries, pool,
  counters, histograms) and the cycle orchestration; the Node struct
  carries only the observables the info socket sees.

  Partition-map accumulation (regime guard, ownership merge, agreement
  check) lives in `Aerospike.PartitionMapMerge`. The Tender calls it; it
  never open-codes the merge.

  ETS writes against the cluster-state tables (`owners`, `node_gens`,
  `meta.:ready`) are routed through `Aerospike.PartitionMapWriter`. The
  Tender never mutates those tables itself; every per-node `put_node_gen`,
  `drop_node`, `delete_node_gen`, `apply_segments`, and `recompute_ready`
  is a synchronous call into the writer so a `tend_now/1` observer sees a
  fully-committed cycle.

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
  alias Aerospike.Node, as: ClusterNode
  alias Aerospike.NodeCounters
  alias Aerospike.NodeSupervisor
  alias Aerospike.PartitionMap
  alias Aerospike.PartitionMapMerge
  alias Aerospike.PartitionMapWriter
  alias Aerospike.Policy
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

  @type seed :: {String.t(), :inet.port_number()}
  @type namespace :: String.t()

  @typedoc """
  Start options:

    * `:name` — registered name (required).
    * `:transport` — module implementing `Aerospike.NodeTransport` (required).
    * `:seeds` — list of `{host, port}` tuples (required, non-empty).
    * `:namespaces` — list of configured namespaces the cluster must serve
      before `:ready` flips to `true` (required, non-empty).
    * `:tables` — `%{owners: atom(), node_gens: atom(), meta: atom(), txn_tracking: atom()}`
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
    * `:user` / `:password` — cluster-wide session-login credentials.
      When both are present, the Tender performs a full password login
      on every fresh info socket it opens (seed bootstrap and peer
      discovery), caches the returned session token per node, and
      forwards the token to `NodeSupervisor.start_pool/2` so pool
      workers authenticate via the cached token instead of paying a
      bcrypt round trip. Must be strings or both absent.
  """
  @type option ::
          {:name, GenServer.name()}
          | {:transport, module()}
          | {:seeds, [seed(), ...]}
          | {:namespaces, [namespace(), ...]}
          | {:tables, TableOwner.tables()}
          | {:writer, atom() | pid()}
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
          | {:user, String.t()}
          | {:password, String.t()}

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
  @spec tables(GenServer.server()) :: %{
          owners: atom(),
          node_gens: atom(),
          meta: atom(),
          txn_tracking: atom()
        }
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
    * `:host`, `:port`, and `:connect_opts` — direct-connect details the
      stream path uses to open a dedicated socket for long-lived reads.
  """
  @type node_handle :: %{
          pool: pid(),
          counters: NodeCounters.t(),
          breaker: %{
            circuit_open_threshold: non_neg_integer(),
            max_concurrent_ops_per_node: pos_integer()
          },
          use_compression: boolean(),
          host: String.t(),
          port: :inet.port_number(),
          connect_opts: keyword()
        }

  @doc """
  Returns the pool pid, counters reference, breaker thresholds, and
  direct-connect details for `node_name` in one GenServer hop.

  Replaces the sequence of `pool_pid/2` + `node_counters/2` that the
  command path used previously; commands now pay one round trip per op
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
    name = Keyword.fetch!(opts, :name)
    transport = Keyword.fetch!(opts, :transport)
    seeds = Keyword.fetch!(opts, :seeds)
    namespaces = Keyword.fetch!(opts, :namespaces)
    tables = Keyword.fetch!(opts, :tables)
    writer = Keyword.get(opts, :writer, PartitionMapWriter.via(name))

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

    %Policy.ClusterDefaults{retry: retry_opts} = Policy.cluster_defaults(opts)
    RetryPolicy.put(meta_tab, retry_opts)

    user = Keyword.get(opts, :user)
    password = Keyword.get(opts, :password)

    validate_auth_pair!(user, password)

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
      user: user,
      password: password,
      tables: tables,
      owners_tab: owners_tab,
      node_gens_tab: node_gens_tab,
      meta_tab: meta_tab,
      writer: writer,
      nodes: %{},
      peers_refresh_needed?: false,
      ready?: read_ready(meta_tab)
    }

    cleanup_orphan_pools(state)

    {:ok, maybe_schedule_tend(state)}
  end

  defp validate_auth_pair!(nil, nil), do: :ok

  defp validate_auth_pair!(user, password) when is_binary(user) and is_binary(password),
    do: :ok

  defp validate_auth_pair!(_, _),
    do:
      raise(ArgumentError,
        message: "Aerospike.Tender: :user and :password must both be strings or both be absent"
      )

  @impl GenServer
  def handle_call(:tend_now, _from, state) do
    {:reply, :ok, run_tend(state)}
  end

  def handle_call(:ready?, _from, state) do
    {:reply, state.ready?, state}
  end

  def handle_call(:tables, _from, state) do
    {:reply, state.tables, state}
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
      {:ok,
       %{
         status: :active,
         pool_pid: pool,
         counters: counters,
         node: %ClusterNode{host: host, port: port, session: session, features: features}
       }}
      when is_pid(pool) and not is_nil(counters) ->
        handle = %{
          pool: pool,
          counters: counters,
          breaker: state.breaker_opts,
          use_compression: state.use_compression and MapSet.member?(features, :compression),
          host: host,
          port: port,
          connect_opts: Keyword.put(pool_connect_opts(state, session), :node_name, node_name)
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
      Map.new(state.nodes, fn {name, entry} ->
        {name,
         %{
           status: entry.status,
           failures: entry.failures,
           recoveries: entry.recoveries,
           last_tend_at: entry.last_tend_at,
           last_tend_result: entry.last_tend_result,
           generation_seen: entry.node.generation_seen,
           counters: entry.counters,
           tend_histogram: entry.tend_histogram,
           features: entry.node.features
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
    Enum.each(state.nodes, fn {_, entry} ->
      ClusterNode.close(state.transport, entry.node.conn)
    end)

    :ok
  end

  ## Tend cycle

  defp run_tend(state) do
    :telemetry.span(Telemetry.tend_cycle_span(), %{}, fn ->
      new_state =
        %{state | peers_refresh_needed?: false}
        |> bootstrap_if_needed()
        |> refresh_nodes()
        |> maybe_discover_peers()
        |> maybe_refresh_partition_maps()
        |> recompute_ready()

      {new_state, %{}}
    end)
  end

  # Calls `discover_peers/1` only when any event this cycle flagged that a
  # peer-list refresh is warranted. Matching the C, Go, and Java clients,
  # the trigger fires when at least one active node's `peers-generation`
  # changed this cycle (including the "freshly-seen" sentinel case where
  # the cached generation is `nil`), or when a new node was registered
  # mid-cycle from the seed or peer-discovery paths.
  defp maybe_discover_peers(%{peers_refresh_needed?: false} = state), do: state
  defp maybe_discover_peers(state), do: discover_peers(state)

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
        case PartitionMapMerge.verify_cluster_stable(cluster_stable_contributors(state)) do
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

  # Builds the `%{node_name => hash}` map `PartitionMapMerge.verify_cluster_stable/1`
  # consumes. Inactive nodes are excluded by construction (they cannot serve
  # traffic), as are active nodes whose info call failed this cycle (they
  # carry a nil hash after `clear_cluster_stable/1` zeroed it at cycle start).
  defp cluster_stable_contributors(state) do
    state.nodes
    |> Enum.filter(fn {_, entry} ->
      entry.status == :active and is_binary(entry.node.cluster_stable)
    end)
    |> Map.new(fn {name, entry} -> {name, entry.node.cluster_stable} end)
  end

  # Re-enters seed bootstrap whenever `state.nodes` is empty. In steady
  # state the map is non-empty and this is a `map_size == 0` check — the
  # same cost as a boolean latch. When every node has been dropped (full
  # outage followed by the grace-cycle drop), the next tend cycle re-runs
  # the seed against the configured seeds. A seed that is still dead fails
  # the info probe inside `bootstrap_seed/3` and logs at `:warning`; no
  # state is written and the next cycle retries. The `:seeds` option is
  # validated non-empty in `init/1`, so the "no configured seeds" edge
  # case cannot occur here.
  defp bootstrap_if_needed(%{nodes: nodes} = state) when map_size(nodes) > 0, do: state

  defp bootstrap_if_needed(state) do
    Enum.reduce(state.seeds, state, fn {host, port}, acc ->
      bootstrap_seed(acc, host, port)
    end)
  end

  # Dials `{host, port}` and registers the resulting Node struct, unless
  # the server reports a node name already present in `state.nodes`. The
  # seed list is dialled verbatim from `connect_opts`, so
  # `service-clear-alt` does not belong here — peer discovery is the only
  # place the alternate-services toggle takes effect.
  defp bootstrap_seed(state, host, port) do
    case ClusterNode.seed(
           state.transport,
           host,
           port,
           state.connect_opts,
           auth_opts(state)
         ) do
      {:ok, node} ->
        case Map.fetch(state.nodes, node.name) do
          {:ok, _existing} ->
            ClusterNode.close(state.transport, node.conn)
            state

          :error ->
            register_new_node(state, node, :bootstrap)
        end

      {:error, %Error{} = err} ->
        Logger.warning("Aerospike.Tender: seed #{host}:#{port} bootstrap failed: #{err.message}")

        state

      {:error, :no_node_info} ->
        Logger.warning("Aerospike.Tender: seed #{host}:#{port} missing 'node' info")
        state
    end
  end

  defp auth_opts(state), do: [user: state.user, password: state.password]

  defp register_new_node(state, %ClusterNode{} = node, reason)
       when reason in [:bootstrap, :peer_discovery] do
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

    case ensure_pool(state, node, counters) do
      {:ok, pool_pid} ->
        entry = %{
          node: node,
          status: :active,
          failures: 0,
          recoveries: 0,
          last_tend_at: nil,
          last_tend_result: nil,
          pool_pid: pool_pid,
          counters: counters,
          tend_histogram: tend_histogram
        }

        emit_transition(node.name, :unknown, :active, reason)

        %{
          state
          | nodes: Map.put(state.nodes, node.name, entry),
            peers_refresh_needed?: true
        }

      :error ->
        ClusterNode.close(state.transport, node.conn)
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
        {:ok, entry} -> refresh_node(acc, entry)
        :error -> acc
      end
    end)
  end

  defp clear_cluster_stable(state) do
    nodes =
      Map.new(state.nodes, fn {name, entry} ->
        {name, %{entry | node: ClusterNode.clear_cluster_stable(entry.node)}}
      end)

    %{state | nodes: nodes}
  end

  defp refresh_node(state, entry) do
    case ClusterNode.refresh(entry.node, state.transport) do
      {:ok, updated_node, %{partition_generation: gen, peers_generation_changed?: peers_changed?}} ->
        state = put_node_struct(state, updated_node)
        state = maybe_flag_peers_refresh(state, peers_changed?)
        store_node_gen_if_changed(state, updated_node.name, gen)
        register_success(state, updated_node.name)

      {:error, %Error{} = err} ->
        Logger.debug(
          "Aerospike.Tender: #{entry.node.name} refresh-node info failed: #{err.message}"
        )

        register_failure(state, entry.node.name)

      {:error, :malformed_reply} ->
        register_failure(state, entry.node.name)
    end
  end

  defp maybe_flag_peers_refresh(state, true), do: %{state | peers_refresh_needed?: true}
  defp maybe_flag_peers_refresh(state, false), do: state

  # Replaces the `%ClusterNode{}` embedded in `state.nodes[name]` without
  # touching any lifecycle field. Used by every stage that calls a Node
  # function and expects the updated observables to land back in state.
  defp put_node_struct(state, %ClusterNode{name: name} = node) do
    case Map.fetch(state.nodes, name) do
      {:ok, entry} -> %{state | nodes: Map.put(state.nodes, name, %{entry | node: node})}
      :error -> state
    end
  end

  # Records a successful info reply against the node record. Clears the
  # `failures` counter; if the node was `:inactive`, flips it back to
  # `:active` and increments `:recoveries` so the recovery count survives
  # future transitions. Also stamps `last_tend_at`/`last_tend_result`.
  #
  # Zeroes the per-node `:failed` counter as the tender-side "failure
  # window decay" for the Task 6 circuit breaker. The info socket just
  # answered cleanly, which is the Tender's strongest statement about
  # node health — any pool-path transport failures recorded before this
  # cycle are stale relative to that statement. The breaker in
  # `Aerospike.CircuitBreaker.allow?/2` re-reads `:failed` on every
  # command so the reset is observed immediately by the next caller.
  defp register_success(state, name) do
    now = monotonic_ms()

    update_entry(state, name, fn entry ->
      {status, recoveries} =
        case entry.status do
          :active ->
            {:active, entry.recoveries}

          :inactive ->
            emit_transition(name, :inactive, :active, :recovery)
            {:active, entry.recoveries + 1}
        end

      reset_failed_counter(entry.counters)

      %{
        entry
        | failures: 0,
          status: status,
          recoveries: recoveries,
          last_tend_at: now,
          last_tend_result: :ok
      }
    end)
  end

  defp reset_failed_counter(nil), do: :ok
  defp reset_failed_counter(ref), do: NodeCounters.reset_failed(ref)

  defp register_peers_success(state, name) do
    now = monotonic_ms()

    update_entry(state, name, fn entry ->
      {status, recoveries} =
        case entry.status do
          :active ->
            {:active, entry.recoveries}

          :inactive ->
            emit_transition(name, :inactive, :active, :recovery)
            {:active, entry.recoveries + 1}
        end

      %{
        entry
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

    update_entry(state, name, fn entry ->
      %{entry | failures: 0, last_tend_at: now, last_tend_result: :ok}
    end)
  end

  defp store_node_gen_if_changed(state, name, gen) do
    case PartitionMap.get_node_gen(state.node_gens_tab, name) do
      {:ok, ^gen} -> :ok
      _ -> PartitionMapWriter.put_node_gen(state.writer, name, gen)
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
      update_entry(state, name, fn entry ->
        %{
          entry
          | failures: entry.failures + 1,
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
  # new references via `register_new_node/3`. The `:atomics` term is
  # GC'd once no process retains it.
  defp mark_inactive(state, name) do
    case Map.fetch(state.nodes, name) do
      {:ok, entry} ->
        drop_pool(state, entry)
        PartitionMapWriter.delete_node_gen(state.writer, name)
        PartitionMapWriter.drop_node(state.writer, name)

        cleared_node = %ClusterNode{
          entry.node
          | session: nil,
            generation_seen: nil,
            applied_gen: nil,
            peers_generation_seen: nil
        }

        updated = %{
          entry
          | node: cleared_node,
            status: :inactive,
            failures: 0,
            pool_pid: nil,
            counters: nil,
            tend_histogram: nil
        }

        emit_transition(name, :active, :inactive, :failure_threshold)
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

      {entry, nodes} ->
        drop_pool(state, entry)
        ClusterNode.close(state.transport, entry.node.conn)
        PartitionMapWriter.delete_node_gen(state.writer, name)
        PartitionMapWriter.drop_node(state.writer, name)
        emit_transition(name, :inactive, :unknown, :dropped)
        %{state | nodes: nodes}
    end
  end

  defp update_entry(state, name, fun) do
    case Map.fetch(state.nodes, name) do
      {:ok, entry} -> %{state | nodes: Map.put(state.nodes, name, fun.(entry))}
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
    ordered = state.nodes |> sorted_entries() |> Enum.filter(&(&1.status == :active))

    {state, peers_by_name} =
      Enum.reduce(ordered, {state, %{}}, &collect_peers/2)

    Enum.reduce(peers_by_name, state, fn {_name, peer}, acc ->
      ensure_peer_connected(acc, peer)
    end)
  end

  defp collect_peers(entry, {state, peers_acc}) do
    case ClusterNode.refresh_peers(entry.node, state.transport,
           use_services_alternate: state.use_services_alternate
         ) do
      {:ok, _node, peers} ->
        {register_peers_success(state, entry.node.name), merge_peers(peers_acc, peers)}

      {:error, %Error{} = err} ->
        Logger.debug(fn ->
          key = ClusterNode.peer_info_key(state.use_services_alternate)
          "Aerospike.Tender: #{entry.node.name} #{key} failed: #{err.message}"
        end)

        {register_failure(state, entry.node.name), peers_acc}

      {:error, :malformed_reply} ->
        {register_failure(state, entry.node.name), peers_acc}
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
        connect_and_register_peer(state, name, host, port)
    end
  end

  defp connect_and_register_peer(state, name, host, port) do
    case state.transport.connect(host, port, state.connect_opts) do
      {:ok, conn} ->
        login_and_register_peer(state, name, host, port, conn)

      {:error, %Error{} = err} ->
        Logger.warning(
          "Aerospike.Tender: could not connect to peer #{name} at #{host}:#{port}: #{err.message}"
        )

        state
    end
  end

  # Peers learn their `name` from the parent node's `peers-clear-std` reply
  # rather than from a fresh `node` info probe, matching Go's
  # `node.go:dialNode` behaviour. Only the `features` key is fetched here;
  # `ClusterNode.fetch_features/3` collapses a probe failure to an empty
  # MapSet so the peer still registers.
  defp login_and_register_peer(state, name, host, port, conn) do
    case ClusterNode.login(state.transport, conn, auth_opts(state)) do
      {:ok, session} ->
        features = ClusterNode.fetch_features(state.transport, conn, name)

        peer_node = %ClusterNode{
          name: name,
          host: host,
          port: port,
          conn: conn,
          session: session,
          features: features,
          generation_seen: nil,
          applied_gen: nil,
          cluster_stable: nil,
          peers_generation_seen: nil
        }

        register_new_node(state, peer_node, :peer_discovery)

      {:error, %Error{} = err} ->
        Logger.warning(
          "Aerospike.Tender: peer #{name} at #{host}:#{port} login failed: #{err.message}"
        )

        ClusterNode.close(state.transport, conn)
        state
    end
  end

  ## Partition map refresh

  defp refresh_partition_maps(state) do
    state.nodes
    |> sorted_entries()
    |> Enum.filter(&(&1.status == :active))
    |> Enum.reduce(state, fn entry, acc ->
      case Map.fetch(acc.nodes, entry.node.name) do
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
  defp maybe_refresh_partition_map(state, entry) do
    if partition_map_fetch_needed?(entry.node) do
      fetch_and_apply_replicas(state, entry)
    else
      state
    end
  end

  defp partition_map_fetch_needed?(%ClusterNode{generation_seen: nil}), do: true
  defp partition_map_fetch_needed?(%ClusterNode{applied_gen: nil}), do: true

  defp partition_map_fetch_needed?(%ClusterNode{generation_seen: same, applied_gen: same}),
    do: false

  defp partition_map_fetch_needed?(%ClusterNode{}), do: true

  defp fetch_and_apply_replicas(state, entry) do
    # Measure the full replicas fetch + apply for this node. Per-node
    # latency is the useful operator signal (a single slow node shows up
    # here before it does in the cluster-wide tend_cycle span), so the
    # sample lands in the node's own histogram rather than a cluster
    # aggregate.
    start_native = System.monotonic_time()

    result =
      case ClusterNode.refresh_partitions(entry.node, state.transport) do
        {:ok, _node, segments} ->
          apply_replicas(state, entry, segments)

        {:error, %Error{} = err} ->
          Logger.debug("Aerospike.Tender: #{entry.node.name} replicas failed: #{err.message}")

          register_failure(state, entry.node.name)

        {:error, :malformed_reply} ->
          register_failure(state, entry.node.name)
      end

    record_tend_sample(entry.tend_histogram, System.monotonic_time() - start_native)

    result
  end

  defp record_tend_sample(nil, _duration_native), do: :ok
  defp record_tend_sample(ref, duration_native), do: TendHistogram.record(ref, duration_native)

  # Advances `applied_gen` to the node's `generation_seen` only when every
  # segment is accepted by the regime guard. If any segment is rejected as
  # stale the applied generation stays at its prior value so the next tend
  # cycle refetches `replicas` and retries the merge.
  defp apply_replicas(state, entry, segments) do
    applied? =
      PartitionMapWriter.apply_segments(
        state.writer,
        entry.node.name,
        segments,
        state.namespaces
      )

    state = register_replicas_success(state, entry.node.name)

    if applied? do
      mark_partition_map_applied(state, entry.node.name, entry.node.generation_seen)
    else
      state
    end
  end

  defp mark_partition_map_applied(state, name, gen) do
    update_entry(state, name, fn entry ->
      %{entry | node: ClusterNode.mark_partition_map_applied(entry.node, gen)}
    end)
  end

  ## Ready flag

  defp recompute_ready(state) do
    ready? = PartitionMapWriter.recompute_ready(state.writer, state.namespaces)
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

  defp ensure_pool(%{node_supervisor: nil}, _node, _counters), do: {:ok, nil}

  defp ensure_pool(state, %ClusterNode{} = node, counters) do
    opts =
      [
        node_name: node.name,
        transport: state.transport,
        host: node.host,
        port: node.port,
        connect_opts: pool_connect_opts(state, node.session),
        pool_size: state.pool_size,
        counters: counters,
        features: node.features
      ]
      |> maybe_put_pool_opt(:idle_timeout_ms, state.idle_timeout_ms)
      |> maybe_put_pool_opt(:max_idle_pings, state.max_idle_pings)

    case NodeSupervisor.start_pool(state.node_supervisor, opts) do
      {:ok, pool_pid} ->
        {:ok, pool_pid}

      {:error, reason} ->
        Logger.warning(
          "Aerospike.Tender: could not start pool for #{node.name} at #{node.host}:#{node.port}: " <>
            "#{inspect(reason)}"
        )

        :error
    end
  end

  # Builds the per-worker connect opts for a pool:
  #
  #   * When no creds are configured → verbatim `state.connect_opts`.
  #   * When creds are configured but the info socket did not produce a
  #     session token (server has security disabled, or PKI-anonymous) →
  #     forward `:user` + `:password` so workers can run a full password
  #     login on their own socket.
  #   * When a session token is cached → forward `:user` + `:session_token`
  #     so workers run the cheap AUTHENTICATE path; the password stays in
  #     case the server rejects the token with `:expired_session` and the
  #     worker needs to fall back to a full login.
  defp pool_connect_opts(%{user: nil, password: nil} = state, _session), do: state.connect_opts

  defp pool_connect_opts(%{user: user, password: password} = state, nil) do
    state.connect_opts
    |> Keyword.put(:user, user)
    |> Keyword.put(:password, password)
  end

  defp pool_connect_opts(%{user: user, password: password} = state, {token, _expires_at})
       when is_binary(token) do
    state.connect_opts
    |> Keyword.put(:user, user)
    |> Keyword.put(:password, password)
    |> Keyword.put(:session_token, token)
  end

  # Stops the pool for `entry`. Absence of a pool (no supervisor or a nil
  # pool_pid) is a no-op. `{:error, :not_found}` from the NodeSupervisor
  # is logged at `:debug` — a pool that already exited is an acceptable
  # terminal state.
  defp drop_pool(%{node_supervisor: nil}, _entry), do: :ok
  defp drop_pool(_state, %{pool_pid: nil}), do: :ok

  defp drop_pool(state, %{node: %ClusterNode{name: name}, pool_pid: pool_pid}) do
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

  defp sorted_entries(nodes) do
    nodes
    |> Map.values()
    |> Enum.sort_by(& &1.node.name)
  end

  defp maybe_schedule_tend(%{tend_trigger: :manual} = state), do: state

  defp maybe_schedule_tend(state) do
    Process.send_after(self(), :tend, state.tend_interval_ms)
    state
  end

  defp read_ready(meta_tab) do
    case :ets.lookup(meta_tab, :ready) do
      [{:ready, ready?}] -> ready?
      [] -> false
    end
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)

  # Instant telemetry event fired whenever a node's lifecycle status
  # changes. The `:reason` enum is fixed at the call sites — see the
  # `Aerospike.Telemetry` moduledoc for the contract. Logs stay alongside
  # (operators still see the existing Logger output); this is purely
  # additive.
  defp emit_transition(name, from, to, reason) do
    :telemetry.execute(
      Telemetry.node_transition(),
      %{system_time: System.system_time()},
      %{node_name: name, from: from, to: to, reason: reason}
    )
  end
end
