# Tier 2 — Cluster Correctness — Notes

## Sources Reviewed

- `spike-docs/ROADMAP.md` — Tier 2 scope, exit criteria, explicit
  deferrals to Tier 3/4.
- `aerospike_driver_spike/docs/plans/tier-1-5-pool-hardening/{plan,notes,log}.md`
  — what Tier 1.5 left in place for Tier 2 to build on.
- `aerospike_driver_spike/lib/aerospike/tender.ex` — single-writer
  cluster state, bootstrap/peers/replicas cycle, boolean reachability
  via `failures` counter + `failure_threshold`.
- `aerospike_driver_spike/lib/aerospike/router.ex` — stateless ETS
  reader; today it only reads `:owners` + `:meta.ready`.
- `aerospike_driver_spike/lib/aerospike/node_pool.ex`,
  `lib/aerospike/node_supervisor.ex` — pool checkout path, no in-flight
  counters today, `:idle_timeout_ms` not wired through Tender.
- `aerospike_driver_spike/lib/aerospike/get.ex` — only command path
  today, passes `:timeout` verbatim to `command/3`, no retry, no
  attempt counter.
- `aerospike_driver_spike/lib/aerospike/partition_map.ex` — owners
  table has regime guard; `node_gens` keyed by node_name.
- `aerospike_driver_spike/lib/aerospike/protocol/result_code.ex` —
  `:partition_unavailable` is code 11.
- `official_libs/aerospike-client-go/node.go` — tend requests
  `node`, `peers-generation`, `partition-generation` per node;
  `partitionChanged` bit flips when generation advances.
- `official_libs/aerospike-client-java/client/src/com/aerospike/client/query/QueryValidate.java`
  — reference use of `cluster-stable:namespace=<ns>` is **per-query
  validation**, not a global cluster-key check. Roadmap's "cluster key
  (`cluster-stable`) agreement" refers to the **info key returned by
  `cluster-stable:` with no args**, which is a cluster-wide hash that
  changes on any topology change. Confirmed on the running server:
  `asinfo -v cluster-stable:` returns a short hex string.

## Findings That Shape The Plan

1. **`:tending` is a transition label, not a long-lived state.** The
   roadmap's `:active | :tending | :inactive` implies `:tending` flags
   "refresh in progress". In the spike the tend cycle is single-writer
   and synchronous inside the Tender, so `:tending` is only observable
   during a single cycle. Modelling it as a first-class state adds
   noise unless the router or the pool reads it — neither needs to.
   The implementation collapses to `:active | :inactive` with a
   `last_tend_at`/`last_tend_result` pair on the node record; tests
   that want to observe "tend in progress" use the existing
   `tend_now/1` manual trigger. This is a divergence from the roadmap
   label set that must be recorded here and in `plan.md`.

2. **Cluster-key verification belongs at map-commit, not per-node.**
   Every tended node returns its own `cluster-stable` hash. The
   guard fires only when *every* tended node agrees; a single
   disagreeing node blocks the whole cycle's `PartitionMap.update/5`
   calls. This matches the roadmap wording ("all tended nodes report
   the same cluster key") and avoids the trap of overwriting a
   partition entry from a node whose peers think differently about
   who's in the cluster. Implementation: capture
   `cluster-stable` during the existing `refresh_nodes` info fetch
   (one extra info key per tend), verify agreement before the
   `refresh_partition_maps` stage runs, skip the partition write
   entirely on mismatch.

3. **`partition-generation` tracking is already half-done.** The
   spike already caches `partition-generation` per node and the
   roadmap Tier 2 item ("only re-request the partition map when it
   changes") is a small behavioural tweak on top: today
   `maybe_refresh_partition_map` always re-fetches `replicas`; the
   change is to skip the fetch when `node_gens` has not advanced
   since the last successful replicas apply. Need a second ETS
   column (or a second table) tracking the *last-applied* generation
   distinct from the *last-observed* generation.

4. **Rebalance-class error has two distinct sources.** The roadmap
   frames it as "the map changed while your request was in flight".
   Concretely that is either (a) the server replies
   `PARTITION_UNAVAILABLE` (result code 11) because the addressed
   node no longer owns the partition, or (b) the command succeeds
   but the client's routing decision was made against a stale map
   that has since changed (detected by comparing the `owners`
   entry's `:regime` at pick-time vs. reply-time). For Tier 2 we
   surface (a) only — the server's own "not mine" signal is cheap
   and deterministic; (b) needs a monotonic op-id stamped on the
   routing decision and is deferred unless the retry tests show it
   matters. Record as Phase Boundary and revisit during Task 5.

5. **Circuit breaker state belongs on the node record, not ETS.**
   Tier 1.5's pool never promoted `conn` or pool state into ETS,
   and the roadmap principle "Single-writer ETS discipline" pins
   the Tender as sole writer. The circuit breaker reads in-flight /
   failed counters and the node's `:active | :inactive` label and
   returns a short-circuit verdict. The counters live where the
   writes happen: in-flight / completed are incremented by the
   command path around `NodePool.checkout!`, failures are
   incremented on `{:error, _}` returns. Keep them in :counters or
   :atomics (one reference per node) so the hot path stays
   lock-free; the Tender reads those counters when it decides to
   mark a node `:inactive`. This keeps ETS writer discipline clean
   while letting the command path bump counters without hitting the
   Tender.

6. **Retry policy must consume the rebalance signal, not hide it.**
   Tier 1.5 `Get` returns the transport error as-is. Tier 2's retry
   layer wraps the whole "pick node → checkout → send → parse"
   loop. A rebalance-class error increments `attempt`, re-runs the
   router (which picks the next replica in sequence for reads, or
   re-reads the master after one tend trigger for writes), and
   retries within a caller-supplied total-op budget. Transport
   failures (`:network_error`, `:timeout`) retry the *same replica*
   against a fresh pool worker first, then fall through to the next
   replica. The circuit breaker refuses a checkout before the retry
   loop even picks the node, so retry + breaker share one substrate.

7. **`:idle_timeout_ms` / `:max_idle_pings` are still not plumbed
   through `Tender`.** Tier 1.5 `log.md` Task 6 documented this as
   a deliberate Tier 2 handoff. Any new options this tier adds
   (retry policy, circuit breaker thresholds, tend interval knobs)
   must flow through `Aerospike.start_link/1` → `Tender` →
   `NodeSupervisor.start_pool/2` at the same time. Closing the
   idle-timeout gap is one line per option; batching makes it one
   task, not five.

## Phase Boundaries

- **Tier 3**: telemetry events (including per-node state transitions,
  pool checkout timings, retry events). Tier 2 adds the *data* — the
  state-machine transitions, the counters, the retry attempt count —
  but does not emit `:telemetry.execute/3` calls. Log at `:debug` /
  `:warning` only.
- **Tier 3**: TCP options tuning, auth, TLS. Do not touch
  `Transport.Tcp` connect opts in Tier 2 beyond plumbing already-
  used options through new option paths.
- **Tier 4**: policy structs (`:replica`, `:read_mode_ap`,
  `:send_key`, per-command timeouts beyond `:timeout`). Tier 2 adds
  a `:retry_policy` struct but **only** the fields the retry +
  circuit breaker layer consumes: `:max_retries`,
  `:sleep_between_retries_ms`, `:replica_policy` (`:master | :sequence`
  only; no `:any` or rack-aware variants). Anything richer is Tier 4.
- **Tier 4**: write path (`put`, `delete`, `touch`, etc.). Tier 2
  exercises retry / rebalance logic against the existing GET command
  only. Adding a write just to prove rebalance retry works is out of
  scope — stand up an integration test that forces the server to
  reply `PARTITION_UNAVAILABLE` via `asinfo quiesce` or similar, or
  write a unit test against the Fake.
- **Deferred** (roadmap "Deferred indefinitely"): multi-cluster
  supervision and async request pipelining. Neither affects Tier 2
  task shape.
- **Stale-map rebalance detection** (Finding 4(b)): the "regime at
  pick vs. reply" variant of rebalance detection is deferred until
  an integration scenario forces it. Tier 2 surfaces only the
  server-side `PARTITION_UNAVAILABLE` signal.

## Open Questions To Resolve During Execution

- **Task 2**: where does the cluster-stable hash live? Two
  candidates: (1) an additional ETS table (`cluster_stable` keyed by
  node_name) so the Tender's writer role stays explicit, or (2) the
  in-memory `state.nodes` map. The ETS table is consistent with the
  existing `node_gens` pattern; the in-memory map is cheaper since no
  reader outside the Tender needs the value. Default to in-memory;
  promote to ETS only if Task 6's retry layer needs a lock-free read.

- **Task 4**: `:retry_policy` keyword vs. struct. Tier 4 will grow a
  full policy struct; Tier 2 should not invent an intermediate shape
  that Tier 4 deletes. Plan locks a keyword list at the start-opts
  boundary and a plain map inside the command path, matching the
  shape of existing `connect_opts`. Revisit if Task 4's call sites
  make the keyword form awkward.

- **Task 5**: how to force `PARTITION_UNAVAILABLE` in integration
  tests. Options: (a) `asinfo -v quiesce:` a node to take its
  partitions offline, (b) construct a synthetic ETS entry pointing
  the router at a node that does not own the partition, (c) use the
  Fake transport with a scripted reply. (c) is the lowest-coupling
  approach and is where the unit coverage lives; (a) is the
  integration-level proof and is worth one scenario test. Record
  the decision in `log.md` when Task 5 runs.
