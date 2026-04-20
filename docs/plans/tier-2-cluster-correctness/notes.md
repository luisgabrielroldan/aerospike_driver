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

## Task 8 Execution Findings

- **Single-node kill+restart is a one-way proof.** The plan text framed
  Task 8 as a full round-trip: kill → observe `:inactive` → restart →
  observe return to `:active` → partition map converges within one tend
  cycle. The Tier 2 implementation cannot close that loop against a
  single-node cluster because:
    1. `Aerospike.Tender.bootstrap_if_needed/1` is single-shot (flips
       `bootstrapped?: true` after the first successful seed dial and
       never re-enters).
    2. After the grace-cycle drop, `state.nodes` is empty, so neither
       `refresh_nodes/1` (no nodes to probe) nor `discover_peers/1`
       (no sources to ask for `peers-clear-std`) has a hook to rebuild
       the topology.
    3. The info socket held on the dropped node is closed, so there is
       no surviving channel to promote back to `:active`.
  `Task 1 Execution Findings` already flagged this as a known scope
  gap. `Task 8`'s own "Escalate if" clause converts the gap into a
  deferral rather than a blocker: the outage direction is asserted
  against the live container; the recovery direction is the first
  work item for the next planner (Tier 3 TCP transport re-dial, or a
  supervisor-driven re-bootstrap trigger).

- **Tender bootstrap re-dial is the missing primitive.** The smallest
  scope change that would make Task 8's round-trip testable is a
  Tender policy — on the transition "`state.nodes == %{}` for K
  consecutive cycles while configured seeds exist" — that resets
  `bootstrapped?: false` and re-runs `bootstrap_seed/3` against the
  configured seeds. That change belongs to a later plan because it
  couples to Tier 3's TCP-reconnect scope (what does a reconnected
  info socket cost, and who owns the retry policy on the info-socket
  side versus the pool-worker side). Recorded here as the canonical
  next step for a "cluster auto-recovery" plan.

- **Partition-generation settlement lag after `docker start` is ~1.7 s.**
  Empirical probe: `partition-generation` returns `-1` and `replicas`
  returns an all-zero bitmap for 15-17 × 100 ms after `docker start
  aerospike_spike`. Before that settlement, a newly-started Tender that
  successfully calls `info(conn, ["partition-generation", "replicas"])`
  still fails `Tender.ready?/1` because `PartitionMap.complete?/2`
  rejects the empty-owners reply. Any integration-test teardown that
  restarts the container and hands off to a sibling Tender must wait
  on `partition-generation >= 0`, not just `asinfo -v status ok` —
  this was the root cause of the initial node_kill_test → get_pool_test
  flake. `wait_for_client_ready/3` in the new test encodes the check.

- **Outage-window classification (single-node).** Against the stopped
  container with `failure_threshold: 3`, `tend_interval_ms: 400`,
  `max_retries: 1`:
    * pool workers return `:connection_error` on their first send
      (TCP RST) and `:econnrefused` on subsequent reconnect attempts,
    * `Get.execute/4`'s retry loop classifies both as transport-class
      and exhausts the `max_retries: 1` budget,
    * `Router.pick_for_read/4` continues to return the dead node until
      the Tender's tend cycle clears `owners`; after that clear
      `pick_for_read/4` returns `:cluster_not_ready` instead,
    * `Tender.node_handle/2` returns `{:error, :unknown_node}` once the
      node flips to `:inactive`, and the retry driver treats that atom
      as transport-class (a node that disappeared between pick and
      handle).
  The test accepts all three classification buckets — transport
  `%Error{}`, `:cluster_not_ready`, `:unknown_node` — because the
  exact terminal shape depends on whether the tend cycle ran between
  the `Router.pick_for_read/4` and `Tender.node_handle/2` calls on
  the retry path. All three are valid outage signals for the Tier 2
  acceptance criterion.

- **Test scope is reduced, not watered down.** The integration proof
  asserts every Tier 2 primitive that is actually testable end-to-end
  on a single-node docker server: lifecycle transitions, pool
  teardown on demotion, `owners` clear once the node drops, and the
  retry driver translating outage into typed errors within the
  caller's budget. Unit coverage for the *routing* half of the
  rebalance signal (Task 4's `rebalance?/1`, Task 7's tend-trigger on
  `PARTITION_UNAVAILABLE`) already lives in the Fake-backed tests.
  Combined, they are the exit-criteria proof the plan requires; the
  explicit gap (recovery) is documented both here and in the new
  test's `@moduledoc`.

## Task 3 Execution Findings

- **`applied_gen` lives in-memory, not in ETS.** The plan defaulted to
  option 1 (extend `node_gens` entries to a compound value). That option
  would change the return shape of `PartitionMap.get_node_gen/2` — which
  the `partition_map_test.exs` table tests, `tender_test.exs` assertions,
  and the general `node_gens` contract all depend on as `{:ok, integer()}`.
  Option 2 (add a second ETS table) is overkill for a value only the
  Tender reads. The implementation therefore stores `applied_gen` on the
  Tender's `state.nodes` map alongside `cluster_stable` (Task 2's same
  choice). If Task 6's circuit breaker ever needs a lock-free read of
  `applied_gen`, promote it then — but the retry layer does not need it
  either (it checks the rebalance-class error, not applied generation).

- **Failure-counter accounting shifted.** With the short-circuit in
  place, a tend cycle where `partition-generation` fetch fails cannot
  advance `generation_seen`; `applied_gen` is therefore equal to the
  previous cycle's `generation_seen` (or nil); the replicas fetch is
  skipped. Net effect: a fully-failing cycle contributes **2** failure
  events (refresh-node info + peers-clear-std) instead of **3**. Task 2
  already absorbed the combined refresh-node info call (1 event for
  what used to be 2); Task 3 subtracts another one. Any future test
  that budgets cycles against a `failure_threshold` must compute
  events-per-cycle as 2, not 3. `tender_test.exs` and
  `tender_pool_test.exs` were updated to match (thresholds lowered
  from 5 to 3 in both tests).

- **Short-circuit conditions.** `partition_map_fetch_needed?/1` returns
  true on any of:
    1. `generation_seen: nil` — info has never succeeded for this node
       (fresh peer or post-recovery)
    2. `applied_gen: nil` — replicas have never been successfully
       applied since the last `:inactive` transition or registration
    3. `generation_seen != applied_gen` — generation advanced past the
       last applied value
  Equal non-nil values are the only case where the fetch is skipped.

- **Stale-regime rollback.** When `apply_replicas/3` runs, every
  `merge_replica/6` call returns `true` (applied) or `false` (rejected
  as stale). `apply_replicas/3` folds those into a single `applied?`
  boolean; only when every segment applied does `applied_gen` advance
  to `generation_seen`. This preserves the regime guard's pre-existing
  behaviour while keeping the short-circuit coherent: if the whole
  reply was stale, the next cycle refetches and tries again.

- **New peers discovered mid-cycle still fetch replicas.** A peer
  added by `discover_peers/1` has `generation_seen: nil` and
  `applied_gen: nil`. `partition_map_fetch_needed?/1` returns true
  via the first clause, so the `refresh_partition_maps/1` iteration
  that follows in the same cycle fetches `replicas` for the new peer.
  The peer's `applied_gen` will be set to `nil` (because
  `generation_seen` is also nil at this point), meaning the next
  tend cycle will also fetch replicas — that is one "extra" fetch
  on the cycle after discovery, which is harmless and matches the
  pre-Task-3 behaviour (always fetch on first cycle with a known
  connection).

## Task 2 Execution Findings

- **Combined refresh-node info call.** `refresh_nodes/1` now calls
  `info(conn, ["partition-generation", "cluster-stable"])` as one combined
  request instead of separate calls (module attribute
  `@refresh_node_info_keys`). Tests script that exact list. Task 3's
  generation short-circuit must hook into this combined reply: the gen
  value is read from the same handler clause that captures the cluster-
  stable hash. Splitting the calls back apart would force re-scripting
  every existing test and lose the round-trip saving documented in the
  reference clients (Java/Go batch info keys per node tend pass).

- **Hash storage is in-memory on the node record.** The cluster-stable
  field lives only on the Tender's `state.nodes` map (not ETS, per the
  Open Question default). It is reset to `nil` at the start of every
  `refresh_nodes/1` call so only nodes that successfully replied this
  cycle contribute to the agreement check. A node that reverts to
  `failures > 0` without producing a new hash has `cluster_stable: nil`
  and is implicitly excluded — `verify_cluster_stable/1` filters on
  `is_binary(node.cluster_stable)`. If Task 6's circuit breaker ever
  needs a lock-free read of the hash, promote to ETS — but until then
  the in-memory placement keeps single-writer discipline trivial.

- **Empty contributors is `{:ok, :empty}`, not a disagreement.** A
  cluster with zero `:active` nodes (full outage, bootstrap failure,
  or every node failed this cycle's info call) returns `{:ok, :empty}`
  from `verify_cluster_stable/1`. The pipeline proceeds to
  `refresh_partition_maps/1`, which is a no-op because the same filter
  excludes inactive / nodeless states. This avoids a noisy warning at
  startup and on real outages.

- **Disagreement preserves prior owners and does not flip status.** The
  cluster-stable guard is a *write-stage* decision, not a node-health
  signal. Disagreeing nodes stay `:active` (they each replied
  successfully — they just disagree on cluster topology). The disagree
  warning is logged once per cycle at `:warning`. Task 7's retry layer
  treats neither side as "bad" — it walks replicas using the existing
  partition map until both agree on the next cycle.

- **No new ETS table.** This task added no ETS columns. Task 3 should
  decide between extending `node_gens` to a compound value or adding a
  new table; this task touched neither.

## Task 1 Execution Findings

- **Grace-cycle semantics are single-shot.** The plan proposed "a
  *second* consecutive threshold breach" as the trigger for the
  `:inactive` → removed transition. Task 1's implementation collapses
  that to "any failure while `:inactive`". Rationale: only
  `refresh_nodes/1` probes inactive nodes; `discover_peers/1` and
  `refresh_partition_maps/1` explicitly skip them (otherwise an inactive
  node would be asked for `peers-clear-std` / `replicas` it cannot
  answer, inflating the failure count noisily). With only one stage
  running per tend cycle for inactive nodes, "second threshold breach"
  and "any single failure" collapse to the same event so long as
  `failure_threshold >= 1`. Future retry / circuit-breaker tasks must
  not assume a multi-cycle grace window on the inactive side.

- **`refresh_nodes/1` still probes `:inactive` nodes.** This is the
  sole recovery path — a successful `partition-generation` reply flips
  `:inactive` → `:active` and increments `:recoveries`. If the info
  socket (`node.conn`) is itself dead (e.g. server closed the TCP FD),
  the probe fails and the node is dropped outright. Reconnection of
  the info socket is **not** implemented in Task 1 — it is out of
  scope and a real recovery depends on the TCP transport eventually
  being redialled via bootstrap / peer discovery. Flag this for Tier 3
  TCP/auth scope if production recovery turns out to need it.

- **Router reads `:owners` only.** Task 1 kept the `:inactive` status
  invisible to ETS readers — it lives on `state.nodes` (the Tender's
  in-memory map). `mark_inactive/2` clears the node's `owners` and
  `node_gens` rows so the Router implicitly stops routing to inactive
  nodes without needing a status-aware guard. The `pool_pid/2` guard
  handles the narrow window between ETS clear and a concurrent caller
  still holding a stale node name.

- **Test hygiene fix: `stop_process/1` races.** The previous version
  checked `Process.alive?/1` before `GenServer.stop/3`, which is racy
  when the process is linked to the test process: the parent dying
  tears down linked children between the check and the stop call.
  Wrapped `GenServer.stop/3` in `try/catch` for `:exit`. Not a
  production code change, but relevant for any future tests that take
  the same shape.

- **`nodes_status/1` is the new diagnostic entry point.** Tests and
  the (Task 6) circuit breaker will read from it, but it is a
  `GenServer.call/2` — not suitable for the hot path. Counter-backed
  reads (Task 5) are the hot-path mechanism.

## Open Questions To Resolve During Execution

- **Task 2**: ~~where does the cluster-stable hash live?~~ Resolved
  during Task 2: in-memory on the node record. Promote to ETS only if
  Task 6's retry layer needs a lock-free read.

- **Task 3**: ~~`node_gens` compound value vs. second ETS table for
  `applied_gen`?~~ Resolved during Task 3: neither. Stored in-memory on
  `state.nodes` alongside `cluster_stable`. See "Task 3 Execution
  Findings" above for rationale.

- **Task 4**: `:retry_policy` keyword vs. struct. Tier 4 will grow a
  full policy struct; Tier 2 should not invent an intermediate shape
  that Tier 4 deletes. Plan locks a keyword list at the start-opts
  boundary and a plain map inside the command path, matching the
  shape of existing `connect_opts`. Revisit if Task 4's call sites
  make the keyword form awkward.

- **Task 5**: ~~how to force `PARTITION_UNAVAILABLE` in integration
  tests.~~ Not exercised in Task 5. Task 5 only wired counters and
  their writer discipline; no integration scenario in this task
  produces `PARTITION_UNAVAILABLE`. Unit coverage via the Fake
  transport proved the `rebalance?/1` bit path in Task 4. Defer the
  live-server proof to Task 8.

- **Task 6**: ~~`:max_concurrent_ops_per_node` default factor.~~
  Resolved during Task 6: `pool_size * 10`. The breaker is a backstop
  for wedged pools rather than a steady-state cap (NimblePool already
  queues), so a generous multiplier is correct. Revisit in Tier 4 if
  policy-struct work lands a first-class concurrency cap (e.g. a
  `:max_concurrent` field on a read policy) that the breaker should
  honour verbatim instead of a derived default.

## Task 6 Execution Findings

- **`:circuit_open` at result-code -28.** Added to
  `Aerospike.Protocol.ResultCode` so the client-side negative-code
  table stays consistent (previous client codes are `:cluster_not_ready`
  at -27, `:parse_error` at -26, etc.). The breaker emits
  `%Error{code: :circuit_open}`; `Error.rebalance?/1` returns `false`
  for it so Task 7's retry driver classifies a breaker trip as
  transport-class (re-pick a different replica, not the same one).

- **`node_handle/2` over two separate accessors.** `Aerospike.Tender`
  grew a new public `node_handle/2` that returns `%{pool: pid,
  counters: ref, breaker: opts}` in one GenServer call. The alternative
  — having `Aerospike.Get.execute/4` call `pool_pid/2` and
  `node_counters/2` sequentially — would double the GenServer hops on
  the hot path and make the breaker-check ordering non-atomic w.r.t.
  concurrent node transitions. The existing `pool_pid/2` and
  `node_counters/2` remain in place; tests that already use them were
  not disturbed.

- **`:failed` decay is tied to `register_success/4`.** That is the
  single spot where a node's combined `partition-generation` +
  `cluster-stable` info call lands cleanly. Decaying on any other hook
  (e.g. after a command completes) would couple the breaker to
  command-path timing instead of the cluster-state view — a node that
  "looks fine" from one perspective but is churning from another should
  fail closed, not open. The guard `reset_failed_counter(nil)`
  handles the cluster-state-only Tender path (no pool, no counters).

- **Breaker is called after `node_handle/2`, before the pool.** Order
  matters: the breaker needs a valid counters ref (so it must follow
  `node_handle/2`) but must refuse before NimblePool's `checkout!/3`
  (so the pool never sees a checkout it would have to immediately
  close). A refusal returns `{:error, %Error{code: :circuit_open}}`
  and never touches the pool.

- **Combined-cap reporting order is pinned.** When both thresholds
  are breached simultaneously, `cond` reports the failure counter
  first. The `CircuitBreakerTest` "reports the failure cap first"
  test pins that order so downstream logs stay stable; if Task 7's
  retry driver ever wants to distinguish the two dimensions by
  message string, it can rely on the "failure counter" / "in_flight"
  substring pattern.

- **Default thresholds.** `:circuit_open_threshold = 10` matches the
  node's existing `:failure_threshold` default, so a cluster-side
  demotion trips at roughly the same budget as a breaker trip.
  `:max_concurrent_ops_per_node = pool_size * 10`: the multiplier is
  intentionally generous because Tier 2's counter set leaves
  `:queued` reserved; the concurrency check effectively reads
  in-flight only and must not interfere with a busy but healthy node
  whose operations legitimately fan out. Revisit in Tier 4 policy
  work if a caller needs a tighter cap.

- **Handoff for Task 7.** `Get.execute/4` now surfaces
  `{:error, %Error{code: :circuit_open}}` as a peer of the transport
  errors. The retry driver should:
    1. Classify `:circuit_open` as transport-class (not rebalance).
    2. Re-pick a different replica (not the same node).
    3. NOT trigger `tend_now/1` on a breaker trip — the breaker
       already reflects live counters; the Tender's clock will
       decay the `:failed` slot on the next successful cycle.
  No scope change to the plan's Task 7 Guidance is needed; the
  "circuit-open refusal counts as a retryable transport-class event"
  test the plan calls out is already implementable.

## Task 5 Execution Findings

- **`@failure_codes` diverges from the plan list.** Plan text listed
  `[:network_error, :timeout, :connection_closed]`. The codebase does
  not emit `:connection_closed` anywhere — the TCP transport surfaces
  closed sockets as `%Error{code: :connection_error}`. The allow-list
  in `NodeCounters` therefore uses `[:network_error, :timeout,
  :connection_error]`. Task 6's breaker reads via
  `NodeCounters.failure?/1` so it inherits the same classification.

- **`:queued` stays reserved.** The slot is allocated but never
  written in Tier 2. The circuit breaker in Task 6 will decide
  whether the queue depth read must be lock-free (`:counters.get/2`)
  or can tolerate the `:sys.get_state(pool)` cost. If the breaker
  reads queue depth on the hot path, Task 6 needs to either (a) add
  writes to `:queued` inside NimblePool's queue enter/exit paths
  (which means patching a custom pool implementation — NimblePool
  does not expose queue-depth callbacks) or (b) accept the GenServer-
  call read. Recommend (b): breaker decisions run once per
  `Get.execute/4` call, not per packet, so one `:sys.get_state/1` per
  request is within budget.

- **`{:close, :failure}` is the new checkin-protocol tuple.** The
  existing `conn` and `:close` checkins kept their semantics. The new
  tuple is the pool-internal way for `Get` (and any future command
  path) to say "drop the worker AND bump `:failed`". If the pool ever
  needs more failure dimensions (e.g. a distinct "parse error" vs.
  "transport error"), extend the tuple rather than growing a second
  checkin verb — the pattern match in `handle_checkin/4` makes that
  cheap.

- **Counter ref lifecycle matches node lifecycle.** A node's counter
  ref is allocated in `register_new_node/5` (only when
  `:node_supervisor` is present) and nilled on `mark_inactive/2`. The
  `:counters` term does not support explicit free; it is GC'd once no
  process holds it. The pool worker process that holds the counters
  in its `pool_state` is the longest-lived holder; when the pool
  shuts down (via `drop_pool/2` inside `mark_inactive/2`) the
  `pool_state` goes out of scope and the counters term follows
  naturally.

- **Test synchronisation quirk.** `NimblePool.checkout!/4` sends the
  `:checkin` message via raw `send/2` and returns before the pool
  processes it. Tests that assert on counters directly after the
  checkout must call `:sys.get_state(pool)` (or use the test module's
  `wait_until/3` helper) to drain the mailbox first. This is a
  general NimblePool property, not a Tier 2 artefact; future tests
  that read atomics/counters after a checkout return should follow
  the same pattern.

## Task 7 decisions

- **Retry policy lives in `:meta` ETS under `:retry_opts`.** The
  Tender validates the caller's start opts once (`RetryPolicy.from_opts/1`
  raises on bad input) and writes the resulting map to `:meta` during
  `init/1`. The command path reads the policy lock-free via
  `RetryPolicy.load/1` and merges per-call overrides via
  `RetryPolicy.merge/2`. Only the Tender writes this slot — matching
  the single-writer discipline that governs every other `:meta`
  entry. `load/1` falls back to `RetryPolicy.defaults/0` when the
  slot is absent so cluster-state-only test harnesses that skip
  Tier 2's retry init keep working without a shim.

- **`RetryPolicy.rebalance?/1` accepts both `{:error, Error.t()}` and
  a bare `%Error{}`.** `transport?/1` only makes sense on the tuple
  form (it needs to confirm the error path), but `rebalance?/1` is
  useful on a raw struct too (e.g. the unit tests that want to check
  classification without a tuple wrapper). The Get command path calls
  `RetryPolicy.rebalance?(result)` with the full
  `{:error, %Error{}}` tuple; the Error module's bare-struct API
  stays intact underneath.

- **Routing atoms are fatal with no retry.** `Router.pick_for_read/4`
  can return `{:error, :cluster_not_ready}` or `{:error, :no_master}`.
  The retry driver returns these verbatim (preserving the Tier 1.5
  surface) rather than retrying: no replica exists to target, so
  re-picking would just burn attempts against the same empty state.
  `{:error, :unknown_node}` from `Tender.node_handle/2`, by contrast,
  *is* retried as transport-class — it means the node disappeared
  between pick and handle, and the next `pick_for_read/4` should land
  elsewhere.

- **Rebalance retry triggers `tend_now` via unlinked `spawn/1`.**
  `Task.start_link` would tie the tend's fate to the caller's. A
  transient tend failure should not propagate. The spawned process
  is discarded; if the tend fails, the next retry either hits the
  same rebalance (budget burns out, `{:error, last_error}`) or sees
  the updated map. `try/catch :exit` inside the spawn absorbs the
  usual GenServer-failure signals without logging noise.

- **`:replica_policy` default flipped from `:master` to `:sequence`.**
  Tier 1.5 hard-coded `:master` in `Get.execute/4`. Tier 2's retry
  driver needs the replica walk as the substrate that transport
  retries ride on, so the default flips. No Tier 1 invariant test
  encoded the old default (Router tests pass the policy explicitly),
  so the flip landed without escalation. Callers that need the old
  behaviour set `replica_policy: :master` either at
  `Aerospike.start_link/1` or per call.

- **Retry loop uses a `ctx` map to keep helper arities sane.** Credo
  flagged three arity-9 helpers on the first pass. Bundling the
  invariants (`tender`, `tables`, `transport`, `policy`, `key`,
  `deadline`) into a single map threaded through every helper drops
  the loop to 3-param helpers with no behavioural change. Future
  additions (e.g. a new classification cue) can extend the ctx
  rather than growing helper arities.

- **Pool-level errors are transport-class.** The transport codes list
  includes `:pool_timeout` and `:invalid_node` alongside the socket
  codes (`:network_error`, `:timeout`, `:connection_error`) and the
  breaker refusal (`:circuit_open`). The pool-level codes surface
  from `NodePool.checkout!/3` without touching a socket, but from the
  retry driver's point of view they are still "the command did not
  reach a node that answered cleanly" — re-dispatching against a
  different replica is the right move.
