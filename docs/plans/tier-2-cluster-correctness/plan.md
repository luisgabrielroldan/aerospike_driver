# Tier 2 — Correctness Under Cluster Churn

- **Status**: pending

## Objective

Build the cluster-state substrate that retry and the circuit breaker
need. Replace the current boolean reachability with an explicit per-
node lifecycle; make partition-map refresh cheap in steady state by
keying off `partition-generation`; refuse to commit a partition map
update when tended nodes disagree on the cluster's `cluster-stable`
hash; surface a distinct "rebalance" error class so a retry layer
can act on it. Add per-node in-flight / queued / failed counters and
the circuit breaker that reads them. Build retry on top of all of
this so GET survives a node going away mid-request within the
caller's total-op budget.

Everything lands in `aerospike_driver_spike/`. The exit criteria are
the roadmap's: a node killed mid-traffic transitions to `:inactive`,
its pool stops, in-flight requests see a rebalance-class error that
the retry layer handles, the partition map converges within one tend
cycle, and a cluster-stable mismatch does not poison ETS.

## Constraints

- Keep changes inside `aerospike_driver_spike/`. Do not touch the
  `aerospike_driver/` release repo.
- Preserve single-writer ETS discipline. The Tender remains the only
  process that writes to `owners`, `node_gens`, `meta`, and any new
  ETS table added in this tier. Counters may live in `:counters` /
  `:atomics` references updated by the command path — those are not
  ETS and do not violate the rule, but document each counter's writer
  explicitly in module docs.
- `NodeTransport` stays the seam. If a tier-2 task demands a new
  transport capability, grow the behaviour; do not bypass it. Tier 2
  is expected to *not* add a new transport callback — the rebalance
  signal is already visible through the existing `command/3` return.
- No backwards-compatibility shims between tiers. Rename or reshape
  freely. If `Aerospike.Get.execute/4`'s option shape changes, update
  tests and `Aerospike.get/3` in lockstep.
- No new features deferred to later tiers: no telemetry events (log
  only), no TCP / auth / TLS work, no new commands beyond GET, no
  policy struct beyond what retry / circuit breaker reads (see
  `notes.md` "Phase Boundaries").
- Follow `.claude/skills/coding-standards/SKILL.md`: `mix format`,
  `mix credo --strict`, `mix compile --warnings-as-errors`, alphabetised
  aliases, underscored integer literals, typed `Aerospike.Error` returns.
- Batch-plumb every new start option through `Aerospike.start_link/1`
  → `Tender` → `NodeSupervisor.start_pool/2`. Close the
  `:idle_timeout_ms` / `:max_idle_pings` gap that Tier 1.5 left open
  at the same time (see `notes.md` Finding 7).

## Assumptions

- Roadmap's `:active | :tending | :inactive` label set is shorthand.
  `:tending` is a transient observable inside one synchronous tend
  cycle, not a persistent state; plan implements `:active |
  :inactive` + a `last_tend_at`/`last_tend_result` pair on the node
  record (`notes.md` Finding 1). This divergence is recorded here so
  later auditors do not treat it as drift.
- `cluster-stable:` with no args returns a cluster-wide hash that
  changes on any topology transition. Verified on the running spike
  server. The Tender captures it per-node and verifies agreement
  before committing partition-map writes (`notes.md` Finding 2).
- `partition-generation` per node is already cached in ETS under
  `node_gens`. Tier 2 adds a parallel "last-applied" generation so
  replicas fetching can short-circuit when the node's generation has
  not advanced since the last successful apply (`notes.md` Finding 3).
- Server-side `PARTITION_UNAVAILABLE` (result code 11) is the
  primary rebalance-class signal. Regime-at-pick vs. regime-at-reply
  detection is deferred (`notes.md` Phase Boundaries). If an
  integration test during Task 5 demonstrates the deferral is wrong,
  escalate; do not grow the scope in-task.
- Per-node counters (in-flight, queued, failed) live in `:counters`
  references owned by the Tender and incremented by the command
  path. The circuit breaker reads them. No GenServer call on the hot
  path (`notes.md` Finding 5).
- Cluster integration scenarios for node failure can be forced via
  the existing single-node `docker compose up -d` by stopping and
  restarting the container, or — for `PARTITION_UNAVAILABLE` — by
  driving a scripted Fake transport. Tier 2 does **not** build a
  multi-node harness (Option A in `spike-docs/integration-testing-
  harness.md`). If a scenario genuinely requires a second live node,
  escalate before adding docker-compose profiles.

## Tasks

### Task 1: Replace boolean reachability with per-node lifecycle
- **Status**: pending
- **Goal**: The Tender's `state.nodes` entry carries an explicit
  `:status` field (`:active | :inactive`) plus `last_tend_at`,
  `last_tend_result`, and a monotonic `:generation_seen` counter.
  Transitions are the only writes to `:status`; the `failures`
  counter remains the input to the transition.
- **Prerequisites**: none
- **Scope**: `lib/aerospike/tender.ex` (node record shape,
  `register_failure/2`, `drop_node/2`, bootstrap paths,
  `handle_call(:pool_pid, ...)`), new tests in
  `test/aerospike/tender_test.exs`.
- **Non-goals**: No ETS surface for node status in this task — the
  Router does not need it yet. No new tend callback ordering. No
  work on cluster-stable, partition-generation short-circuit, or
  counters. No circuit breaker wiring (that is Task 6). No
  `:tending` observable state (see `notes.md` Finding 1).
- **Guidance**: The current implementation drops a node as soon as
  `failures >= failure_threshold`. Change `drop_node/2` to call a
  new `mark_inactive/2` that:
    1. Flips the node record's `:status` to `:inactive`.
    2. Stops the node's pool via the existing `drop_pool/2` path
       (preserve current pool lifecycle — `restart: :temporary`
       means the pool does not come back automatically).
    3. Leaves the node in `state.nodes` for one more tend cycle so
       a subsequent successful info fetch can flip it back to
       `:active`. Only a *second* consecutive threshold breach or
       a dropped-from-peers signal removes the entry entirely.
  Reset `failures` to 0 and bump a `:recoveries` counter on a
  successful info reply from an `:inactive` node. Router's
  `pick_for_*` functions still read only ETS owners, so they
  implicitly exclude inactive nodes (their replicas already
  cleared from owners). Add a guard inside
  `handle_call({:pool_pid, name}, ...)` that returns
  `{:error, :unknown_node}` when the node is `:inactive` — this is
  where Tier 2's short-circuit lives.
  Tests must cover: active → inactive on threshold breach,
  inactive → active on successful partition-generation fetch before
  the drop cycle, pool stopped on the active → inactive flip,
  `pool_pid/2` returns `{:error, :unknown_node}` for inactive.
- **Escalate if**: the Router or the command path needs to read
  `:status` directly. That would push state into ETS and change the
  task shape; revisit with the user before adding a new ETS table.
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/tender_test.exs`
  - Tier 1 / 1.5 invariant tests unchanged:
    `mix test test/aerospike/tender_pool_test.exs test/integration/get_pool_test.exs test/aerospike/node_pool_test.exs`

### Task 2: Cluster-stable agreement guard
- **Status**: pending
- **Goal**: Before any `PartitionMap.update/5` write, the Tender
  verifies that every `:active` tended node returned the same
  `cluster-stable` hash this cycle. Disagreement aborts the write
  stage and retries next cycle; the `:owners` ETS table is left
  untouched.
- **Prerequisites**: Task 1 (the guard skips inactive nodes — that
  classification must exist first).
- **Scope**: `lib/aerospike/tender.ex` (add a `refresh_cluster_stable/1`
  stage before `refresh_partition_maps/1`; capture hash per node
  during `refresh_nodes/1`), `lib/aerospike/protocol/info.ex` if the
  info key list is centralised, tests in `test/aerospike/tender_test.exs`.
- **Non-goals**: No changes to how `replicas` is parsed or stored.
  No persistent cluster-key cache (Finding 2 default: in-memory
  only). No per-namespace `cluster-stable:namespace=<ns>` — that is
  a query-validation primitive (Finding in `notes.md` sources) and
  belongs to Tier 4's scan/query work. No cluster-key agreement
  enforcement at pool checkout or command time.
- **Guidance**: Add `cluster-stable` to the info key list fetched
  during `refresh_nodes/1`. Store the returned hash on the node
  record. After `refresh_nodes/1` completes, call a new
  `verify_cluster_stable/1` that reads every `:active` node's hash
  and either returns `{:ok, hash}` or `{:error, :disagreement,
  per_node_hashes}`. On disagreement, `run_tend/1` logs at
  `:warning` and skips `refresh_partition_maps/1` entirely. On
  agreement, the pipeline proceeds; the agreed hash is stashed on
  `state` only for logging.
  Edge cases:
    - Zero active nodes (bootstrap failure or full outage): guard
      returns `{:ok, :empty}` so the pipeline proceeds (there is
      no one to disagree); `refresh_partition_maps/1` will be a
      no-op anyway.
    - Exactly one active node: `{:ok, hash}` trivially.
    - A node that returned `cluster-stable` but failed
      `partition-generation`: excluded from the hash set (treat as
      inactive for this tick; it will retry next cycle).
  Tests must cover: all-agree (map updates), one-disagree (no
  update, warning logged, `PartitionMap` entries unchanged), empty
  (bootstrap failure — pipeline proceeds). Use the Fake transport
  with scripted info replies.
- **Escalate if**: the agreement check needs to become per-partition
  rather than per-cluster (would indicate the spike's partition map
  model is wrong, not a Tier 2 fix).
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/tender_test.exs`
  - `mix test test/aerospike/partition_map_test.exs` (regime guard
    must still behave as before).

### Task 3: Skip partition-map fetch when generation has not advanced
- **Status**: pending
- **Goal**: `maybe_refresh_partition_map/2` fetches `replicas` only
  when the node's observed `partition-generation` is greater than
  the last-applied generation. Steady-state tend cycles become
  O(#nodes) info calls for `cluster-stable` +
  `partition-generation`, not O(#nodes) + O(#nodes × #namespaces)
  partition-map parses.
- **Prerequisites**: Task 2 (cluster-stable guard lives between
  generation fetch and replicas fetch; Task 3 short-circuits the
  replicas fetch, which sits after the guard).
- **Scope**: `lib/aerospike/tender.ex` (record last-applied
  generation on the node; short-circuit `maybe_refresh_partition_map/2`
  when unchanged), `lib/aerospike/partition_map.ex` (add
  `put_applied_gen/3` / `get_applied_gen/2` or equivalent — decide
  whether to reuse `node_gens` with a compound value or add a
  second table; see Guidance), tests in
  `test/aerospike/tender_test.exs`.
- **Non-goals**: No changes to the replicas parser, regime guard,
  or `PartitionMap.update/5` contract. No cross-node generation
  comparison (node A advancing does not force a refresh of node B).
  No skipping the generation fetch itself.
- **Guidance**: Two last-applied storage options:
    1. Extend `node_gens` entries from `{name, gen}` to
       `{name, %{observed: gen, applied: gen_or_nil}}`. Simpler but
       changes the existing helper signatures.
    2. Add `partition_applied_gens` as a third ETS table owned by
       the Tender. Keeps the boolean-simple `node_gens` contract
       other code (tests, Router symmetry) already reads.
  Default to option 1 because the current `node_gens` has exactly
  one reader (the Tender) and the extra field is invisible outside
  the module. Either way the field must be initialised to `nil`
  for a newly-discovered node so the first cycle always fetches
  replicas, and must be cleared on `drop_node/2`.
  The short-circuit logic: fetch partition-generation as today; if
  the observed value equals the stored `applied` value, skip the
  replicas info call *and* do not reset the node's `failures`
  counter via the replicas path (the partition-generation success
  already did). On a successful replicas apply, set `applied =
  observed` atomically with the `PartitionMap.update/5` writes.
  A failed apply (any segment returns `{:error, :stale_regime}`
  from the regime guard) must not advance `applied` — fall through
  to retry on the next cycle.
  Tests must cover: first cycle always fetches replicas; second
  cycle with unchanged generation skips replicas (assert via Fake
  `{:info, ...}` call count); second cycle with advanced generation
  fetches replicas; `applied` rolls back when `PartitionMap.update/5`
  rejects a segment as stale.
- **Escalate if**: the existing `PartitionMap` table ownership or
  writer-process model has to change to make the applied-gen
  tracking coherent.
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/tender_test.exs test/aerospike/partition_map_test.exs`

### Task 4: Rebalance-class error surfacing
- **Status**: pending
- **Goal**: When the server replies with result code 11
  (`PARTITION_UNAVAILABLE`), `Aerospike.Get` surfaces a distinct
  error value the retry layer can pattern-match on, rather than a
  generic `%Aerospike.Error{code: :partition_unavailable}` that
  looks transport-like. The distinction is the retry layer's cue
  to re-route rather than re-send against the same replica.
- **Prerequisites**: Tasks 2 and 3 (the partition map refresh
  lifecycle is stable; otherwise the retry test's re-route path
  becomes fragile).
- **Scope**: `lib/aerospike/error.ex` (consider adding a
  `rebalance?/1` helper or a tagged variant), `lib/aerospike/get.ex`
  (classification in `do_get/4`), `lib/aerospike/protocol/response.ex`
  if result-code classification lives there, tests under
  `test/aerospike/` and `test/integration/`.
- **Non-goals**: No retry logic (Task 6). No regime-at-pick vs.
  regime-at-reply detection (deferred — `notes.md` Phase
  Boundaries). No new transport-layer error atoms. No changes to
  `Aerospike.Error`'s struct shape beyond a boolean flag or
  classification helper.
- **Guidance**: Current `Response.parse_record_response/2` returns
  a plain `%Aerospike.Error{code: :partition_unavailable}` when the
  server replies with result code 11. That is enough bits to
  classify downstream — prefer a helper `Error.rebalance?(err)`
  that matches `:partition_unavailable` (and any future rebalance-
  class codes identified in reference clients — `notes.md` should
  be updated if more are found during implementation) over adding a
  new struct field. The retry layer calls `rebalance?/1` to decide
  between "re-route" and "retry same replica". Get's own contract
  stays `{:ok, Record.t()} | {:error, Error.t() | atom()}`.
  Tests must cover: Fake scripted reply with result code 11 turns
  into an error for which `rebalance?/1` returns `true`; other
  server errors (`:key_not_found`, `:timeout`, arbitrary transport
  failure) return `false`. No integration test needed unless the
  Fake-based coverage is insufficient.
- **Escalate if**: the result-code table in the spike disagrees
  with the reference clients about which codes are rebalance-class
  (audit all of codes 2, 11, 18 against reference clients before
  widening).
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/`

### Task 5: Per-node in-flight / queued / failed counters
- **Status**: pending
- **Goal**: The pool checkout path increments and decrements per-
  node counters so the (Task 6) circuit breaker can read them
  without blocking on a GenServer call. Counters are `:counters`
  references, one per node, created by the Tender when a node is
  registered and destroyed when the node is dropped.
- **Prerequisites**: Task 1 (node lifecycle — the counters are
  attached to the node record and cleaned up at inactive / drop
  transitions).
- **Scope**: `lib/aerospike/tender.ex` (allocate counters at
  `register_new_node/5`, expose via `pool_pid/2` return or a
  parallel `node_handle/2` call), `lib/aerospike/node_pool.ex`
  (increment/decrement around `checkout!/3`), new module
  `lib/aerospike/node_counters.ex` (or similar — names are
  negotiable), tests under `test/aerospike/`.
- **Non-goals**: No circuit-breaker behaviour (Task 6). No
  telemetry emissions. No backpressure or rejection logic based on
  counter values. No per-pool counters — counters are per-node
  even when the pool has multiple workers.
- **Guidance**: The `:counters` module exposes
  `:counters.new(3, [:atomics])` for three slots: `:in_flight`,
  `:queued`, `:failed` (slot assignment documented in the new
  module). `NodePool.checkout!/3` needs the counters reference; two
  plumbing options:
    1. Stash the counters reference in `pool_state` at
       `init_pool/1`. Simplest — same shape as existing
       `node_name` / `transport` keys.
    2. Return `{pool_pid, counters_ref}` from
       `Tender.pool_pid/2` so the command path wraps the checkout
       itself.
  Option 1 keeps `Aerospike.Get` unchanged in this task and
  localises the plumbing to the pool module. Prefer option 1 and
  defer option 2 to Task 6 if the breaker needs the reference
  outside the pool (it does — see Task 6 Guidance).
  Define who writes what:
    - `:in_flight` — incremented before the user callback runs in
      `handle_checkout/4`, decremented in `handle_checkin/4`
      (both `:ok` and `:remove` branches) and in
      `terminate_worker/3`.
    - `:queued` — NimblePool's queue length. `:counters` is a
      derived write here; simpler to compute on demand via
      `:sys.get_state(pool).queue` when the circuit breaker asks,
      and leave the counter slot reserved but unused in Tier 2.
      Revisit in Task 6 if the breaker needs a truly lock-free
      read — `:sys.get_state/1` is a GenServer call. Document the
      decision in `log.md`.
    - `:failed` — incremented on a command result that `Error.rebalance?/1`
      returns `false` *and* whose code is in a short transport-failure
      allow-list (`:network_error`, `:timeout`, `:connection_closed`).
      Rebalance-class failures do not bump `:failed`; they are a
      routing signal, not a node-health signal.
  Tests must cover: counters are incremented and decremented
  around a scripted Fake checkout; transport-failure errors bump
  `:failed`; rebalance errors do not; counter refs are released
  when a node transitions to `:inactive` (no stale refs leaked).
- **Escalate if**: `:counters` allocation turns out to need a
  supervisor-level owner (it does not today — the Tender holds the
  reference), or if the hot-path cost of `:counters.add/3` shows
  up in the Tier 1 concurrency smoke test.
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/node_pool_test.exs test/aerospike/tender_test.exs`
  - Tier 1.5 concurrency smoke regression:
    `mix test test/integration/get_pool_test.exs --include integration`

### Task 6: Circuit breaker reading counters
- **Status**: pending
- **Goal**: A pure function `Aerospike.CircuitBreaker.allow?/2`
  reads a node's counters and status and returns `:ok` or
  `{:error, %Error{code: :circuit_open, ...}}`. `Aerospike.Get`
  consults it before checkout and short-circuits instead of
  waiting on a dead node. The breaker's thresholds are
  configurable at `Aerospike.start_link/1` time and flow through
  the Tender.
- **Prerequisites**: Tasks 1 and 5 (lifecycle + counters are the
  inputs this reads).
- **Scope**: new `lib/aerospike/circuit_breaker.ex`,
  `lib/aerospike/get.ex` (pre-checkout guard),
  `lib/aerospike/tender.ex` (expose counters reference via
  `pool_pid/2` return or a new `node_handle/2`),
  `lib/aerospike.ex` (thread breaker opts through `start_link/1`),
  tests in `test/aerospike/circuit_breaker_test.exs`.
- **Non-goals**: No half-open state machine with a timer —
  breaker reads are stateless and recompute on every check. No
  automatic node `:inactive` promotion driven by the breaker —
  that is the Tender's job via the existing failure threshold (the
  breaker rejects; the Tender demotes). No per-command breaker
  overrides; the policy lives at cluster start.
- **Guidance**: The breaker is a pure module; there is no
  GenServer and no timer. `allow?(counters_ref, opts)` reads the
  three counter slots and returns `:ok` if
  `:in_flight + :queued < max_concurrent_ops_per_node` and
  `:failed < circuit_open_threshold`, else
  `{:error, %Error{code: :circuit_open, ...}}`. Failure counter
  decay is handled by the Tender: on a successful tend cycle for
  the node, zero the `:failed` slot (writer = Tender, matching the
  single-writer principle for that slot). The `:in_flight` slot is
  owned by the pool path (Task 5). `:queued` stays reserved.
  `Aerospike.Get.execute/4` calls `CircuitBreaker.allow?/2` after
  `Router.pick_for_read/4` and before `Tender.pool_pid/2`. A short-
  circuit counts as a transport-like failure for the retry layer
  (Task 7) so retry can re-route without waiting.
  Start-opts:
    - `:circuit_open_threshold` (default: 10 failures)
    - `:max_concurrent_ops_per_node` (default: matches
      `:pool_size` × some factor — revisit against reference
      clients; place a sane default and note it in `log.md`)
  These flow: `Aerospike.start_link/1` → `Tender.init/1` → pool
  state (so `NodePool.checkout!/3` can bail out before adding to
  `:in_flight`, matching the rule "the breaker refuses before the
  checkout").
  Tests must cover: breaker allows below both thresholds, refuses
  above either, Tender zeroes `:failed` on successful tend, pool
  is not touched when the breaker refuses (no wasted checkout).
- **Escalate if**: the breaker thresholds collide with retry
  semantics in a way the plan did not anticipate (e.g. the
  `:max_concurrent_ops_per_node` check makes the retry path
  starve legitimate requests). Revisit with the user before
  relaxing thresholds.
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/circuit_breaker_test.exs test/aerospike/tender_test.exs test/aerospike/node_pool_test.exs`

### Task 7: Retry policy
- **Status**: pending
- **Goal**: `Aerospike.Get` (and, by extension, the caller
  `Aerospike.get/3`) retries within a caller-supplied total-op
  budget, picking the next replica on rebalance-class errors and
  the same replica on transport-class errors up to a configurable
  attempt cap. Circuit-breaker refusal counts as transport-class
  for the purposes of re-routing (it means "this node is unusable
  right now").
- **Prerequisites**: Tasks 4 (rebalance classification) and 6
  (circuit breaker). Task 3 (generation short-circuit) is not
  strictly required but makes the rebalance→tend→replay loop
  deterministic.
- **Scope**: `lib/aerospike/get.ex` (wrap the existing pick→checkout
  →send loop in a retry driver), `lib/aerospike.ex` (expose retry
  opts at the public API), possibly a new
  `lib/aerospike/retry_policy.ex` if the logic grows past ~60
  lines in `Get`, tests in `test/aerospike/` and
  `test/integration/`.
- **Non-goals**: No retry for non-idempotent commands (GET is
  idempotent; write-path retry is Tier 4). No backoff strategy
  richer than a fixed `:sleep_between_retries_ms`. No
  `:max_retries_per_replica` distinct from the global cap. No
  jitter. No circuit-breaker half-open timers.
- **Guidance**: Retry control flow:
    1. Compute a monotonic op-budget deadline:
       `deadline = monotonic_now() + :timeout`.
    2. `attempt = 0`.
    3. Loop:
       a. `Router.pick_for_read(tables, key, policy, attempt)`.
       b. `CircuitBreaker.allow?(counters_ref, opts)`. On refusal,
          `attempt += 1`, goto (a) unless budget exhausted or
          `attempt >= max_retries`.
       c. `NodePool.checkout!` → `transport.command/3` with a
          per-attempt deadline = `min(remaining_budget,
          per_attempt_cap)`.
       d. On `{:ok, body}` — parse, return.
       e. On rebalance error — trigger one `Tender.tend_now/1`
          (async; do not block), `attempt += 1`, goto (a) unless
          exhausted.
       f. On transport error — `attempt += 1`, goto (a) unless
          exhausted. Do NOT re-pick if `attempt < replica_count`
          and `policy == :sequence`; the replica walker already
          rotates. For `policy == :master`, all retries target the
          master.
       g. On any other error — return verbatim (no retry).
    4. On budget exhaustion, return the last error with
       `%Error{in_doubt: true}` if the last attempt was sent but
       unacknowledged (socket closed mid-reply) — the transport
       layer already tags `in_doubt` for idempotent-unsafe cases;
       GET is read-only, so in-doubt is strictly informational.
  Start-opts plumbed through `Aerospike.start_link/1` → `Tender`
  → read at command time:
    - `:max_retries` (default: 2)
    - `:sleep_between_retries_ms` (default: 0 — defer jittered
      backoff to Tier 4)
    - `:replica_policy` (default: `:sequence`; the existing
      Tier 1.5 default was `:master`, which this task flips — see
      Escalate)
  Tests must cover: transport error on attempt 0 re-routes to next
  replica on attempt 1 (assert via Fake scripted replies); one
  rebalance error → `tend_now` called → attempt 1 sees the new
  map; budget exhaustion returns the most recent error; circuit-
  open refusal counts as a retryable transport-class event;
  `:max_retries: 0` disables retry entirely and matches Tier 1.5
  behaviour.
  Integration coverage: kill the single-node docker container
  mid-traffic; assert in-flight requests see a rebalance-class or
  transport error that retry converts into a deterministic
  `:cluster_not_ready` or surface-error once the budget runs out.
- **Escalate if**: flipping the default `:replica_policy` from
  `:master` to `:sequence` breaks a Tier 1 invariant test. The
  plan calls the flip explicitly because replica walking is the
  substrate retry reads; if a Tier 1 test encodes the old default,
  either change the test (document the invariant adjustment in
  `log.md`) or take the decision back to the user.
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/`
  - `mix test --include integration` (local docker single-node)

### Task 8: Integration proof — node killed mid-traffic
- **Status**: pending
- **Goal**: Reproduce the roadmap's exit-criteria scenario end-to-
  end against the local single-node server: kill the container
  mid-traffic, observe the node transitions to `:inactive`, its
  pool stops, in-flight GETs surface a rebalance-class or
  transport error that the retry layer absorbs, and the partition
  map converges within one tend cycle when the node comes back.
- **Prerequisites**: Tasks 1 through 7 complete.
- **Scope**: `test/integration/` (new file or extension of
  `get_pool_test.exs`). No production-code changes.
- **Non-goals**: No multi-node scenarios (single node is
  sufficient to cover lifecycle + retry — rebalance routing is
  unit-tested via Fake in Task 7). No performance measurements.
  No telemetry assertions.
- **Guidance**: Use `System.cmd("docker", ["stop",
  "aerospike_spike"])` / `start` rather than inventing a new
  harness (`spike-docs/integration-testing-harness.md` recommends
  Option C). Poll `Tender`'s node map until the node flips to
  `:inactive`; poll again after `docker start` until it returns to
  `:active` and the partition map is re-applied (use
  `Tender.ready?/1`). Wrap the whole test in a `setup_all` that
  re-asserts container health, and tag it `@moduletag :integration`
  so it only runs under `--include integration`.
  Budget the test at ~10 s wall-clock: container stop is fast,
  tend interval default is 1 s, partition-generation fetch on
  restart should re-populate within 1-2 cycles.
- **Escalate if**: the single-node scenario cannot cover the
  rebalance path (e.g. killing the only node yields `:cluster_not_
  ready` rather than any observable rebalance signal). If so,
  unit coverage via Fake (Tasks 4 and 7) stands as the primary
  proof; document the gap and note that the full scenario needs
  the multi-node harness deferred to a later plan.
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test --include integration`
  - Run the new scenario test 5 times with different `--seed`
    values to confirm it is not flaky; log results in `log.md`.
