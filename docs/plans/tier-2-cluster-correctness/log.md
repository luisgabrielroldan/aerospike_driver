# Tier 2 — Cluster Correctness — Log

Execution log for `plan.md`. Append entries as tasks move between
`pending`, `in_progress`, and `done`. Keep entries short and factual;
longer context belongs in `notes.md`.

## Entry template

```
## YYYY-MM-DD — Task N
- Status: pending → in_progress
- Notes: <what changed, files touched, anything the next executor needs>
```

## Log

## 2026-04-20 — Task 1

- Status: pending → in_progress → done
- Files: `lib/aerospike/tender.ex`, `test/aerospike/tender_test.exs`
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `tender_test.exs` / `tender_pool_test.exs` / `node_pool_test.exs` /
  `get_pool_test.exs` ✓, full `mix test` ✓ (91 tests, verified across
  10 seeds after fixing an unrelated `on_exit` race — see notes), mix
  dialyzer ✓.
- Notes: The plan's "second consecutive threshold breach" trigger for
  the `:inactive` → removed transition collapses to "any failure while
  `:inactive`" because `refresh_nodes/1` is the only stage that probes
  inactive nodes (the other stages now filter them). Recorded in
  `notes.md` so Task 7's retry semantics do not assume a multi-cycle
  grace window.
- Commit: none — `aerospike_driver_spike` repo is on `main`, and the
  commit-changes skill refuses to commit there without an explicit
  dedicated plan branch.

## 2026-04-20 — Task 2

- Status: pending → in_progress → done
- Files: `lib/aerospike/tender.ex`,
  `test/aerospike/tender_test.exs`,
  `test/aerospike/tender_pool_test.exs`,
  `test/aerospike/supervisor_test.exs`
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `tender_test.exs` ✓ (17 tests, 0 failures across seeds 1/7/42/137/1042),
  `partition_map_test.exs` ✓ (17 tests), full `mix test` ✓ (96 tests),
  `mix test --include integration` ✓ against the live single-node server
  (confirms real `cluster-stable:` returns a non-empty hex hash so the
  refresh path stays healthy in production), `mix dialyzer` ✓.
- Notes: Combined the per-node refresh into a single `info(conn,
  ["partition-generation", "cluster-stable"])` call rather than two
  sequential calls. Failure-counter accounting is unchanged — one
  failed combined call still bumps `failures` by one — but every test
  that scripts the old `["partition-generation"]` key list had to be
  updated to the combined key list. Cluster-stable hash lives on the
  in-memory node record (per Open Question default in `notes.md`); it
  is cleared at the start of every `refresh_nodes/1` so a stale hash
  from a previous cycle cannot contribute to the current cycle's
  agreement check. Empty-set (`{:ok, :empty}`) is treated as agreement
  (no contributors) so bootstrap failure does not log spurious
  warnings.
- Commit: none — `aerospike_driver_spike` repo is on `main`, and the
  commit-changes skill refuses to commit there without an explicit
  dedicated plan branch.

## 2026-04-20 — Task 3

- Status: pending → in_progress → done
- Files: `lib/aerospike/tender.ex`,
  `test/aerospike/tender_test.exs`,
  `test/aerospike/tender_pool_test.exs`
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `tender_test.exs` + `partition_map_test.exs` ✓ (39 tests across seeds
  0/137/1042/4221/9999), full `mix test` ✓ (101 tests, 5 integration
  excluded), `mix test --include integration` ✓ (101 tests, 0 failures
  against the live single-node server), `mix dialyzer` ✓.
- Notes: Chose in-memory `applied_gen` on the Tender's node record over
  the plan's default option 1 (extend `node_gens` entry shape).
  Rationale: option 1 would change `PartitionMap.get_node_gen/2`'s
  `{:ok, integer()}` return shape that multiple tests and the
  `node_gens` ETS contract already depend on; option 2 (a second ETS
  table) adds surface for a value only the Tender reads; in-memory
  storage matches how Task 2 handled `cluster_stable` and keeps the
  single-writer invariant trivial. Divergence recorded in `notes.md`.
  Short-circuit condition: fetch unless `generation_seen` is nil,
  `applied_gen` is nil, or they differ. `applied_gen` is nil-reset in
  `mark_inactive/2` so recovery always refetches; `apply_replicas/3`
  advances it atomically with `PartitionMap.update/5` writes and rolls
  back to the prior value when any segment is rejected as stale by
  the regime guard. Two existing tests needed updating because each
  failing tend cycle now contributes 2 failures instead of 3 (the
  replicas call is skipped when the generation does not advance);
  thresholds in both tests were lowered and comments clarified.
- Commit: none — `aerospike_driver_spike` repo is on `main`, and the
  commit-changes skill refuses to commit there without an explicit
  dedicated plan branch.

## 2026-04-20 — Task 4

- Status: pending → in_progress → done
- Files: `lib/aerospike/error.ex`,
  `test/aerospike/error_test.exs` (new),
  `test/aerospike/protocol/response_test.exs` (new)
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `mix test test/aerospike/error_test.exs test/aerospike/protocol/response_test.exs`
  ✓ (5 doctests + 13 tests), `mix test test/aerospike/` ✓ (5 doctests,
  109 tests, 0 failures — Task 4's specified check), full `mix test`
  ✓ (5 doctests, 114 tests, 0 failures, 5 integration excluded),
  `mix dialyzer` ✓ (0 errors).
- Notes: Implemented the retry cue as `Aerospike.Error.rebalance?/1`
  rather than a new struct field, matching the plan's "prefer a helper
  over a struct field" guidance. Only `:partition_unavailable` (wire
  code 11) qualifies today; audited Go (`multi_command.go` branches on
  `PARTITION_UNAVAILABLE` separately from other server errors) and
  Java (`PartitionTracker.partitionUnavailable/2` flips the partition's
  retry flag on the same code) — neither widens the class to other
  codes for record-level reads. `rebalance?/1` accepts any term so
  callers can match uniformly on `{:error, atom() | Error.t()}`
  returns — bare atoms like `:cluster_not_ready`, `:no_master`, and
  `:unknown_node` (Get's non-Error return shape) all classify as
  non-rebalance without needing a wrapper. `Aerospike.Get` was NOT
  modified: the existing path already surfaces
  `%Error{code: :partition_unavailable}` via
  `Response.parse_record_response/2`, which is the exact bits
  `rebalance?/1` reads. Task 7 (retry) will be the first caller.
  Added a new `test/aerospike/protocol/` subdirectory — first tests
  under the protocol namespace, follows the `lib/aerospike/protocol/`
  layout.
- Commit: none — `aerospike_driver_spike` repo is on `main`, and the
  commit-changes skill refuses to commit there without an explicit
  dedicated plan branch.

## 2026-04-20 — Task 5

- Status: pending → in_progress → done
- Files: `lib/aerospike/node_counters.ex` (new),
  `lib/aerospike/node_pool.ex`,
  `lib/aerospike/node_supervisor.ex`,
  `lib/aerospike/tender.ex`,
  `lib/aerospike/get.ex`,
  `test/aerospike/node_counters_test.exs` (new),
  `test/aerospike/node_pool_test.exs`,
  `test/aerospike/tender_test.exs`
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `mix test test/aerospike/node_pool_test.exs` ✓ (17 tests),
  `mix test test/aerospike/tender_test.exs` ✓, full `mix test` ✓
  (5 doctests, 133 tests, 0 failures, 5 integration excluded),
  `mix test --include integration` ✓ (5 doctests, 133 tests, 0 failures),
  `mix dialyzer` ✓ (0 errors).
- Notes: Chose plumbing option 1 (stash counters in `pool_state` at
  `init_pool/1`) as the plan recommended. The pool's checkin protocol
  grew a new `{:close, :failure}` tuple — the pool-internal way to say
  "drop the worker *and* bump `:failed`". Existing `:close` and normal
  `conn` checkins are unchanged, so no call sites outside `Get` needed
  edits. `Get.do_get/4` now returns `{:close, :failure}` on both
  transport errors and parse errors (a body we cannot decode is
  transport-adjacent for a given socket). The plan's `:failed` allow-
  list named `:connection_closed`, but the codebase emits
  `:connection_error` (not `:connection_closed`) from its transport
  layers — `@failure_codes` uses `[:network_error, :timeout,
  :connection_error]` to match the codes that actually flow. Recorded
  in `notes.md` so Task 6 reads the same list.
  Counter allocation is gated on `:node_supervisor` presence: when the
  Tender runs in cluster-state-only mode (no pool) it never allocates
  a counter ref, and `node_counters/2` returns `{:error, :unknown_node}`
  so callers cannot pretend counters exist when they do not. On
  `mark_inactive/2` the node record's `counters` field is nilled; the
  `:counters` term itself is GC'd naturally once no process holds it
  (the module does not support explicit free). `:queued` stays
  reserved — Tier 2 writes nothing to it; Task 6 will revisit if the
  breaker needs a lock-free read.
  Pool-level errors that occur before `fun` runs (`:pool_timeout`,
  `:invalid_node`) are deliberately not counted as `:failed`: `fun`
  never touched a live socket, so there is no node-health signal to
  record. `handle_cancelled(:checked_out, ...)` decrements `:in_flight`
  when a caller crashes while holding a worker; the `:queued` branch
  is a no-op.
  Test hygiene: NimblePool's `checkout!/4` sends the `:checkin` message
  via `send/2` and returns immediately, so tests that read counters
  right after `checkout!` can race the pool's `handle_checkin/4`. Fixed
  by inserting `:sys.get_state(pool)` as a deterministic drain between
  the checkout return and the counter assertions. The counters-test
  file also asserts tuple shape (`{:atomics, ref}`) rather than
  `is_reference/1` because `:counters.new/2` hands back the atomics
  wrapper, not a bare ref.
  Handoff for Task 6: the breaker needs the counter ref outside the
  pool. `Tender.node_counters/2` already exposes it; thread it through
  `Get.execute/4` between `Router.pick_for_read/4` and
  `Tender.pool_pid/2` (or fold `pool_pid/2` and `node_counters/2` into
  a single `node_handle/2` as the plan's Task 5 Guidance anticipated).
- Commit: none — `aerospike_driver_spike` repo is on `main`, and the
  commit-changes skill refuses to commit there without an explicit
  dedicated plan branch.

## 2026-04-20 — Task 6

- Status: pending → in_progress → done
- Files: `lib/aerospike/circuit_breaker.ex` (new),
  `lib/aerospike/get.ex`,
  `lib/aerospike/tender.ex`,
  `lib/aerospike/protocol/result_code.ex`,
  `test/aerospike/circuit_breaker_test.exs` (new),
  `test/aerospike/get_test.exs` (new),
  `test/aerospike/tender_test.exs`
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `mix test test/aerospike/circuit_breaker_test.exs
  test/aerospike/tender_test.exs test/aerospike/node_pool_test.exs` ✓,
  full `mix test` ✓ (5 doctests, 150 tests, 0 failures, 5 excluded),
  `mix dialyzer` ✓ (0 errors).
- Notes: Breaker is a pure module with a single `allow?/2` clause that
  re-reads the three counter slots on every call — no GenServer, no
  ETS, no timer, matching the plan's "stateless by design" guidance.
  `cond` reports the failure-cap breach first; the combined-breach test
  pins that order so downstream logs stay stable. Added `:circuit_open`
  at `ResultCode` -28 with message "Node circuit breaker is open" to
  round out the client-side negative-code table — `Error.rebalance?/1`
  stays false for it (a breaker trip is transport-class for retry).
  Chose `node_handle/2` over extending `pool_pid/2` because the command
  path needs pool + counters + breaker opts together; one GenServer
  hop replaces two (`pool_pid/2` + `node_counters/2`) and the existing
  accessors stay untouched. `handle_call({:node_handle, ...}, ...)`
  requires both a pid pool and a non-nil counters ref — cluster-state-
  only Tenders and nodes missing either slot correctly return
  `{:error, :unknown_node}`. `Aerospike.Get.execute/4` calls the
  breaker between `Tender.node_handle/2` and the pool checkout, so a
  refusal never touches the pool and the retry layer (Task 7) sees
  `{:error, %Error{code: :circuit_open}}` without paying for a
  checkout+send.
  `:failed` decay hook lives inside `register_success/4`: that is the
  single "successful tend cycle" spot (the node answered the combined
  `partition-generation` + `cluster-stable` info call cleanly), so
  decay is lock-step with the cluster-state view of node health.
  `reset_failed_counter(nil)` guards the cluster-state-only path where
  a node record has no counters ref.
  Default thresholds: `:circuit_open_threshold = 10`,
  `:max_concurrent_ops_per_node = pool_size × 10`. The concurrency
  multiplier is intentionally generous — it is a backstop for a wedged
  pool rather than a steady-state cap (NimblePool already queues). The
  failure ceiling matches the node's existing `:failure_threshold`
  default so a cluster-side demotion trips at roughly the same budget
  as a breaker trip.
  Handoff for Task 7: `Get.execute/4` returns
  `{:error, %Error{code: :circuit_open}}` verbatim; the retry driver
  should treat it as transport-class and re-pick a replica (same rule
  it uses for `:network_error` / `:timeout` / `:connection_error`), no
  `tend_now` trigger needed because the breaker already encodes "node
  is unusable right now".
- Commit: none — `aerospike_driver_spike` repo is on `main`, and the
  commit-changes skill refuses to commit there without an explicit
  dedicated plan branch.

## 2026-04-20 — Task 7

- Status: pending → in_progress → done
- Files: `lib/aerospike/retry_policy.ex` (new),
  `lib/aerospike/get.ex` (retry loop rewrite),
  `lib/aerospike/tender.ex` (thread retry opts through init →
  `RetryPolicy.put/2` on the `:meta` ETS table),
  `lib/aerospike.ex` (document new `get/3` retry opts),
  `test/aerospike/retry_policy_test.exs` (new),
  `test/aerospike/get_retry_test.exs` (new).
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `mix test test/aerospike/` ✓ (full unit suite, 5 doctests + 173
  tests, 0 failures — the retry unit surface adds 19 new tests across
  `retry_policy_test.exs` and `get_retry_test.exs`),
  `mix test --include integration` ✓ (5 doctests + 173 tests, 0
  failures against the local single-node docker server),
  `mix dialyzer` ✓ (0 errors).
- Notes: Retry policy is stored in the `:meta` ETS table under
  `:retry_opts` with the Tender as sole writer (matching the single-
  writer discipline for every other `:meta` slot). Command path loads
  it lock-free via `RetryPolicy.load/1` in `Get.execute/4`, then
  overlays per-call opts with `RetryPolicy.merge/2`. `load/1` falls
  back to `RetryPolicy.defaults/0` when the slot is absent so the
  cluster-state-only test harnesses (which skip Tier 2's retry init)
  keep working without a shim.
  Three disjoint error classes drive the loop: rebalance (delegates to
  `Error.rebalance?/1`), transport (the existing Tier 1.5 failure-code
  set plus `:pool_timeout`, `:invalid_node`, and `:circuit_open`), and
  fatal (server-logical codes and routing atoms, return verbatim).
  `RetryPolicy.rebalance?/1` accepts both the bare `%Error{}` and the
  `{:error, _}` tuple form for symmetry with `transport?/1`, which the
  Get command path relies on to thread the raw `result` through
  `classify/3` without manual unwrapping.
  Routing atoms (`:cluster_not_ready`, `:no_master`) are fatal with no
  retry: the Router's verdict is that no replica exists to target, so
  re-picking would burn attempts against the same empty state.
  `{:error, :unknown_node}` from `Tender.node_handle/2`, however, is
  transport-class — a node that disappeared between `pick_for_read/4`
  and the handle lookup is a routing stale signal, and the next
  attempt's re-pick is expected to land elsewhere.
  Rebalance handling fires an unlinked `spawn/1` that calls
  `Tender.tend_now/1` inside a `try`/`catch :exit` guard so a transient
  Tender failure cannot tear down the caller. The pid is discarded; if
  the tend fails the next retry either hits the same rebalance (and
  the budget burns out) or sees the updated map — both are acceptable
  terminal states for the loop.
  Default `:replica_policy` flipped from Tier 1.5's `:master` to
  `:sequence` as the plan called for. No Tier 1 invariant test
  encoded the old default, so no escalation was needed. The Router's
  `pick_for_read/4` already accepts the policy per call; `Get.execute/4`
  simply stopped hard-coding `:master` in favour of reading the
  policy from ETS/merged opts.
  Credo originally flagged three arity-9 helpers in the retry loop.
  Refactored the loop to bundle `tender`, `tables`, `transport`,
  `policy`, `key`, and `deadline` into a `ctx` map threaded through;
  every helper is now 3 params or fewer. No behavioural change.
  Pre-existing async flake:
  `test cluster-stable agreement guard disagreement skips refresh and
  leaves prior owners untouched (Aerospike.TenderTest)` exited
  `{:exit, :shutdown}` once during a full `mix test` run but passed on
  every targeted re-run and on the subsequent full runs used for the
  integration-tagged invocation. Unrelated to Task 7's code; flagged
  for a future cleanup pass but does not block this task.
- Commit: none — `aerospike_driver_spike` repo is on `main`, and the
  commit-changes skill refuses to commit there without an explicit
  dedicated plan branch.

## 2026-04-20 — Task 8

- Status: pending → in_progress → done
- Files: `test/integration/node_kill_test.exs` (new)
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `mix test --include integration` ✓ (5 doctests + 174 tests, 0 failures
  — the new file adds 1 test; full integration run completes in ~5 s),
  5-seed sweep on the new test (seeds 0 / 1 / 42 / 137 / 9999) ✓ — each
  run ~4 s, all passing, `mix dialyzer` ✓ (0 errors).
- Notes: Single-node kill scenario exercises `:active` → `:inactive` →
  dropped across tend cycles (failure_threshold=3, tend_interval=400 ms)
  and asserts that outage GETs surface transport-class errors or the
  router's `:cluster_not_ready` / `:unknown_node` atoms through the
  Tier 2 retry path. `Tender.node_handle/2` and `Tender.pool_pid/2`
  both refuse the node once :inactive, confirming pool teardown. The
  plan's "partition map converges within one tend cycle when the node
  comes back" clause was intentionally not exercised — the Tender's
  `bootstrap_if_needed/1` is single-shot (notes.md Task 1 finding,
  Tier 1 scope), so a kill+restart against a single-node cluster
  cannot recover without Tier 3 work (TCP transport re-dial on the
  info socket, or a re-bootstrap hook driven by the supervisor).
  Task 8's own "Escalate if" clause explicitly covers this gap and
  defers full kill+restart round-trip proof to the multi-node harness
  plan. Findings added to `notes.md` under a new Task 8 section so the
  next tier's planner sees the recovery primitive as its first piece
  of work.
  Sibling integration tests (`get_test.exs`, `get_pool_test.exs`)
  initially flaked after the new test's `on_exit` because the docker
  container was "TCP up" before its partition map had finalised
  (`partition-generation` returns `-1` and the `replicas` bitmap is
  all-zero for ~1.7 s after `docker start`). The test's teardown now
  waits for `partition-generation >= 0` in addition to `asinfo -v status
  ok`, which closes the race across repeated runs and five seeds.
- Commit: none — `aerospike_driver_spike` repo is on `main`, and the
  commit-changes skill refuses to commit there without an explicit
  dedicated plan branch.
