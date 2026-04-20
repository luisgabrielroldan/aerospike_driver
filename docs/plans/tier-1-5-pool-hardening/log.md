# Tier 1.5 — Pool Hardening — Log

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

## Task 1 — Confirm partial-reply buffering semantics

**Date:** 2026-04-20 — **Status:** done

- Files: `docs/plans/tier-1-5-pool-hardening/notes.md`,
  `docs/plans/tier-1-5-pool-hardening/plan.md`
- Checks: investigation-only task, no code checks required
- Notes: Finding 7 added to `notes.md`. Passive `{:packet, :raw}` +
  `:gen_tcp.recv(socket, N, _)` already guarantees exact-N reads, so
  server fragmentation is transparent. Go client's `Read` loop is a
  consequence of Go's `net.Conn.Read` semantics, not a buffering win
  the spike is missing. Task 5 rewritten as documentation + regression
  test only; no new buffering code.

## Task 2 — Harden pool warm-up and document it

**Date:** 2026-04-20 — **Status:** done

- Files: `lib/aerospike/node_pool.ex`,
  `lib/aerospike/node_supervisor.ex`,
  `lib/aerospike/transport/fake.ex`,
  `test/aerospike/node_pool_test.exs`
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `mix test` (78 tests, 0 failures, integration excluded) ✓,
  dialyzer ✓
- Notes: `lazy: false` pinned in `NodeSupervisor.start_pool/2`;
  `init_worker/1` now logs each successful connect at `:debug` and
  failures at `:warning`. `Transport.Fake` grew `script_connect/3` and
  `connect_count/2` so warm-up tests can script per-attempt failures
  and assert eager init ran exactly `pool_size` times. No commit
  created — spike repo is on `main`.

## Task 3 — Idle-deadline eviction via handle_ping/2

**Date:** 2026-04-20 — **Status:** done

- Files: `lib/aerospike/node_pool.ex`,
  `lib/aerospike/node_supervisor.ex`,
  `lib/aerospike/transport/fake.ex`,
  `test/aerospike/node_pool_test.exs`
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `mix test` (81 tests, 0 failures, integration excluded) ✓,
  node_pool_test 5× with different seeds ✓, dialyzer ✓
- Notes: `NodePool.handle_ping/2` returns `{:remove, :idle}`; defaults
  `idle_timeout_ms: 55_000` and `max_idle_pings: 2` wired through
  `NodeSupervisor.start_pool/2` as NimblePool's `:worker_idle_timeout`
  and `:max_idle_pings`. 55_000 ms sits below Aerospike's default
  `proto-fd-idle-ms` (60_000) so the client closes first. Fake gained
  `close_count/2` so tests can distinguish ping-close from shutdown-
  close. No commit — spike repo is on `main`.

## Task 4 — Split read-deadline from total-op deadline

**Date:** 2026-04-20 — **Status:** done

- Files: `lib/aerospike/node_transport.ex`,
  `lib/aerospike/transport/tcp.ex`,
  `lib/aerospike/transport/fake.ex`,
  `lib/aerospike/get.ex`,
  `test/aerospike/transport/fake_test.exs`,
  `test/aerospike/transport/tcp_test.exs` (new),
  `test/aerospike/node_pool_test.exs`
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `mix test` (84 tests, 0 failures, integration excluded) ✓,
  `mix test test/integration/ --include integration` (3 tests, 0
  failures) against `docker compose up -d` ✓, dialyzer ✓
- Notes: `@callback command/2` → `command/3` with `deadline_ms`;
  `%Transport.Tcp{}` dropped `:recv_timeout` and renamed the remaining
  init-time knob to `:info_timeout` so it is unambiguous that the field
  is only used by `info/2` (info has no per-call deadline). `Get`
  derives the deadline from the caller's `:timeout` verbatim — see
  notes.md "Task 4 deadline policy". Fake gained
  `last_command_deadline/2` and retires the old `command/2` signature;
  `consume_script` unchanged (script key is still `{:command,
  node_id}`). No commit — spike repo is on `main`.

## Task 5 — Document passive-mode framing and regression-test fragmentation

**Date:** 2026-04-20 — **Status:** done

- Files: `lib/aerospike/transport/tcp.ex`,
  `test/aerospike/transport/tcp_test.exs`
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `mix test` (85 tests, 0 failures, integration excluded) ✓,
  `mix test test/integration/get_pool_test.exs --include integration`
  against `docker compose up -d` (2 tests, 0 failures) ✓,
  dialyzer ✓
- Notes: `Transport.Tcp` `@moduledoc` now cites passive `{:packet, :raw}`
  + `:gen_tcp.recv/3` exact-N semantics and defers coalescing/buffering
  with a pointer to Finding 7. A private comment above `recv_message/2`
  explains the two-recv framing for future readers. Fragmentation
  regression test splits the reply four bytes in (mid-header) so both
  `recv_exact` calls must survive fragmentation — covers the guarantee
  end-to-end through a loopback `:gen_tcp` listener. No commit — spike
  repo is on `main`.

## Task 6 — Concurrency smoke test and regression check

**Date:** 2026-04-20 — **Status:** done

- Files: `test/integration/get_pool_test.exs`
- Checks: format ✓, compile --warnings-as-errors ✓, credo --strict ✓,
  `mix test` (87 tests, 0 failures, integration excluded) ✓,
  `mix test --include integration` (87 tests, 0 failures) ✓,
  `mix test test/integration/get_pool_test.exs --include integration`
  5× across seeds (0, 1, 42, 12345, 99999), 4 tests / 0 failures
  each ✓, dialyzer ✓
- Notes: Added two integration tests. (a) `pool_size: 10` smoke at
  100 concurrent GETs asserts eager warm-up via
  `:queue.len(:sys.get_state(pool).resources) == 10` before any GET
  runs, then drives 100 concurrent `Aerospike.get/3` calls through
  `Task.await_many/2` and expects every reply to be `:key_not_found`.
  (b) Idle-eviction test starts `NodeSupervisor` directly and calls
  `NodeSupervisor.start_pool/2` with `idle_timeout_ms: 100,
  max_idle_pings: 2, pool_size: 2` against the real docker server,
  captures the checked-out socket via `conn.socket`, waits past the
  deadline polling `:inet.port/1` until the socket closes, then
  asserts the next checkout returns a different socket and the pool
  refills to `pool_size`. Bypass of `Aerospike.start_link` is because
  the Tender does not forward `:idle_timeout_ms` to `NodeSupervisor`
  — Tier 2's per-node state machine owns that plumbing. No commit —
  spike repo is on `main`.
