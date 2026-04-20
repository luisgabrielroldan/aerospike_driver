# Tier 1.5 — Pool Hardening

- **Status**: done

## Objective

Finish the per-node `NimblePool` with the capabilities Tier 1 deliberately
skipped, without introducing new processes or new cross-component
contracts. Changes land in `Aerospike.NodePool`, `Aerospike.NodeSupervisor`,
`Aerospike.Transport.Tcp`, and — where the roadmap explicitly requires it —
the `Aerospike.NodeTransport` behaviour.

The scope is exactly the four roadmap items for Tier 1.5:

1. Warm-up at pool start (verify / harden, since NimblePool is already
   eager by default).
2. Idle deadline / eviction for workers that sit unused.
3. Separation between a socket-read deadline and the caller's total-op
   deadline in `command/2`.
4. Partial-reply buffering in `Transport.Tcp.recv` so fragmented server
   responses do not consume one `:gen_tcp.recv/2` call per frame.

Exit condition: every Tier 1 test continues to pass unchanged, the new
behaviours are covered by unit tests, and no measurable regression appears
in the Tier 1 concurrency smoke test.

## Constraints

- Keep changes inside `aerospike_driver_spike/`. No edits to the
  `aerospike_driver/` release repo as part of this plan.
- Preserve the `NodeTransport` behaviour as the only seam between
  cluster logic and I/O. If a Tier 1.5 item demands a new capability (the
  read-deadline separation does), grow the behaviour explicitly — do not
  bypass it or leak `:gen_tcp` calls through.
- Single-writer ETS discipline still holds. Tier 1.5 touches only the pool
  and the transport; the Tender remains the sole writer of cluster-state
  tables and must not be rewired.
- No new processes. No compression, auth, TLS, telemetry, retry,
  circuit-breaker, or per-node state machine work — those are Tier 2 or
  Tier 3.
- No backwards-compatibility shims between tiers. If a behaviour callback
  signature changes, update every implementer (`Transport.Tcp`,
  `Transport.Fake`, tests) in the same task.
- Follow `.claude/skills/coding-standards/SKILL.md`: `mix format`,
  `mix credo --strict`, `mix compile --warnings-as-errors`, alphabetised
  aliases, underscored integer literals, typed `Aerospike.Error` returns.

## Assumptions

- NimblePool's built-in `:worker_idle_timeout` + `handle_ping/2` callback
  is the intended mechanism for idle eviction. Verified against
  `deps/nimble_pool/lib/nimble_pool.ex`. No hand-rolled timer or ETS
  required.
- NimblePool defaults to non-lazy initialisation (`lazy: false`), so every
  worker is pre-opened inside its `Supervisor.init/1` reduce today. The
  warm-up item is therefore mostly about confirming and hardening
  behaviour when one or more initial connects fail, not adding the warm-up
  machinery from scratch.
- The "partial-reply buffering" item is ambiguous given that
  `:gen_tcp.recv(socket, N, timeout)` with `N > 0` already blocks until
  exactly `N` bytes arrive on a passive `{:packet, :raw}` socket. The
  observable win from buffering is coalescing header+body into fewer
  `:gen_tcp.recv/3` syscalls and giving the parser a single incremental
  buffer. The first task in this plan is an investigation task that
  confirms the intended semantics before we implement.
- Integration tests can run against the local single-node Aerospike server
  (`docker compose up -d`, port 3000). The cluster profile is not
  required for Tier 1.5 work.

## Tasks

### Task 1: Confirm partial-reply buffering semantics
- **Status**: done
- **Goal**: Decide what "partial-reply buffering" means for
  `Transport.Tcp.recv` — i.e. whether the roadmap item is about reducing
  `:gen_tcp.recv/3` syscalls, supporting server-side fragmentation that
  today's code already handles, or switching to active-mode framing.
  Record the decision in `notes.md` so Task 5 has unambiguous scope.
- **Prerequisites**: none
- **Scope**: `lib/aerospike/transport/tcp.ex`, `deps/nimble_pool/` (read
  only), any reference client behaviour worth citing
  (`official_libs/aerospike-client-go/`,
  `official_libs/aerospike-client-python/` — read only),
  `docs/plans/tier-1-5-pool-hardening/notes.md`.
- **Non-goals**: No code changes. No protocol-level buffering changes.
  No switch to active-mode sockets unless the investigation explicitly
  recommends it and the user approves.
- **Guidance**: Trace the current `recv_message/1` path. Confirm that
  `:gen_tcp.recv/3` on a passive `{:packet, :raw}` socket already returns
  exactly the requested byte count (or an error), so server fragmentation
  is transparent today. Then identify the concrete buffering win the
  roadmap points at — the likely candidates are (a) one `recv` for the
  full frame once we know its length, eliminating the separate
  header/body recv pair, or (b) a buffered-read helper that reads once
  into a local binary and slices header and body out, so the body
  `recv` cost drops to a buffer splice. Pick one based on evidence from
  the reference clients and write a one-paragraph decision into
  `notes.md` under `Findings That Shape The Plan`. If the win is
  negligible, record that and shrink Task 5 to "no-op; document why".
- **Escalate if**: Investigation concludes the intended change requires
  switching to active-mode sockets or reshaping the `command/2`
  contract beyond Task 4's scope — either would invalidate later tasks
  and should be taken back to the user.
- **Checks**:
  - `notes.md` has a single paragraph locking the buffering approach.
  - No source files under `lib/` are modified in this task.

### Task 2: Harden pool warm-up and document it
- **Status**: done
- **Goal**: Guarantee that a pool starting with a reachable node opens
  `pool_size` workers before the first checkout and that a partial
  failure (some workers connect, some don't) does not leave the pool in
  a silently degraded state.
- **Prerequisites**: Task 1 (so `notes.md` reflects the current
  transport shape before we touch init paths).
- **Scope**: `lib/aerospike/node_pool.ex` (`init_pool/1`,
  `init_worker/1`), `lib/aerospike/node_supervisor.ex` (confirm
  `lazy: false` is the active default and assert it explicitly), new
  unit tests under `test/aerospike/node_pool_test.exs`.
- **Non-goals**: No change to how a pool is *created* by the Tender.
  No retry-on-connect logic beyond what NimblePool already provides via
  `{:remove, reason}` and `:init_worker` re-scheduling.
- **Guidance**: NimblePool already eagerly calls `init_worker/1` for
  each of `pool_size` workers during its `Supervisor.init/1` reduce when
  `lazy` is not set. Make that explicit in `NodeSupervisor.start_pool/2`
  (pass `lazy: false` or document the default) so a future change to
  NimblePool's default cannot silently break warm-up.

  The current `init_worker/1` returns `{:remove, {:connect_failed, err}}`
  on connect failure. NimblePool reacts to `{:remove, _}` during startup
  by sending itself `{:init_worker}`, which retries asynchronously. That
  is acceptable steady-state behaviour, but for warm-up we want to
  distinguish "one worker failed to connect and will retry" (fine) from
  "every worker failed to connect" (the pool is useless and the Tender
  should hear about it). Tier 2 adds the per-node state machine that
  actually acts on this distinction; Tier 1.5 only needs to log
  pool-wide warm-up progress at `:info` / `:warning` via `Logger` and
  keep the current `{:remove, _}` path intact.

  New tests should cover: (a) pool boot with a Fake that scripts N
  successful connects runs `init_worker/1` N times before any checkout
  (assert by counting Fake `{:connect, _, _}` calls), (b) pool boot
  with one scripted connect failure still allows checkout on the
  remaining workers and eventually re-initialises the failed worker,
  and (c) a pool configured with `pool_size: 3` has three workers
  available for concurrent checkout without any checkout paying a
  connect cost.
- **Escalate if**: The Fake transport cannot express per-connect-attempt
  scripting (each call to `Fake.connect/3` currently returns a generic
  success — confirm this and either extend the Fake minimally or take
  the decision back to the user).
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/node_pool_test.exs`
  - Existing Tier 1 tests pass unchanged:
    `mix test test/aerospike/node_supervisor_test.exs test/integration/get_pool_test.exs`

### Task 3: Idle-deadline eviction via handle_ping/2
- **Status**: done
- **Goal**: Workers that sit idle longer than a configurable deadline
  are closed and replaced on the next checkout, using NimblePool's
  native `:worker_idle_timeout` + `handle_ping/2` callback.
- **Prerequisites**: Task 2 (warm-up path is well understood and
  tested before we add eviction on top of it).
- **Scope**: `lib/aerospike/node_pool.ex` (add `handle_ping/2`),
  `lib/aerospike/node_supervisor.ex` (thread `:idle_timeout_ms` and
  `:max_idle_pings` start-opts through to `NimblePool.start_link/1` as
  `:worker_idle_timeout` and `:max_idle_pings`), new unit tests.
- **Non-goals**: No cluster-level eviction policy. No metric or
  telemetry for idle events — logging only. No cross-pool coordination.
- **Guidance**: `handle_ping(conn, pool_state)` should return `{:remove,
  :idle}` so NimblePool closes the worker via `terminate_worker/3`,
  which already calls `transport.close/1`. Pick a default
  `idle_timeout_ms` that is conservative enough to match Aerospike's
  default `proto-fd-idle-ms` of 60_000 ms; make it explicit rather
  than inheriting NimblePool's behaviour. Default `max_idle_pings` to a
  small number (e.g. 2) so a big pool does not drop all idle workers in
  a single cycle — the roadmap deliberately allows "replace on next
  checkout", so we do not need to re-establish dropped workers
  eagerly.

  Unit tests should use a Fake + a short `idle_timeout_ms` (e.g.
  50 ms) and assert: (a) a worker that is checked out and returned
  within the deadline is kept, (b) a worker that sits idle past the
  deadline is closed (the Fake observes a `{:close, ref}`) and the
  next checkout sees a fresh `conn.ref`, (c) `max_idle_pings` bounds
  how many workers get dropped per cycle.
- **Escalate if**: The Fake's `close/1` accounting cannot express
  "closed during ping" distinct from "closed during pool shutdown".
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/node_pool_test.exs`
  - Tier 1 tests unchanged.

### Task 4: Split read-deadline from total-op deadline
- **Status**: done
- **Goal**: `NodeTransport.command/2` grows an explicit read-deadline
  parameter distinct from the caller's overall timeout, so a slow node
  can blow its per-read budget without consuming the caller's whole
  operation budget. The total-op budget still lives with the caller (in
  `Aerospike.Get` today); the read deadline is plumbed down to
  `:gen_tcp.recv/3`.
- **Prerequisites**: Task 1 (the buffering decision affects how many
  `recv` calls a single `command/2` issues, and therefore how the
  deadline is applied).
- **Scope**: `lib/aerospike/node_transport.ex` (callback signature),
  `lib/aerospike/transport/tcp.ex` (thread deadline through
  `send_recv`/`recv_message`), `lib/aerospike/transport/fake.ex`
  (accept and ignore/record the deadline), `lib/aerospike/get.ex`
  (pass a deadline derived from the `:timeout` option), relevant tests
  under `test/aerospike/transport/` and `test/integration/`.
- **Non-goals**: No retry layer. No deadline propagation across pool
  boundaries. No per-read-vs-per-frame sub-splitting. The caller's
  total-op deadline remains owned by `Aerospike.Get`, not by the
  transport.
- **Guidance**: The roadmap is explicit that `command/2` takes a
  socket-read deadline. Keep the signature change minimal:

      @callback command(conn(), request :: iodata(), deadline_ms :: non_neg_integer()) ::
                  {:ok, binary()} | {:error, Aerospike.Error.t()}

  Drop the implicit `recv_timeout` stored in `%Transport.Tcp{}` or
  keep it only as a default when the callback's deadline is `nil` —
  decide and document in `notes.md` before implementing. Tier 1's
  principle is "no backwards-compatibility shims", so prefer removing
  the struct field and always passing the deadline through the
  callback.

  In `Aerospike.Get.execute/4`, compute the read deadline as a
  fraction of the `:timeout` option (e.g. same value; the whole point
  is that a single-read command already spends its budget on the read).
  A future retry layer in Tier 2 will pick a smaller per-attempt
  deadline. Document that decision in `notes.md` under `Phase
  Boundaries`.

  Update `Aerospike.Transport.Fake.command/3` to accept the deadline
  and record it (the Fake's consume path does not actually wait, but
  tests may assert on the deadline passed). Update all existing call
  sites — `Get`, tender-path if any, tests.
- **Escalate if**: The callback signature change ripples into modules
  outside the stated scope (e.g. the Tender calls `command/2`
  directly — confirm by grepping).
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/transport/ test/aerospike/node_pool_test.exs test/integration/`
  - A new test in `test/aerospike/transport/tcp_test.exs` (or existing
    equivalent) proves that `command/3` honours the read deadline
    (use a TCP server fixture or the Fake, depending on what already
    exists).

### Task 5: Document passive-mode framing and regression-test fragmentation
- **Status**: done
- **Goal**: Task 1 concluded (see `notes.md` Finding 7) that passive
  `{:packet, :raw}` framing with `:gen_tcp.recv/3` already handles
  server-side fragmentation correctly and that the one-header-one-body
  two-call pattern is optimal for the current non-pipelined request
  contract. This task locks that decision into code comments and adds
  a regression test that proves fragmentation is transparent, so a
  future change cannot silently regress the guarantee.
- **Prerequisites**: Task 4 (the read deadline must already be plumbed
  through so the regression test uses the post-Task-4 `command/3`
  shape).
- **Scope**: `lib/aerospike/transport/tcp.ex` (`@moduledoc` plus a
  short `@doc` on `recv_message/1` or the equivalent private function,
  citing Finding 7 of the plan notes). Tests: extend
  `test/aerospike/transport/tcp_test.exs` (create if absent) with a
  fragmentation test that stands up an ephemeral `:gen_tcp` listener,
  sends the reply in two `:gen_tcp.send/2` calls with a small delay
  between them, and asserts `command/3` still returns the assembled
  body.
- **Non-goals**: No new buffering code. No `:gen_tcp.recv(socket, 0,
  deadline)` buffered helper. No per-connection read buffer. No
  active-mode sockets. No compression. No request pipelining — Tier
  1.5 explicitly defers this under `Phase Boundaries`. If any of
  these emerge as necessary, stop and escalate.
- **Guidance**: The regression test is the load-bearing artifact.
  Structure it so the listener accepts one connection, reads the
  request, then sends `<<header_bytes::binary-size(4)>>`, sleeps for a
  few ms, sends `<<rest_of_header::binary, body::binary>>`, and closes.
  `command/3` must return `{:ok, body}`. The moduledoc addition should
  be short — two to three sentences citing passive-mode framing and
  referencing `notes.md` Finding 7 for the full reasoning; do not
  duplicate the paragraph.
- **Escalate if**: The regression test cannot be written without
  adding new transport machinery (e.g. the test requires changes to
  `Transport.Tcp` beyond the signature already in place after Task 4).
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/transport/`
  - Tier 1 integration smoke test passes against
    the local single-node server:
    `mix test test/integration/get_pool_test.exs`

### Task 6: Concurrency smoke test and regression check
- **Status**: done
- **Goal**: Prove the four changes above do not regress the Tier 1
  concurrency smoke test and that the warm-up + read-deadline +
  buffering changes produce the expected behaviour end-to-end against
  a real Aerospike server.
- **Prerequisites**: Tasks 2, 3, 4, 5 complete.
- **Scope**: `test/integration/get_pool_test.exs` (extend if needed),
  possibly a new smoke test file under `test/integration/`. No
  production code changes in this task.
- **Non-goals**: No performance benchmarks. No load tests. No telemetry
  assertions. No Tier 2 work.
- **Guidance**: Run the existing concurrent-checkout test at a higher
  concurrency (e.g. 100 tasks against a `pool_size: 10` pool) and
  confirm: no timeouts, all reads succeed, the first request does not
  pay a connect cost (assert timing or inspect logs), and an idle
  pool left sitting for `idle_timeout_ms` evicts and re-opens on the
  next cycle. Use `@moduletag :integration` (not `:cluster`) so the
  test runs against `docker compose up -d`'s single node.

  If any of the four hardening items produces an observable regression
  against the Tier 1 smoke test, stop and escalate — "Tier 1 tests
  pass unchanged" is an exit-criteria line, not a best-effort target.
- **Escalate if**: The Tier 1 concurrency smoke test regresses on the
  `pool_size: 1` baseline; that is a plan-level signal, not an
  in-task fix.
- **Checks**:
  - `mix test test/integration/get_pool_test.exs`
  - `mix test` (full suite, single-node profile)
  - Notes appended to `log.md` describing the smoke-test outcome.
