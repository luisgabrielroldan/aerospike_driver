# Tier 1.5 — Pool Hardening — Notes

## Sources Reviewed

- `spike-docs/ROADMAP.md` — Tier 1.5 scope, exit criteria, out-of-scope
  list.
- `aerospike_driver_spike/lib/aerospike/node_pool.ex` — current
  NimblePool callbacks, checkout contract, `terminate_worker/3`
  semantics.
- `aerospike_driver_spike/lib/aerospike/node_supervisor.ex` — how pools
  are started; no `lazy:` or `worker_idle_timeout:` passed today.
- `aerospike_driver_spike/lib/aerospike/transport/tcp.ex` — passive
  `{:packet, :raw}` socket, `recv_timeout` stored on the struct,
  separate header/body `:gen_tcp.recv/3` calls.
- `aerospike_driver_spike/lib/aerospike/node_transport.ex` — frozen
  behaviour; Tier 1.5 will grow it.
- `aerospike_driver_spike/lib/aerospike/get.ex` — only caller of
  `command/2` today; owns the `:timeout` option.
- `aerospike_driver_spike/deps/nimble_pool/lib/nimble_pool.ex` —
  `:lazy`, `:worker_idle_timeout`, `:max_idle_pings`, `handle_ping/2`.

## Findings That Shape The Plan

1. **NimblePool is already eager.** `lazy: false` is the default;
   `NimblePool.init/1` reduces over `1..pool_size` calling
   `init_worker/1` synchronously. "Warm-up at pool start" is therefore
   mostly a *confirm and document* task, not a new feature. The real
   risk is a future NimblePool default change silently breaking us,
   hence Task 2 pins `lazy: false` explicitly.

2. **Idle eviction is a built-in.** NimblePool exposes
   `:worker_idle_timeout` + `handle_ping/2` + `:max_idle_pings`. Tier
   1.5 wires them, it does not re-invent them. No manual timer, no
   ETS.

3. **`command/2` has exactly one caller today: `Aerospike.Get`.** The
   behaviour signature change in Task 4 ripples to `Transport.Tcp`,
   `Transport.Fake`, and `Get` — a small, containable blast radius.
   Grep before declaring scope final.

4. **Passive `{:packet, :raw}` already handles server fragmentation.**
   `:gen_tcp.recv(socket, N, timeout)` with `N > 0` blocks until N
   bytes arrive, regardless of how the server fragments. The roadmap's
   "partial-reply buffering" is not about correctness — it is about
   syscall count and/or giving the parser a single buffer. Task 1
   locks the interpretation before Task 5 commits code.

5. **Aerospike's default `proto-fd-idle-ms` is 60_000 ms.** Our idle
   deadline should default to something *below* that so we close
   sockets before the server does; otherwise the first post-idle
   request sees the server-side close. Picked as the default in Task
   3.

6. **The spike has no external users.** Rename or delete freely:
   `Transport.Tcp`'s `recv_timeout` field can go away in Task 4 if the
   deadline always flows through the callback.

7. **Task 1 decision — buffering is a documentation no-op.** The Go
   client's `Connection.Read` loops over `conn.Read(buf[total:length])`
   because Go's `net.Conn.Read` returns whatever the kernel has ready,
   not exactly N bytes (see
   `official_libs/aerospike-client-go/connection.go:253-301` and the
   two-call header/body pattern in
   `official_libs/aerospike-client-go/record_parser.go:43-76`). Erlang's
   `:gen_tcp.recv(socket, N, timeout)` on a passive `{:packet, :raw}`
   socket with `N > 0` already blocks inside the VM until exactly N
   bytes arrive (or the timeout fires / socket closes), so server-side
   fragmentation is invisible to the caller — the spike does in two
   `recv` calls what Go does in a loop around `Read`. Coalescing
   header+body into one `recv` is not possible without reading the
   header first (body length lives in the header), and reading past the
   header into a per-connection buffer only helps when the same reply
   is parsed incrementally, which the current `{:ok, body}` contract
   does not expose. Request pipelining (multiple in-flight requests
   whose replies arrive interleaved) is the one case where a
   per-connection read buffer would be necessary, and Tier 1.5's
   `Phase Boundaries` explicitly defers pipelining. Therefore Task 5
   shrinks to a documentation change: add a `@moduledoc` note on
   `Transport.Tcp` citing passive-mode framing, the two-read pattern,
   and the deferral of pipelining, and write a regression test that
   asserts a reply split across two `:gen_tcp.send` chunks by a fake
   server is still reassembled correctly. No new buffering code.

## Phase Boundaries

- **Tier 2**: per-node state machine, retry, circuit breaker, rebalance
  detection, per-node request counters. Tier 1.5 must **not** add the
  "pool is fully dead" escalation — it only logs. The distinction
  belongs to the Tier 2 state machine.
- **Tier 3**: telemetry, TCP options tuning, auth, TLS. Do not grow
  `Transport.Tcp`'s connect opts in Tier 1.5 beyond what is needed to
  thread the read deadline and buffer size.
- **Deferred**: async request pipelining. Buffering in Task 5 must not
  prepare for multiplexed in-flight requests — one request, one reply.

## Open Questions To Resolve During Execution

- **Task 1**: ~~exact buffering strategy~~ **Resolved**: see Finding 7.
  No new buffering code; Task 5 becomes a documentation-plus-regression-
  test change.
- **Task 4**: ~~should `Aerospike.Get` derive the read deadline from
  the `:timeout` option (same value) or reserve some overhead for
  encode / decode / routing?~~ **Resolved**: `:timeout` passed
  verbatim. See `Task 4 — Deadline policy and behaviour ripple`
  point 2.

## Task 2 — Fake transport extensions

Task 2 added two helpers to `Aerospike.Transport.Fake` that Task 3
(idle eviction) and Task 5 (regression test) will likely reuse:

- `Fake.script_connect(fake, node_id, :ok | {:error, %Error{}})`
  queues per-`connect/3`-call outcomes. Empty queue → default path
  (success unless `disconnect/2` is set).
- `Fake.connect_count(fake, node_id)` returns the cumulative number of
  `connect/3` invocations, including failures.

`disconnect/2` remains a global "all subsequent connects fail" flag.
Use `script_connect/3` for "first N attempts fail, rest succeed"
patterns where global flipping is too coarse.

For Task 3, the corresponding hook for assertions like "the worker
was closed during ping, not pool shutdown" does **not** exist in the
Fake. The `{:close, ref}` GenServer call is the only signal today —
counting close calls before vs. after a known shutdown event is the
likely path. Extend the Fake with a `close_count/2` only if the
distinction is necessary.

## Task 3 — Fake `close_count/2` added

Task 3 needed deterministic evidence that `handle_ping/2` closed an
idle worker without a corresponding checkout + `:close` check-in.
`Aerospike.Transport.Fake` now tracks per-node close counts in the
same shape as `connect_count/2`:

- `Fake.close_count(fake, node_id)` returns cumulative `close/1`
  invocations for connections addressing `node_id`.

Close accounting happens inside `handle_call({:close, ref}, ...)`
when `ref` is still in `state.conns`. A close on an unknown ref
(double-close, ref never opened) is not counted.

`handle_ping/2` logs idle evictions at `:debug`, matching the
connect/disconnect logging pattern introduced in Task 2. Task 4's
read-deadline plumbing should keep that logging style if it adds new
transport-level log lines.

## Task 3 — NimblePool idle semantics worth remembering

Two behaviours are easy to miss when reading `nimble_pool`'s source:

1. **Workers retain their original idle timestamp until pinged.** When
   `handle_ping/2` returns `{:ok, _}` the worker stays eligible on the
   next cycle because NimblePool deliberately does not bump the
   metadata timestamp (see `deps/nimble_pool/lib/nimble_pool.ex`
   around line 947, "if we are checking for idle resources again and
   the timestamp is the same, it is because it has to be checked
   again"). `{:remove, :idle}` avoids that loop; `{:ok, _}` would
   ping the same worker every cycle.
2. **`max_idle_pings` caps evictions per cycle, not per deadline.**
   So with `pool_size: 4, max_idle_pings: 1, worker_idle_timeout:
   50`, full drain takes four cycles (~200 ms), not one. Future
   idle-behaviour tests should budget poll windows against this.

## Task 4 — Deadline policy and behaviour ripple

Decisions locked in during Task 4. Tier 2's retry work will revisit
them; Tier 1.5 does not.

1. **`command/2` → `command/3(conn, iodata, deadline_ms)`.** The third
   argument is a non-negative integer in milliseconds applied to each
   `:gen_tcp.recv/3` on a passive `{:packet, :raw}` socket (header and
   body are separate recv calls; see Finding 7). It is intentionally
   not a monotonic deadline — each `recv` gets its own budget. If a
   future reader wonders why: pipelining is out of scope (`Phase
   Boundaries`), so every `command/3` is a strictly sequential
   send-then-recv pair. No retry layer is paying close attention to
   elapsed time yet.

2. **`Aerospike.Get` passes `:timeout` verbatim.** GET is a single
   send + single reply. Encode/decode/checkout overhead is microseconds
   compared to a network `recv`, so using the full op budget as the
   per-read deadline is conservative — same wall-clock behaviour the
   pre-Task-4 `recv_timeout` produced. Tier 2's retry layer is where a
   smaller per-attempt deadline derived from a monotonic op budget
   belongs; do not re-derive it here.

3. **`%Transport.Tcp{}` `:recv_timeout` field removed.** Per Finding
   6 (no external users), renamed to `:info_timeout` because the only
   remaining caller is `info/2`, which has no per-call deadline.
   `info/2` uses `connect_opts[:timeout]` as its default, preserving
   Tender-path behaviour.

4. **`Tender` never calls `command/*`.** Only `Get.execute/4` does.
   Confirmed by grep before the signature change; no Tender-path
   edits were needed.

5. **`Transport.Fake` instrumentation.**
   - `command/3` accepts and records the deadline. Script keys stay
     `{:command, node_id}` — the deadline is not part of the script
     selector.
   - `Fake.last_command_deadline(fake, node_id)` returns the most
     recent deadline observed for that node (or `nil`). Task 5's
     regression test does not need deadline-value assertions, but
     keep the helper — Tier 2's retry tests will.

6. **TCP read-deadline regression test.**
   `test/aerospike/transport/tcp_test.exs` stands up a loopback
   `:gen_tcp.listen/2` socket, accepts once, and either refuses to
   reply (drives the `:timeout` path) or sends a valid header+body
   pair (drives the happy path). The helper `header/1` mirrors
   `Aerospike.Protocol.Message.encode_header/3` inline so the test
   stays isolated from Message internals. Reuse this scaffolding for
   Task 5's fragmentation regression test instead of rolling a new
   listener — keep the two tests in the same file.

## Task 6 — Tender does not forward idle-timeout options

Confirmed while writing the idle-eviction integration test:
`Aerospike.Tender.ensure_pool/4` builds the keyword list for
`NodeSupervisor.start_pool/2` from only
`[node_name, transport, host, port, connect_opts, pool_size]`. Neither
`:idle_timeout_ms` nor `:max_idle_pings` are plumbed from
`Aerospike.start_link/1` / `Tender.start_link/1` through to the pool
today, so every pool created via `Aerospike.start_link` gets
`NodeSupervisor`'s defaults (55_000 / 2).

This is deliberate for Tier 1.5 — the roadmap reserves idle-timeout
policy for the Tier 2 per-node state machine — but any Tier 2 task
that lets operators tune idle policy must:

1. Grow `Tender`'s `state` to hold the user-supplied idle options
   (with the current defaults when absent).
2. Extend `ensure_pool/4` to forward them.
3. Re-do the integration smoke via `Aerospike.start_link` instead of
   the direct `NodeSupervisor.start_pool` path the Task 6 test uses.

Until then, callers that need a shorter idle deadline must start the
pool through `NodeSupervisor.start_pool/2` directly, as the Task 6
idle-eviction integration test does.

## Task 6 — Observing eager warm-up without production instrumentation

The Task 6 smoke test needs a deterministic assertion that every
configured worker is ready before any GET runs. The Fake-based unit
tests use `Fake.connect_count/2`; `Transport.Tcp` has no equivalent
hook. The integration test uses `:queue.len(:sys.get_state(pool).resources)`
instead, which is fragile if NimblePool reshapes its state struct but
works with the current 1.x line and is the minimum-scope option that
avoids adding production instrumentation. If a future NimblePool bump
renames `:resources` or changes it away from `:queue`, the smoke test
is the single location to update.
