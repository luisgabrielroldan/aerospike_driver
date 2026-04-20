# Tier 2.5 — Wire Protocol Breadth — Log

## 2026-04-20 — Plan drafted

Draft written, pending user approval before execution. Key planning
observations recorded in `notes.md`:

- The roadmap's "proto v2 and v3" is factually wrong against every
  reference client; Task 1 reframes the work as *validation* of the
  fixed v2 header rather than *negotiation*.
- The roadmap's "partition-map v4 / v5 variants" has no referent in
  the reference clients; the real variants are the `replicas` vs.
  `replicas-all` info keys, both already handled. Task list drops the
  "variant decoder" work entirely and absorbs its intent into the
  Task 4 features probe (which is how reference clients negotiate
  `replicas` family membership anyway).
- Message-level compression is the load-bearing correctness item of
  the tier — any server with `enable-compression true` can return a
  type-4 frame today and the spike would fail to decode it. Split
  into Task 1 (type validation) + Task 2 (inflate) + Task 3 (deflate)
  + Task 4 (feature capture) + Task 5 (wire-through) to keep each
  task reviewable independently.

No tasks executed yet.

## Task 1 — Validate proto version on every reply

**Date:** 2026-04-20 — **Status:** done

- Files: `lib/aerospike/transport/tcp.ex`,
  `lib/aerospike/protocol/message.ex`,
  `test/aerospike/transport/tcp_test.exs`,
  `test/aerospike/protocol/message_test.exs` (new).
- Checks: format ✓, compile (warnings-as-errors) ✓, credo --strict ✓,
  `mix test test/aerospike/transport/ test/aerospike/protocol/message_test.exs` ✓ (8 doctests, 25 tests),
  Tier 1/1.5/2 invariants ✓ (`tender_test`, `node_pool_test`, `get_pool_test`: 56 tests),
  full `mix test` ✓ (13 doctests, 187 tests), dialyzer ✓.
- Notes: Task executed with the adjustment agreed on plan approval — no
  `# TODO: Task 2` placeholder clause. `validate_command_type/1` today
  accepts only `@type_as_msg`; Task 2 will widen it to
  `{@type_as_msg, @type_compressed}` as part of its own edit, keeping
  the diff self-contained.

## Task 2 — Accept and inflate compressed reply frames

**Date:** 2026-04-20 — **Status:** done

- Files: `lib/aerospike/transport/tcp.ex`,
  `lib/aerospike/protocol/message.ex`,
  `test/aerospike/transport/tcp_test.exs`,
  `test/aerospike/protocol/message_test.exs`.
- Checks: format ✓, compile (warnings-as-errors) ✓, credo --strict ✓,
  `mix test test/aerospike/transport/ test/aerospike/protocol/message_test.exs` ✓ (8 doctests, 34 tests),
  full `mix test` ✓ (13 doctests, 196 tests, 0 failures), dialyzer ✓.
- Notes: `Transport.Fake` was **not** changed. The originally-scoped
  `{:reply_compressed, iodata}` script action would be dead code because
  `Fake.command/3` replaces `Tcp.command/3` at the behaviour seam; a
  scripted compressed reply through Fake cannot exercise the production
  inflate branch. Compressed-reply tests drive `Transport.Tcp` via a
  real loopback `:gen_tcp` listener (same harness as Task 1). See
  `notes.md` / *Plan Corrections / Task 2*; Task 5's round-trip plan
  was adjusted accordingly.
