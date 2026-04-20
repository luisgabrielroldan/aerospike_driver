# Tier 2.5 — Wire Protocol Breadth — Notes

## Sources Reviewed

- `spike-docs/ROADMAP.md` — Tier 2.5 scope and phase boundaries.
- `aerospike_driver_spike/lib/aerospike/transport/tcp.ex`,
  `protocol/message.ex`, `protocol/asm_msg.ex`,
  `protocol/partition_map.ex`, `protocol/peers.ex`,
  `protocol/info.ex`, `tender.ex`, `get.ex`,
  `node_transport.ex` — current spike surface.
- `aerospike_driver_spike/docs/plans/tier-2-cluster-correctness/plan.md`
  — conventions for task shape, non-goals, escalation, and
  batch-plumbing through `Aerospike.start_link/1` →
  `Tender` → pool.
- `aerospike_driver_spike/docs/plans/tier-3-observability-auth-tls/plan.md`
  and `notes.md` — explicit boundary with Tier 2.5 (Tier 3
  defers proto versioning, compression, IP-alias handling,
  partition-map variants to Tier 2.5).
- `official_libs/aerospike-client-go/command.go`,
  `connection.go`, `multi_command.go`, `client_policy.go`,
  `node_validator.go`, `partition_parser.go` — wire-level
  reference for proto version, compressed frame layout, feature
  negotiation, services-alternate toggle.
- `official_libs/aerospike-client-java/client/src/com/aerospike/client/command/Command.java`,
  `RecordParser.java`, `admin/AdminCommand.java` — cross-check
  version constant and compressed frame shape.
- `official_libs/aerospike-client-c/src/include/aerospike/as_proto.h`,
  `src/main/aerospike/as_proto.c` — cross-check version
  rejection behaviour (the C client is the strictest).
- `MEMORY.md` — existing protocol learnings (message framing,
  admin message layout, body-offset rule, elixir gotchas).

## Findings That Shape The Plan

**Finding 1 — Proto v3 does not exist in any official client.**
C `AS_PROTO_VERSION = 2`, Go `_CL_MSG_VERSION = 2`, Java
`CL_MSG_VERSION = 2L`. The C client's `as_proto.c:86-88` and
`:115-121` explicitly reject any non-2 header with an
`AEROSPIKE_ERR_CLIENT`. Java's `RecordParser.java:60` does the
same. The roadmap's "v2 and v3 negotiation" is therefore a
description of work that does not match the protocol.

Plan adaptation: Task 1 treats the proto-version work as
*validation* — reject any `version != 2` — and routes on `type`
rather than `version`. If a future server does introduce a v3
frame, the failure mode is a well-typed `:parse_error`, which
is a deterministic signal to revisit the plan.

**Finding 2 — There is no "partition-map v4 / v5 variant" in
the reference clients.** Go `partition_parser.go` uses a single
info key, `replicas`. The historical variants that do exist are:
- `replicas` — the modern form, regime-aware, used by
  every current server version.
- `replicas-all` — legacy, no regime field. Already handled
  by `Aerospike.Protocol.PartitionMap.parse_replicas_value/2`
  (via `classify_replicas_all/1`).
- `replicas-master` / `replicas-prole` — pre-3.0 legacy,
  deprecated, not used by any current reference client.

Plan adaptation: no "decoder widening" task. The existing
`parse_replicas_value/2` + `parse_replicas_with_regime/1` pair
already covers the modern + legacy shapes, and the reference
clients do not parse more than that. The intent of the roadmap
item — "handle the fuller set" — is folded into Task 4's
features probe: the Tender captures the `replicas` feature
token (present on every modern server) so future tiers can
assume regime-aware parsing.

**Finding 3 — Compressed frame layout (verified against Go and
Java).**

Outbound (type 4):

```
[0..7]   proto header: version=2, type=4, length = compressedSize + 8
[8..15]  uint64 big-endian: uncompressed frame size (header + body)
[16..]   zlib-compressed bytes of the full uncompressed AS_MSG frame
         (its own 8-byte header + body)
```

Inbound: same layout. After inflating the zlib bytes, the result
is a complete AS_MSG frame with its own 8-byte proto header
(version=2, type=3).

Go reference: `command.go:3574-3627` (encode) +
`multi_command.go:150-173` (decode). Java reference:
`Command.java:2841` (encode) + `CommandRead.java` handling of
`MSG_TYPE_COMPRESSED` on reply.

Threshold for outbound compression: Go
`command.go:128` hard-codes `_COMPRESS_THRESHOLD = 128`. Java
`Command.java` uses the same constant. Requests below the
threshold are sent uncompressed regardless of
`UseCompression`.

Fallback for post-compression bloat: Go
`command.go:3611-3625` checks whether compression actually
shrank the payload; if not, it keeps the uncompressed buffer.
The plan adopts the same check in Task 3.

**Finding 4 — `services-alternate` toggle is static and
cluster-wide.** Go `ClientPolicy.UseServicesAlternate` picks
`peers-clear-std` vs. `peers-clear-alt` and (for TLS)
`peers-tls-std` vs. `peers-tls-alt` at `ClientPolicy`
construction. `node_validator.go:185` reads
`ClientPolicy.serviceString()` to pick the *service* info key
too (for seed-time address canonicalisation), but the spike's
`bootstrap_seed/3` today trusts the seed host/port verbatim and
does not re-canonicalise — so Tier 2.5 only needs to honour
the toggle on peer discovery, not on seed dialling.

Plan adaptation: Task 6 is scoped to the peer-discovery key
swap. A code comment in `bootstrap_seed/3` documents the
intentional asymmetry so a future reader does not add
`service-clear-alt` there on a hunch.

**Finding 5 — Multi-version testing without a docker profile.**
The roadmap's CE latest + CE N-1 exit criterion can be met by
swapping the image tag in `docker-compose.yml` and running the
suite twice. A permanent multi-image profile would add
maintenance burden (which tag counts as "N-1" changes every
release) and encourage "forked-code for old-server" bugs that
hurt the release driver.

Plan adaptation: Task 7 swaps the image tag in a single
transaction, logs both runs' results, and reverts. Any
divergence is an escalation, not a code branch.

**Finding 6 — Compression negotiation needs to see the server's
`features` list, not its `supports-compression` reply.** Go
`node.go:refreshFeatures` fetches a single `features` info key
and splits on `;`. The token `compression` (if present) is the
only signal required. The older `supports-compression` key
returns `true`/`false` for individual capability probes but is
not used by the Go client for compression; we follow Go's
pattern.

Plan adaptation: Task 4 standardises on `features`. The
`supports-compression` key is explicitly out of scope and
mentioned in Task 4's non-goals only if it becomes a point of
contention during implementation.

## Phase Boundaries

- **Tier 2 (done)**: per-node lifecycle, cluster-stable
  agreement, partition-generation short-circuit, rebalance
  classification, counters + circuit breaker + retry.
- **Tier 3 (out of scope for this phase, tracked in roadmap)**:
  TCP tuning options, telemetry, auth, TLS. Task 5's
  `:use_compression` plumbing intentionally flows through the
  same shape Tier 3 will use for `:tls_opts` (both are
  cluster-wide start-opts that land on `Tender` state and ride
  the `node_handle/2` return). Tier 3 can reuse the plumbing;
  it does not have to be rewritten.
- **Tier 4 (out of scope for this phase)**: per-command
  `:use_compression` in the policy struct, `IpMap` (explicit IP
  translation table), rack-aware peer selection, named-bin GET,
  write commands beyond GET, scan / query compression on
  multi-frame replies.

## Open Questions To Resolve During Execution

- **Task 4 feature-token allow-list size.** The Task description
  limits the recognised tokens to `compression` and `pipelining`
  for now. If Task 5 or Task 6 needs to check another token
  (e.g. `peers-clear-alt` feature gating), widen the allow-list
  in the same commit — do not carry the widening to a separate
  task.
- **Task 2 inflation buffer ceiling.** `:zlib.uncompress/1` has
  a default output buffer limit of 16 MiB. GET replies are
  bounded well below that, so Tier 2.5's primary path is fine;
  a scan/query reply wrapping a huge CDT could theoretically
  exceed it, but scan/query is not in scope here. Revisit if
  Task 7's CE N-1 run surfaces a reply that blows the limit.
- **Task 7 image tag selection.** The exact CE N-1 tag depends
  on the state of Aerospike's docker hub at execution time.
  Choose at execution, record in `log.md`.

## Plan Corrections

### Task 2 — `Aerospike.Transport.Fake` scripted action is dead code

Task 2's scope line calls for a `{:reply_compressed, iodata}` scripted
action on `Aerospike.Transport.Fake`. On inspection this is a plan-level
mistake: `Transport.Fake` replaces `Transport.Tcp` at the
`NodeTransport` behaviour seam. `Fake.command/3` returns scripted
bytes directly to the caller without ever calling `Tcp`'s inflate
branch. A `{:reply_compressed, inner_body}` script entry could only
either:

1. Return `{:ok, inner_body}` — identical to the existing
   `script_command(fake, node_id, {:ok, inner_body})`; adding a
   dedicated function is dead API surface.
2. Go through a mini-inflater inside Fake — a test-only reimplementation
   of the production branch that does not test the production code.

Neither shape is useful. Task 2 therefore does **not** change
`Transport.Fake`; Task 2's compressed-reply tests exercise the real
`Transport.Tcp` branch via a loopback `:gen_tcp` listener (the same
harness Task 1 uses for header-validation tests).

Knock-on effect for Task 5: the round-trip test described in Task 5
(`test/integration/compression_test.exs`) must also drive the
compressed branch through a real `:gen_tcp` listener in the
`Aerospike.Transport.Tcp` shape, not through `Transport.Fake`.
Task 5's guidance should be re-read with this in mind when that
task runs.
