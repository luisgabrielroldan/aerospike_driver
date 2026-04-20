# Tier 2.5 — Wire Protocol Breadth

- **Status**: pending

## Objective

Widen the spike's wire-protocol surface so it talks to the server matrix
a released driver has to support, not just the single CE build the
earlier tiers were validated against. Concretely: validate the 8-byte
proto header on every inbound frame, accept the `AS_MSG_COMPRESSED`
(type 4) reply frame and transparently inflate it, optionally compress
outbound AS_MSG requests above a threshold when the server advertises
support, capture server features during bootstrap so compression (and
future capability-gated features) can be negotiated per-node, and let
operators choose between `services-clear-std` / `services-clear-alt`
for peer discovery so clients on a different subnet can route to the
right endpoints.

Everything lands in `aerospike_driver_spike/`. Tier 2's exit criteria
continue to hold: single-writer ETS discipline, Tender remains the
only writer of cluster-state tables, `NodeTransport` stays the I/O
seam.

## Constraints

- Keep changes inside `aerospike_driver_spike/`. Do not touch the
  `aerospike_driver/` release repo.
- Preserve single-writer ETS discipline. Per-node features captured
  during bootstrap live on the Tender's in-memory node record, not in
  a new ETS table. If a future tier needs hot-path reads of features
  (the breaker and retry layer today do not), that migration is a
  separate plan — do not pre-build it here.
- `NodeTransport` stays the seam. Compression on the reply path and
  optional compressed send both land inside `Aerospike.Transport.Tcp`;
  the behaviour gains at most one new option on `command/3`
  (`:use_compression`), not a new callback. Scan/query streaming
  frames stay out of scope — Tier 4's `multi_command` equivalent will
  revisit compression on multi-frame replies.
- No backwards-compatibility shims between tiers. Rename or reshape
  freely; update every implementer (`Transport.Tcp`, `Transport.Fake`,
  tests) in the same task.
- No new features deferred to later tiers: no telemetry events (log
  only), no TCP / auth / TLS work (Tier 3), no new commands beyond
  GET, no policy structs beyond what compression negotiation needs,
  no rack-aware or `services-alternate` switching logic that is more
  than a static start-opt.
- Follow `.claude/skills/coding-standards/SKILL.md`: `mix format`,
  `mix credo --strict`, `mix compile --warnings-as-errors`,
  alphabetised aliases, underscored integer literals, typed
  `Aerospike.Error` returns.
- Batch-plumb every new start option through
  `Aerospike.start_link/1` → `Tender` → `NodeSupervisor.start_pool/2`
  / `NodePool.init_worker/1` (the Tier 2 convention; see
  `notes.md` Finding 4 for why the compression toggle has to flow
  all the way to the pool worker and not stop at the Tender).

## Assumptions

- The roadmap's "proto v2 and v3" phrasing is wrong. Every official
  client (C `AS_PROTO_VERSION`, Go `_CL_MSG_VERSION`, Java
  `CL_MSG_VERSION`) hard-codes version 2, and the C client explicitly
  rejects any non-v2 header. This plan treats proto-version work as
  *validation* (assert version == 2, error otherwise) rather than
  *negotiation*; see `notes.md` Finding 1. If server-side 8.x
  introduces a new version, that is a future tier's problem.
- The roadmap's "partition-map v4 / v5 variants" phrasing is also
  imprecise. The real variants are the info-key family `replicas`
  (modern, regime-aware), `replicas-all` (legacy, no regime), and
  the long-deprecated `replicas-master` / `replicas-prole`. The
  spike already parses `replicas` and `replicas-all`
  (`lib/aerospike/protocol/partition_map.ex`). See `notes.md`
  Finding 2: we audit the parser against the reference clients'
  decoder and only widen the set of accepted formats if a reference
  format is provably unsupported today. We do **not** invent new
  format variants.
- Message-level compression on the reply path is the load-bearing
  correctness item: a server configured with
  `enable-compression true` can unilaterally return a type-4 frame
  with the body wrapped in zlib, and the spike's current
  `Transport.Tcp.command/3` discards `type` after decoding the
  header. This is an unacknowledged correctness bug for any
  compression-enabled server today, not just a feature gap.
- Compressed send is opt-in per call (the Go / Java clients guard it
  behind `policy.UseCompression` and a 128-byte `COMPRESS_THRESHOLD`).
  This plan lifts the threshold constant verbatim and does **not**
  try to auto-enable based on server features — the same server
  that advertises `pipelining` may have compression disabled for a
  namespace, and the client cannot know per-namespace state.
- `services-clear-std` vs. `services-clear-alt` is a static,
  cluster-wide toggle at `Aerospike.start_link/1` (Go
  `ClientPolicy.UseServicesAlternate`). The Tier 2.5 scope is the
  *toggle*, not a general IP-map / alternate-alias resolver; the
  Go client's `IpMap` is out of scope (Tier 4 or later).
- Server-version matrix: we target CE latest + CE N-1 as the
  roadmap requires, verified by running the existing integration
  suite once against each container. No multi-server docker profile
  is added — swap the image tag in `docker-compose.yml` under a
  comment, run the suite, revert. See `notes.md` Finding 5 for why
  this beats a permanent multi-version profile.

## Tasks

### Task 1: Validate proto version on every reply
- **Status**: done
- **Goal**: `Aerospike.Transport.Tcp.command/3` and
  `Aerospike.Transport.Tcp.info/2` reject any reply whose proto header
  reports a `version` other than 2, returning a typed
  `Aerospike.Error{code: :parse_error}` with a message that names the
  observed version. The reply's `type` is also validated against the
  expected message type per call (info → 1, command → 3 or 4;
  type 4 handling arrives in Task 2 — this task paves the way by
  routing on `type` rather than discarding it).
- **Prerequisites**: none
- **Scope**: `lib/aerospike/transport/tcp.ex` (add a `validate_header/2`
  guard, route on `type` rather than discarding it),
  `lib/aerospike/protocol/message.ex` (tighten `decode_header/1` docs
  so callers know version/type are both consumed),
  `test/aerospike/transport/tcp_test.exs` and
  `test/aerospike/protocol/message_test.exs` (new negative tests).
  The `Aerospike.Transport.Fake` test double gets a Task-1-visible
  behaviour change only if its scripted replies need to emit version
  bytes explicitly — today the Fake shortcuts the header entirely, so
  the Fake changes are limited to forwarding a new `:reply_version`
  option where Task 2's compressed-reply tests will need it.
- **Non-goals**: No compressed-reply handling (Task 2). No change to
  `Aerospike.Get`, `Aerospike.Tender`, or any cluster-state module —
  this task is transport-local. No new error codes; reuse
  `:parse_error` with a descriptive message (existing callers
  already pattern-match by code, and widening the set ripples into
  the retry classifier).
- **Guidance**: Today `send_recv/3` calls `decode_header/1`, matches
  on `{:ok, {version, type, length}}`, and then ignores `version`;
  `info/2` filters by `type == @type_info` but `command/3` does not
  filter at all. The fix is a single predicate per call path:
    1. `info/2` already rejects non-info types via the `with`
       pattern; make the rejection explicit with a typed
       `Aerospike.Error{code: :parse_error}` instead of the current
       "expected info reply (type N), got type M" message that lives
       inside the `else` branch. Centralise the message construction
       in a `type_mismatch_error/2` helper so Task 2 can reuse it.
    2. `command/3` grows a `validate_command_type/1` that accepts
       `@type_as_msg` today and will accept `@type_compressed` in
       Task 2 (leave the clause for type-4 commented out with a
       `# TODO: Task 2` to keep the diff self-contained — that
       comment disappears in Task 2's edit).
    3. Both paths call a shared `validate_version/1` that returns
       `{:error, %Error{code: :parse_error, message: "..."}}` for
       any `version != 2`.
  Edge cases:
    - An info-over-AS_MSG mix (server sends a type-3 frame to an
      info request) is already rejected today; keep the shape.
    - A zero-length body with a valid header is a legal server
      reply for some admin flows; the current code handles it via
      `recv_body/3`'s `length == 0` clause. Do not tighten that.
  Tests must cover: version-3 reply → `:parse_error`; command path
  receiving a type-1 frame → `:parse_error`; info path receiving a
  type-3 frame → `:parse_error`; version-2 type-3 reply still
  decodes. Use the Fake transport with scripted replies.
- **Escalate if**: a reference client is discovered that explicitly
  accepts version != 2 for some capability we need (e.g. a future
  info-v3 variant). Today none exist; if one shows up, flag before
  widening the guard.
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/transport/ test/aerospike/protocol/message_test.exs`
  - Tier 1 / 1.5 / 2 invariant tests unchanged:
    `mix test test/aerospike/tender_test.exs test/aerospike/node_pool_test.exs test/integration/get_pool_test.exs`

### Task 2: Accept and inflate compressed reply frames
- **Status**: done
- **Goal**: When the server replies with an `AS_MSG_COMPRESSED` frame
  (type 4), `Aerospike.Transport.Tcp.command/3` transparently inflates
  the payload and returns the uncompressed AS_MSG body to the caller.
  From `Aerospike.Get`'s perspective nothing changes — it still calls
  `AsmMsg.decode/1` on the returned bytes. The compressed frame's
  wire layout is fixed by the Go / Java / C reference clients and is
  captured in `notes.md` Finding 3; the task lifts that layout
  verbatim.
- **Prerequisites**: Task 1 (the type dispatch in `command/3` is where
  the type-4 clause lives; routing on `type` is the Task 1 scaffold
  this task fills in).
- **Scope**: `lib/aerospike/transport/tcp.ex` (type-4 reply branch,
  new `decompress_body/1` helper), `lib/aerospike/protocol/message.ex`
  (a named parser for the "compressed AS_MSG wrapper" — an 8-byte
  uncompressed-size prefix followed by zlib-compressed bytes; name
  it `decode_compressed_payload/1` so callers do not have to know
  the wire-level detail), new unit tests in
  `test/aerospike/transport/tcp_test.exs` and
  `test/aerospike/protocol/message_test.exs`. No `Transport.Fake`
  change — see `notes.md` / *Plan Corrections / Task 2* for why the
  originally-scoped `{:reply_compressed, iodata}` script action
  would be dead code.
- **Non-goals**: No compressed-send path (Task 3). No feature
  negotiation logic (Task 4). No scan / query streaming frames
  (Tier 4). No configuration to *disable* inflation — a compressed
  reply is a server decision; the client either handles it or
  breaks. No alternate compression algorithms (the protocol is
  zlib-only per `command.go:_AS_MSG_TYPE_COMPRESSED` usage).
- **Guidance**: Wire layout of a compressed reply (verified against
  Go `multi_command.go:150-173` and `command.go:3621-3622`; captured
  in `notes.md` Finding 3):
    1. Outer 8-byte proto: `version=2`, `type=4`,
       `length=compressedSz + 8`. The `+8` accounts for the inner
       uncompressed-size header the compressed body wraps.
    2. First 8 bytes of the body are a big-endian `uint64` that is
       the *uncompressed* AS_MSG frame's total size including its
       own 8-byte header.
    3. Remaining `length - 8` bytes are zlib-compressed. Inflating
       them produces a complete inner AS_MSG frame — its own
       8-byte proto header (version=2, type=3, inner length) plus
       the AS_MSG body.
  Implementation sketch:
    - `command/3`, after `recv_body/3` for a type-4 reply, reads
      the first 8 bytes of the body as the inner uncompressed
      size (consume-and-verify: if the inflated length does not
      match, return `:parse_error`), inflates the remaining
      bytes with `:zlib.uncompress/1`, and re-parses the inflated
      bytes as a type-3 proto frame via
      `Message.decode_header/1` + the existing body slicing.
    - `:zlib.uncompress/1` raises on malformed input; wrap it in a
      `try` and surface `:parse_error` with the zlib error reason.
    - Once inflated, the inner frame's `type` must be `3`
      (AS_MSG). A compressed frame wrapping a non-AS_MSG body is
      unspecified in the reference clients; reject with
      `:parse_error`.
  Fake transport: add a `{:reply_compressed, iodata}` scripted
  action that wraps the provided inner body with a zlib-compressed
  wrapper so tests exercise the exact branch the real server would
  hit. Do **not** teach the Fake to "auto-decide" whether to
  compress — the test script is authoritative.
  Tests must cover: compressed reply of a well-formed GET response
  round-trips via the same code path as the uncompressed one;
  compressed reply with a corrupted zlib stream → `:parse_error`;
  compressed reply advertising a wrong inner size → `:parse_error`;
  compressed reply wrapping a type-1 body → `:parse_error`.
- **Escalate if**: the Erlang `:zlib.uncompress/1` limit (default
  ~16 MiB per call) turns out to be insufficient for a legitimate
  reply. Tier 2.5 does not scale to scan/query replies, so this
  should not trigger; if it does, revisit with the user rather than
  switching to a streaming inflater here.
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/transport/ test/aerospike/protocol/message_test.exs`

### Task 3: Optional compressed AS_MSG send above a threshold
- **Status**: pending
- **Goal**: `Aerospike.Transport.Tcp.command/3` grows an opt-in
  `:use_compression` option. When set, requests whose encoded size
  exceeds a fixed threshold (128 bytes, matching Go
  `_COMPRESS_THRESHOLD`) are wrapped in a type-4 compressed frame
  before `:gen_tcp.send/2`. Requests below the threshold are sent
  uncompressed even when the option is set, matching reference
  behaviour. The option defaults to `false` so every Task 1 / 2
  test continues to exercise the plain path.
- **Prerequisites**: Task 2 (the compressed-wire layout is already
  captured in `Message.decode_compressed_payload/1` and its
  symmetric encoder gets added here).
- **Scope**: `lib/aerospike/transport/tcp.ex` (new
  `maybe_compress/2` applied to the iodata request before send),
  `lib/aerospike/protocol/message.ex` (new
  `encode_compressed_payload/1` — the symmetric counterpart to the
  Task 2 decoder), `lib/aerospike/node_transport.ex` (document the
  new `:use_compression` option on `command/3`), tests in
  `test/aerospike/transport/tcp_test.exs` and
  `test/aerospike/protocol/message_test.exs`.
- **Non-goals**: No public-API plumbing yet — `:use_compression`
  stays a transport option that the Tender / pool sets, not a
  `Aerospike.get/3` per-command knob. No auto-toggle based on
  server features (Task 4 wires the server-side capability check).
  No compression for info frames (info replies are small and
  reference clients do not compress them). No alternate levels;
  `:zlib.compress/1` default level matches Go's
  `gzip.DefaultCompression` closely enough for steady-state
  payloads; pick level explicitly only if the Task 4 roundtrip
  test shows divergence.
- **Guidance**: Symmetric wire layout (Go
  `command.go:3574-3627`):
    1. Compress the complete outbound AS_MSG frame (8-byte proto
       header + body) via `:zlib.compress/1`.
    2. Prepend an 8-byte big-endian `uint64` = the *uncompressed*
       frame's total size (header + body).
    3. Wrap the result in a new type-4 proto header: `version=2`,
       `type=4`, `length = compressedSize + 8`.
  Threshold check uses `IO.iodata_length/1` on the pre-compressed
  request; below 128 bytes, short-circuit and send the uncompressed
  iodata as today. `:zlib.compress/1` is allowed to produce output
  larger than the input for tiny payloads — after compression,
  verify `compressedSize + 8 < uncompressedSize`; if not,
  short-circuit and send uncompressed (matches Go
  `command.go:3611-3625` fallback).
  Option plumbing: `command/3`'s current signature is
  `command(conn, request, deadline_ms)`. Extend to
  `command(conn, request, deadline_ms, opts \\ [])` where
  `opts` accepts `:use_compression` (boolean). Update the
  `Aerospike.NodeTransport` behaviour's `@callback` to match.
  `Aerospike.Transport.Fake` grows the same option for parity; its
  behaviour remains "send what the script says" — the option is
  ignored unless the script opts in.
  Tests must cover: request above threshold with
  `use_compression: true` produces a type-4 frame whose inflated
  payload equals the original iodata bytes; request below
  threshold with the option set is still sent as type 3;
  compression that would inflate the payload falls back to type 3;
  round-trip through a compressed reply (Task 2) still works when
  the request was also compressed.
- **Escalate if**: reference-client parity turns out to require a
  non-default zlib level. Go uses `DefaultCompression` (6); Erlang
  `:zlib.compress/1` uses `default` which is also 6. No escalation
  expected.
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/transport/ test/aerospike/protocol/message_test.exs`

### Task 4: Capture server features on bootstrap
- **Status**: pending
- **Goal**: The Tender's per-node bootstrap (`bootstrap_seed/3` and
  `register_new_node/5`) captures the node's `features` info-key
  value and stores it on the in-memory node record as a set of
  feature atoms. Downstream code (Task 5 wiring + future tiers)
  consults the set to decide whether to enable compression on that
  node's pool workers. No ETS writes are added; the feature set
  lives only on the Tender's node record and flows into
  `NodeSupervisor.start_pool/2` as a start-opt at pool boot. If a
  node fails the `features` probe it is still registered — features
  absence degrades to "all optional features off", not a bootstrap
  failure.
- **Prerequisites**: none (this is an independent cluster-state
  extension; can run in parallel with Tasks 1–3).
- **Scope**: `lib/aerospike/tender.ex` (add `"features"` to the
  bootstrap info probe, parse the `;`-separated feature list on
  the node record), `lib/aerospike/protocol/info.ex` (a new
  `parse_features/1` that returns a `MapSet` of atoms;
  unrecognised features are dropped silently — future tiers may
  widen the recognition list), `lib/aerospike/node_pool.ex`
  (accept `:features` in `init_pool/1` and stash on `pool_state`
  so Task 5 can read it), tests in
  `test/aerospike/tender_test.exs` and
  `test/aerospike/protocol/info_test.exs`.
- **Non-goals**: No persistent feature cache — if a node
  disconnects and reconnects its features are re-fetched. No
  peer-discovery-driven feature refresh; if the `features` list
  changes mid-lifecycle, Tier 2.5 does not react (the Go client
  caches features at node-add time; we match that). No UI /
  telemetry exposure of features (Tier 3). No feature-driven
  behaviour in this task — this task only *captures* features;
  Task 5 is where compression negotiation consumes them.
- **Guidance**: Info-key name and format (verified against Go
  `node.go:refreshFeatures` and Java
  `Node.java:updateFeatures`): the info command is `features`
  (or, for older servers, falls back to `cluster-name` probes —
  ignore that fallback; CE servers we target report `features`).
  Response body is a `;`-separated list of tokens, e.g.
  `replicas;peers;pipelining;batch-index;cluster-stable;...`.
  Parser:
    - `String.split/2` on `;`, trim, drop empties.
    - Map recognised tokens to atoms via a small allow-list that
      lives in `Aerospike.Protocol.Info`. Tier 2.5 only needs
      `compression` and `pipelining` on the allow-list; every
      other token becomes a `{:unknown, raw_string}` tuple that
      is preserved in the set for diagnostic logging but not
      matched on behaviour-wise.
    - Return value: `MapSet.t(atom() | {:unknown, String.t()})`.
  Tender plumbing:
    - In `bootstrap_seed/3`, fetch `["node", "features"]` in a
      single info call instead of the current `["node"]`.
    - On `features` parse failure or missing key, register the
      node with `features: MapSet.new()`; log at `:debug`.
    - Thread `features` through `register_new_node/5` onto the
      node record. Add the field to the `nodes_status/1` snapshot.
    - Pass `features` into `NodeSupervisor.start_pool/2` opts so
      it lands on `NodePool`'s `pool_state`.
  Tests must cover: bootstrap with a scripted
  `features=compression;pipelining` reply produces the expected
  MapSet; missing `features` key registers the node with an
  empty MapSet and logs at `:debug`; the snapshot surfaces the
  set; a pool started for a node receives `:features` in its
  state.
- **Escalate if**: the server returns `features` via a different
  info key on some CE branch (e.g. 8.x). Tier 2.5 targets CE
  latest + CE N-1; if N-1 diverges, add the fallback in this
  task rather than deferring.
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/tender_test.exs test/aerospike/protocol/info_test.exs test/aerospike/node_pool_test.exs`

### Task 5: Wire compression opt into the pool path when the server supports it
- **Status**: pending
- **Goal**: When (a) `Aerospike.start_link/1` was started with
  `use_compression: true` and (b) the node's `features` MapSet
  includes `:compression`, every `NodePool.checkout!/3` call that
  the command path makes passes `use_compression: true` into the
  underlying `NodeTransport.command/4`. Any other combination sends
  uncompressed. The choice is per-node, decided at command-dispatch
  time, and does not require a pool restart if features are
  re-fetched (Tier 2.5 does not re-fetch, but the plumbing must
  not encode "features are frozen at pool-start" because a future
  tier may).
- **Prerequisites**: Tasks 3 and 4.
- **Scope**: `lib/aerospike.ex` (expose a cluster-level
  `:use_compression` start-opt; default `false`),
  `lib/aerospike/tender.ex` (thread `use_compression` through
  `start_link/1` → `init/1` and expose it on the `node_handle/2`
  return so `Aerospike.Get` picks it up per-call),
  `lib/aerospike/get.ex` (read `use_compression` from the handle
  and pass to `transport.command/4`), `lib/aerospike/node_pool.ex`
  (no changes expected — the option flows through the
  `checkout!/3` callback, not pool state), tests in
  `test/aerospike/get_test.exs`,
  `test/aerospike/tender_test.exs`, and a round-trip
  `test/integration/compression_test.exs` that exercises the full
  stack against the Fake transport with a compressed reply.
- **Non-goals**: No per-command override of `:use_compression`
  (Tier 4 policy work). No "try compressed, fall back to plain on
  server error" logic — if the server advertises compression
  support and then rejects a compressed frame, that is a server
  bug and surfaces as a plain `:parse_error`. No live
  feature-list refresh triggered by a compression rejection
  (Tier 3 adds the generalised reconnect).
- **Guidance**: `node_handle/2`'s current shape is
  `%{pool, counters, breaker}`. Extend to
  `%{pool, counters, breaker, use_compression}`. The boolean is
  computed in `handle_call({:node_handle, ...}, ...)` as
  `cluster_compression_on? and MapSet.member?(features, :compression)`.
  `Aerospike.Get.check_breaker/3` already has the handle in scope
  before `NodePool.checkout!/3`; pass the boolean into
  `do_get/5` (signature widens) and into the underlying
  `transport.command/4` opts. The transport behaviour's
  `@callback` signature was widened in Task 3 — wire it through
  consistently.
  Start-opt plumbing:
    - `Aerospike.start_link/1` accepts `:use_compression` (boolean,
      default `false`) and forwards it to `Tender.init/1`.
    - `Tender.init/1` stores the boolean on its state under
      `:use_compression` and consults it in `handle_call` for
      `node_handle/2`.
    - Nothing is written to ETS — the boolean lives on the
      Tender and on the per-call handle.
  Tests must cover: cluster-level off + node supports compression
  → plain frame; cluster-level on + node does not advertise
  compression → plain frame; cluster-level on + node supports
  compression → compressed frame. Round-trip: cluster-level on +
  node supports compression + server replies compressed → decoded
  record matches. The "verify the wire bytes" parts drive the
  command path through a real `:gen_tcp` loopback listener in the
  `Aerospike.Transport.Tcp` shape rather than `Transport.Fake` —
  the Fake bypasses `Tcp.command/4` entirely, so it cannot assert
  the on-the-wire compressed frame. See `notes.md` / *Plan
  Corrections / Task 2* for why.
- **Escalate if**: `node_handle/2`'s call rate shows up in the
  Tier 1 concurrency smoke. The handle already allocates a map
  per call; adding one more field is sub-ns but the GenServer
  hop is the real cost. If Tier 2's existing benchmarks flag a
  regression, switch to a per-node compile-time flag cached on
  `pool_state` at pool start (accepting the "features frozen at
  pool start" caveat called out in Task 4's guidance).
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/get_test.exs test/aerospike/tender_test.exs`
  - `mix test test/integration/compression_test.exs --include integration`
  - Tier 1.5 concurrency smoke regression:
    `mix test test/integration/get_pool_test.exs --include integration`

### Task 6: `services-clear-alt` toggle for peer discovery
- **Status**: pending
- **Goal**: `Aerospike.start_link/1` accepts a
  `:use_services_alternate` boolean (default `false`). When
  `true`, the Tender's peer-discovery info call uses
  `peers-clear-alt` instead of `peers-clear-std`, and the
  Tender's bootstrap info call uses `service-clear-alt` instead
  of the implicit default (today there is no explicit service
  info call — the seed list is trusted verbatim; see `notes.md`
  Finding 4 for what has to change). The choice is cluster-wide
  and static, matching Go `ClientPolicy.UseServicesAlternate`.
- **Prerequisites**: none (this is orthogonal to the compression
  track and can be interleaved).
- **Scope**: `lib/aerospike/tender.ex` (compute the info-key name
  from state at `discover_peers/1` time;
  `@refresh_node_info_keys` currently hard-codes
  `"peers-clear-std"` in `collect_peers/2` — lift the constant
  to state-derived), `lib/aerospike/protocol/peers.ex` (no
  changes expected — the reply format is identical between the
  two variants), `lib/aerospike.ex` (accept and forward the
  start-opt), tests in `test/aerospike/tender_test.exs`.
- **Non-goals**: No `IpMap` (Go's explicit IP translation
  table) — Tier 4 or later. No TLS variants
  (`peers-tls-std` / `peers-tls-alt`) — those arrive with Tier 3
  TLS support. No per-namespace or per-rack overrides. No
  auto-detection of whether the server supports
  `services-alternate` (Go does not check either — a
  misconfigured server returns an error and the client does not
  second-guess).
- **Guidance**: Current call site:
  `state.transport.info(node.conn, ["peers-clear-std"])` in
  `collect_peers/2`. Replace with a state-derived key name:
    ```
    peer_info_key =
      if state.use_services_alternate,
        do: "peers-clear-alt",
        else: "peers-clear-std"
    ```
  Thread `use_services_alternate` through `init/1` onto
  `state`. The `%{key => value}` reply map lookup needs the
  same state-derived key, not a hard-coded match — the existing
  `%{"peers-clear-std" => value}` pattern break is the
  mechanical refactor this task owns.
  Bootstrap symmetry: today `bootstrap_seed/3` fetches
  `["node"]` and, after Task 4, `["node", "features"]`. The
  `service-clear-alt` key is only useful when the seed list is
  being *extended* via peers — the seed itself is dialled
  verbatim from `connect_opts`. Document this explicitly in a
  code comment inside `bootstrap_seed/3` so a future reader
  does not add a spurious `service-clear-alt` fetch here. The
  toggle only affects peer discovery, not seed dialling.
  Tests must cover: `use_services_alternate: false` emits
  `peers-clear-std` on the Fake (assert via the Fake's
  `:info` call log); `use_services_alternate: true` emits
  `peers-clear-alt`; a peer reply parsed under either key
  produces identical node registrations.
- **Escalate if**: a CE N-1 server disagrees on the
  `peers-clear-alt` reply format (Go assumes symmetry; if the
  integration run in Task 7 disagrees, escalate before bending
  the parser).
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test test/aerospike/tender_test.exs`

### Task 7: Server-matrix integration verification
- **Status**: pending
- **Goal**: Run the full Tier 1 / 1.5 / 2 invariant suite plus the
  new Tier 2.5 compression and services-alternate paths against
  CE latest (the current docker image) and CE N-1 (a tagged older
  image). Record pass / fail, latency distribution deltas, and
  any protocol quirks in `log.md`. If CE N-1 reveals a divergence,
  open an escalation (document the observation; do not paper over
  it in-task).
- **Prerequisites**: Tasks 1–6 complete.
- **Scope**: `docker-compose.yml` (swap the CE image tag to N-1
  for one run; revert after logging), `log.md` (two runs'
  results), optionally `README.md` if CE N-1 requires a
  configuration note. No production-code changes.
- **Non-goals**: No permanent multi-version docker profile (a
  single `docker-compose.yml` image tag swap + revert keeps the
  repo small — the roadmap does not call for a matrix harness).
  No automated CI pipeline for the matrix (Tier 3 observability
  work, if any, revisits this). No new tests — the existing
  suite is expected to cover the matrix; gaps surfaced here get
  logged as follow-ups in `spike-docs/follow-ups/`.
- **Guidance**: The CE image tag progression is documented on
  Aerospike's docker hub; pick the highest `N-1` image whose
  minor version is exactly one less than CE latest. Before
  swapping, note the current tag in `log.md` so the revert is
  trivial. Suggested steps:
    1. `docker compose down -v` (full reset per
       `CLAUDE.md` "integration test environment hygiene").
    2. Swap the image tag in `docker-compose.yml` (comment the
       old tag inline).
    3. `docker compose up -d`; wait for `cluster-stable:`.
    4. Run the full integration suite:
       `mix test --include integration --seed 0`.
    5. Run the full suite again with a different seed to
       confirm determinism.
    6. Capture per-file pass/fail and any new flake signatures
       in `log.md`.
    7. Revert the tag; rerun once against CE latest to confirm
       we are back to green.
  If CE N-1 breaks a test that passes on CE latest:
    - Is it a protocol-level divergence (e.g. an info key name
      change)? → escalate. Do **not** add a server-version fork
      in production code as part of this task.
    - Is it an environmental quirk (e.g. different default TTL
      semantics)? → document in `notes.md` and open a follow-up
      plan.
  Budget: ~20 minutes wall-clock for the full matrix (two image
  pulls + two suite runs + revert). If the N-1 image exceeds
  1 GB and pulls are slow, run the N-1 pass as a foreground
  task and do not parallelise.
- **Escalate if**: CE N-1 rejects the current compression frame
  layout, reports a different `features` info-key format, or
  requires a different proto version guard. Any of those is a
  plan-shaping finding, not a Task 7 fix.
- **Checks**:
  - `mix format --check-formatted`
  - `mix credo --strict`
  - `mix compile --warnings-as-errors`
  - `mix test --include integration` (on both CE latest and
    CE N-1; results in `log.md`)
  - `docker compose ps` confirms the current tag matches the
    committed `docker-compose.yml` on session exit.
