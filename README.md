# Aerospike Driver

`aerospike_driver_spike/` is the active Aerospike Elixir client library.
Internally this codebase may still be called "the spike", but the public
package identity is `aerospike_driver`: the intended replacement for the older
driver that remains in this workspace as a migration/reference repo.

The goal is a production-grade, OTP-native Aerospike client with a sharper
runtime foundation than the legacy driver: supervised cluster ownership,
deterministic routing, reusable unary execution, explicit streaming paths, and
a concrete test matrix tied to real Aerospike environments.

## Current Surface

The library currently proves these command families through the public
`Aerospike` entry point:

- Unary CRUD: `get/3`, `put/4`, `exists/2`, `touch/2`, `delete/2`
- Unary operate: `operate/4` with the currently admitted write/read subset plus
  `Aerospike.Op`, `Aerospike.Op.List`, `Aerospike.Op.Map`, `Aerospike.Op.Exp`,
  `Aerospike.Op.Bit`, `Aerospike.Op.HLL`, and `Aerospike.Ctx`
- Batch helpers: `batch_get/4`, `batch_get_header/3`, `batch_exists/3`,
  `batch_get_operate/4`, `batch_delete/3`, `batch_udf/6`, and
  heterogeneous `batch_operate/3` entries built with `Aerospike.Batch`
- Root helpers: `start_link/1`, `child_spec/1`, `close/2`, `key/3`,
  `key_digest/3`
- Scans: `scan_stream/3`, `scan_stream!/3`, `scan_all/3`, `scan_all!/3`,
  `scan_count/3`, `scan_count!/3`
- Secondary-index queries: `query_stream!/3`, `query_all/3`, `query_count/3`,
  `query_page/3`
- Unary and write-family commands (`get`, `put`, `delete`, `touch`, `exists`,
  `operate`, `apply_udf`, `add`, `append`, `prepend`) support
  `%Aerospike.Exp{}` via `:filter`.
- Scan and query builders support expression filtering through
  `Scan.filter/2` and `Query.filter/2`.
- Query admin/runtime helpers: `create_index/4`, `create_expression_index/5`,
  `drop_index/4`, `query_aggregate/6`, `query_execute/4`, `query_udf/6`
- Typed geo values through `Aerospike.Geo` and geo secondary-index filters
  through `Aerospike.Filter.geo_within/2` and `Aerospike.Filter.geo_contains/2`
- Enterprise XDR filter management through `set_xdr_filter/4`
- Transactions: `transaction/2`, `transaction/3`, `commit/2`, `abort/2`,
  `txn_status/2`

The runtime also proves:

- Startup validation at `Aerospike.start_link/1`
- Supervised node pools and Tender-driven discovery
- Telemetry across checkout, transport, info/login, retry, and tend paths

## Quick Start

Start the Community Edition single-node profile:

```bash
docker compose up -d
```

Then open `iex -S mix` in this repo and start one cluster manually:

```elixir
{:ok, _sup} =
  Aerospike.start_link(
    name: :aerospike,
    transport: Aerospike.Transport.Tcp,
    hosts: ["127.0.0.1:3000"],
    namespaces: ["test"],
    pool_size: 2
  )

key = Aerospike.key("test", "demo", "hello")

{:ok, _meta} = Aerospike.put(:aerospike, key, %{"count" => 1})
{:ok, record} = Aerospike.get(:aerospike, key)
:ok = Aerospike.close(:aerospike)
```

Required startup options are `:name`, `:transport`, `:hosts`, and
`:namespaces`. Cluster options such as retry, pool, breaker, and auth settings
are validated synchronously by `Aerospike.start_link/1` before the cluster
runtime boots. Use `Aerospike.Cluster.ready?/1` to observe when the published
cluster view is ready to route commands.

## Batch APIs

Batch helpers keep result order aligned with the caller's input order. Read
helpers return explicit per-key tuples, so a missing key is an error tuple
rather than `nil`:

```elixir
keys = [
  Aerospike.key("test", "demo", "one"),
  Aerospike.key("test", "demo", "missing")
]

{:ok, results} = Aerospike.batch_get(:aerospike, keys)
# [
#   {:ok, %Aerospike.Record{}},
#   {:error, %Aerospike.Error{result_code: :key_not_found}}
# ]

ops = [Aerospike.Op.get("count")]
{:ok, results} = Aerospike.batch_get_operate(:aerospike, keys, ops)
```

Helpers that can perform writes or record UDFs return
`%Aerospike.BatchResult{}` entries. Each result includes the target key,
`:ok` or `:error` status, any returned record payload, the error reason, and
whether a write outcome is in doubt:

```elixir
{:ok, delete_results} = Aerospike.batch_delete(:aerospike, keys)

{:ok, udf_results} =
  Aerospike.batch_udf(:aerospike, keys, "records", "mark_seen", [])
```

For curated heterogeneous work, build entries with `Aerospike.Batch` and pass
them to `batch_operate/3`:

```elixir
entries = [
  Aerospike.Batch.read(Aerospike.key("test", "demo", "one")),
  Aerospike.Batch.put(Aerospike.key("test", "demo", "two"), %{"count" => 2}),
  Aerospike.Batch.operate(
    Aerospike.key("test", "demo", "three"),
    [Aerospike.Op.put("count", 3)]
  ),
  Aerospike.Batch.delete(Aerospike.key("test", "demo", "old"))
]

{:ok, results} = Aerospike.batch_operate(:aerospike, entries)
```

The current public batch option surface is intentionally narrow. Batch helpers
accept only the batch-level `:timeout` option; per-entry write policies and
public batch retry options are not exposed.

## Advanced Operate And Geo

`operate/4` accepts primitive tuple operations plus builder modules for CDT,
bit, HyperLogLog, and expression operations. Returned operation values are
accumulated into `%Aerospike.Record{bins: map}` by bin name.

Nested CDT operations use `Aerospike.Ctx` steps:

```elixir
key = Aerospike.key("test", "demo", "nested-profile")

{:ok, _} =
  Aerospike.put(:aerospike, key, %{
    "profile" => %{"events" => []}
  })

{:ok, _record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.List.append("profile", "signed-in",
      ctx: [Aerospike.Ctx.map_key("events")]
    )
  ])
```

Bit operations work on Aerospike blob bins. Plain Elixir binaries are encoded
as strings, so seed bit bins with `{:blob, binary}` when using this client:

```elixir
key = Aerospike.key("test", "demo", "bit-flags")

{:ok, _} =
  Aerospike.put(:aerospike, key, %{"flags" => {:blob, <<0>>}})

{:ok, _} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.Bit.set("flags", 0, 8, <<0b1010_0000>>)
  ])

{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.Bit.count("flags", 0, 8)
  ])

2 = record.bins["flags"]
```

HyperLogLog helpers create, update, and read probabilistic cardinality bins:

```elixir
key = Aerospike.key("test", "demo", "hll-visitors")

{:ok, _} = Aerospike.put(:aerospike, key, %{"seed" => 0})

{:ok, _} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.HLL.init("visitors", 14, 0)
  ])

{:ok, _} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.HLL.add("visitors", ["ada", "grace", "katherine"], 14, 0)
  ])

{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.HLL.get_count("visitors")
  ])

3 = record.bins["visitors"]
```

Geo bins accept typed values from `Aerospike.Geo`, and ordinary secondary
indexes can be created with `type: :geo2dsphere`:

```elixir
key = Aerospike.key("test", "places", "portland")
point = Aerospike.Geo.point(-122.68, 45.52)

{:ok, _} = Aerospike.put(:aerospike, key, %{"loc" => point})

{:ok, task} =
  Aerospike.create_index(:aerospike, "test", "places",
    bin: "loc",
    name: "places_loc_geo_idx",
    type: :geo2dsphere
  )

:ok = Aerospike.IndexTask.wait(task)

region = Aerospike.Geo.circle(-122.68, 45.52, 10_000.0)

query =
  Aerospike.Query.new("test", "places")
  |> Aerospike.Query.where(Aerospike.Filter.geo_within("loc", region))
  |> Aerospike.Query.max_records(100)

{:ok, records} = Aerospike.query_all(:aerospike, query)
```

## Runtime Model

This client is deliberately OTP-first. These runtime modules are internal
implementation details, not additional public entry points:

- `Aerospike.Cluster.Supervisor` owns cluster startup and validation
- `Aerospike.Cluster.Tender` is the single writer for mutable cluster topology state
- `Aerospike.Cluster.NodeSupervisor` and `Aerospike.Cluster.NodePool` own
  per-node transport pools
- routing decisions are derived from published cluster state rather than hidden
  mutable command-local state

That separation is the main architectural difference from the legacy driver the
workspace still carries as a reference implementation.

## Supported Validation Profiles

Support is currently profile-based, not a semver promise over a pinned
Aerospike server matrix. The compose stacks still resolve `:latest`, so the
current support claim is limited to the exact images exercised during
validation.

| Profile | Purpose | Where it runs |
| --- | --- | --- |
| CE single-node | default local proof for unary, write-family, and index-query flows | `docker compose up -d` in this repo |
| CE three-node cluster | cluster routing, batch, and scan proofs | `docker compose --profile cluster up -d aerospike aerospike2 aerospike3` in `../aerospike_driver/` |
| EE single-node variants | transactions, TLS, auth, and combined operator-surface smoke | `docker compose --profile enterprise up -d ...` in this repo |

Reviewers should record the resolved image ids when they run the final gate.

## Command Boundaries

This library is not yet claiming full Aerospike feature parity.

- Batch helpers accept only the batch-level `:timeout` option.
  `batch_get/4` supports only `bins: :all`.
  `batch_get_operate/4` accepts read-only operation lists.
  `batch_operate/3` accepts entries built with `Aerospike.Batch` and does not
  expose per-entry write policies.
- `operate/4` supports the currently admitted tuple, CDT, bit, HyperLogLog,
  and expression operation surface, not the full historical operate breadth
- scan and query paths support `Scan.filter/2` and `Query.filter/2`
  for server-side expression filters. Queries keep secondary-index predicates in
  `Query.where/2` with `Aerospike.Filter`.
- expression-backed secondary indexes require Aerospike 8.1 or newer
- `set_xdr_filter/4` requires an Enterprise server with XDR configured; it can
  set a `%Aerospike.Exp{}` filter or clear the existing filter with `nil`
- broader expression-builder families, general UDF package management, and a
  wider policy surface remain deferred
- scan/query streams are lazy at the outer `Enumerable` boundary only; the
  current runtime still buffers each node before yielding that node's records
- `query_all/3` and `query_page/3` require `query.max_records`
- scan/query helpers that support node targeting take `node: node_name` in
  `opts` instead of separate `_node` function families
- the older bare scan names (`stream!/3`, `all/3`, `all!/3`, `count/3`,
  `count!/3`) remain as deprecated compatibility aliases for one transition
  window
- `close/2`, `transaction/2`, `transaction/3`, `commit/2`, `abort/2`, and
  `txn_status/2` currently require the cluster's registered atom name because
  supervisor and transaction-tracking lookups resolve from that name
- query cursors resume partition progress; they are not snapshot tokens
- `%Aerospike.Txn{}` is an immutable handle backed by ETS tracking in the
  started cluster, not a transaction owner process

The write-family proof does not yet claim broad TTL semantics beyond the paths
currently exercised in live validation.

## Expression-Backed Server Features

Expression builders produce `%Aerospike.Exp{}` values that can be used in
server-side filters, expression-backed indexes, expression operate operations,
and Enterprise XDR filters.

Create an expression-backed secondary index from an expression source:

```elixir
{:ok, task} =
  Aerospike.create_expression_index(:aerospike, "test", "users",
    Aerospike.Exp.int_bin("age"),
    name: "users_age_expr_idx",
    type: :numeric
  )

:ok = Aerospike.IndexTask.wait(task)
```

Target that named index from a normal query predicate:

```elixir
query =
  Aerospike.Query.new("test", "users")
  |> Aerospike.Query.where(
    Aerospike.Filter.range("age", 18, 40)
    |> Aerospike.Filter.using_index("users_age_expr_idx")
  )
```

Read and write expression results through `operate/4`:

```elixir
Aerospike.operate(:aerospike, key, [
  Aerospike.Op.Exp.read("projected", Aerospike.Exp.int_bin("count")),
  Aerospike.Op.Exp.write("computed", Aerospike.Exp.int(99))
])
```

Set or clear an Enterprise XDR expression filter:

```elixir
filter =
  Aerospike.Exp.eq(Aerospike.Exp.int_bin("active"), Aerospike.Exp.int(1))

:ok = Aerospike.set_xdr_filter(:aerospike, "dc-west", "test", filter)
:ok = Aerospike.set_xdr_filter(:aerospike, "dc-west", "test", nil)
```

## Telemetry Contract

The library emits a fixed telemetry taxonomy under `[:aerospike, ...]`. Use
`Aerospike.Telemetry` as the contract source instead of copying raw event lists
into handlers or metrics modules.

- Pool checkout: `[:aerospike, :pool, :checkout, :start | :stop]`
- Command transport: `[:aerospike, :command, :send, ...]` and
  `[:aerospike, :command, :recv, ...]`
- Info and login RPCs: `[:aerospike, :info, :rpc, ...]`
- Tender spans: `[:aerospike, :tender, :tend_cycle, ...]` and
  `[:aerospike, :tender, :partition_map_refresh, ...]`
- Instant events: `[:aerospike, :node, :transition]` and
  `[:aerospike, :retry, :attempt]`

Common metadata:

- `:node_name` on pool, transport, info/login, retry, and node-transition
  events
- `:attempt` and `:deadline_ms` on command send/recv spans
- `:commands` on info/login spans (`[:login]` for auth handshakes)
- `:from`, `:to`, and `:reason` on node-transition events
- `:classification` and `:remaining_budget_ms` on retry events
- `:bytes` on command-recv `:stop`

```elixir
handler_id = "aerospike-driver-logger"

:ok =
  :telemetry.attach_many(
    handler_id,
    Aerospike.Telemetry.handler_events(),
    fn event, measurements, metadata, _config ->
      IO.inspect({event, measurements, metadata}, label: "aerospike")
    end,
    nil
  )
```

Use `Aerospike.Telemetry.handler_events/0` for whole-surface subscriptions and
derive narrower families from `span_prefixes/0` or the individual helper
functions when you only want part of the contract.

## Development And Validation

Run commands from this directory unless the command explicitly says otherwise.

The repo keeps the Mix aliases explicit because the environments are genuinely
different:

- `mix test.unit` — deterministic default suite, no live Aerospike required
- `mix test.coverage` — deterministic suite with the configured coverage gate
- `mix test.coverage.live` — unit + CE + cluster coverage in one run
- `mix test.coverage.all` — unit + CE + cluster + EE coverage in one run
- `mix test.integration.ce` — live Community Edition single-node proofs
- `mix test.integration.cluster` — live three-node cluster proofs
- `mix test.integration.enterprise` — live Enterprise Edition proofs
- `mix test.integration.all` — all live proofs
- `mix test.live` — CE single-node + cluster live proofs, but not EE
- `mix validate` — format, compile, credo, unit, and coverage gate

For day-to-day use, prefer the repo-local `Makefile`, which minimizes the
operator-facing surface to three entry points:

- `make test PROFILE=unit|ce|cluster|enterprise|live|all`
- `make coverage PROFILE=unit|live|all`
- `make deps PROFILE=ce|cluster|enterprise|all`
- `make validate`, `make deps-down`

The older per-suite targets still exist as compatibility shims:

- `make test-unit`, `make test-coverage`, `make test-coverage-live`, `make test-coverage-all`
- `make test-ce`, `make test-cluster`, `make test-enterprise`, `make test-live`, `make test-all`
- `make deps-up`, `make deps-cluster-up`, `make deps-enterprise-up`, `make deps-all-up`

Deterministic baseline:

```bash
make test PROFILE=unit
make coverage PROFILE=unit
make validate
mix dialyzer
```

Community Edition single-node live proofs:

```bash
make deps PROFILE=ce
make test PROFILE=ce
```

Community Edition three-node cluster proofs:

```bash
make deps PROFILE=cluster
make test PROFILE=cluster
```

Enterprise Edition proofs:

```bash
make deps PROFILE=enterprise
make test PROFILE=enterprise
```

All live proofs:

```bash
make deps PROFILE=all
make test PROFILE=all
```

Coverage with live suites folded into the same run:

```bash
make coverage PROFILE=live
make coverage PROFILE=all
```

If `mix test.coverage` fails, the review gate is still open.

## Where To Look In Code

- `lib/aerospike.ex` for the public entry point and top-level docs
- `lib/aerospike/cluster/supervisor.ex` for startup validation and cluster ownership
- `lib/aerospike/telemetry.ex` for the supported telemetry contract
- `test/integration/write_family_test.exs` for the basic CE proof
- `test/integration/index_query_test.exs` for the live index-query proof
- `test/integration/operator_surface_smoke_test.exs` for the combined EE
  operator proof
