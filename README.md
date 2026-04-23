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
  `Aerospike.Op`, `Aerospike.Op.List`, and `Aerospike.Op.Map`
- Batch reads: `batch_get/4`
- Scans: `stream!/3`, `all/3`, `count/3`
- Secondary-index queries: `query_stream!/3`, `query_all/3`, `query_count/3`,
  `query_page/3`
- Query admin/runtime helpers: `create_index/4`, `drop_index/4`,
  `query_aggregate/6`, `query_execute/4`, `query_udf/6`
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

key = Aerospike.Key.new("test", "demo", "hello")

{:ok, _meta} = Aerospike.put(:aerospike, key, %{"count" => 1})
{:ok, record} = Aerospike.get(:aerospike, key)
```

Required startup options are `:name`, `:transport`, `:hosts`, and
`:namespaces`. Cluster options such as retry, pool, breaker, and auth settings
are validated synchronously by `Aerospike.start_link/1` before the cluster
runtime boots. Use `Aerospike.Cluster.ready?/1` to observe when the published
cluster view is ready to route commands.

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

- `batch_get/4` supports only `bins: :all` and `:timeout`
- `operate/4` supports the currently admitted tuple/CDT surface, not the full
  historical operate breadth
- query filters use `Aerospike.Filter` values instead of pre-encoded bytes
- broader expression filters, general UDF package management, and a wider
  policy surface remain deferred
- scan/query streams are lazy at the outer `Enumerable` boundary only; the
  current runtime still buffers each node before yielding that node's records
- `query_all/3` and `query_page/3` require `query.max_records`
- scan/query helpers that support node targeting take `node: node_name` in
  `opts` instead of separate `_node` function families
- query cursors resume partition progress; they are not snapshot tokens
- `%Aerospike.Txn{}` is an immutable handle backed by ETS tracking in the
  started cluster, not a transaction owner process

The write-family proof does not yet claim broad TTL semantics beyond the paths
currently exercised in live validation.

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

The repo exposes explicit test categories so the command surface matches the
environments the suite actually needs:

- `mix test.unit` — deterministic default suite, no live Aerospike required
- `mix test.coverage` — deterministic suite with the configured coverage gate
- `mix test.integration.ce` — live Community Edition single-node proofs
- `mix test.integration.cluster` — live three-node cluster proofs
- `mix test.integration.enterprise` — live Enterprise Edition proofs
- `mix test.integration.all` — all live proofs
- `mix test.live` — CE single-node + cluster live proofs, but not EE
- `mix validate` — format, compile, credo, unit, and coverage gate

If you prefer one operator-facing entry point, the repo-local `Makefile` wraps
the same categories:

- `make test-unit`, `make test-coverage`, `make validate`
- `make deps-up`, `make deps-cluster-up`, `make deps-enterprise-up`
- `make test-ce`, `make test-cluster`, `make test-enterprise`
- `make test-live`, `make test-all`, `make deps-down`

Deterministic baseline:

```bash
mix test.unit
mix test.coverage
mix validate
mix dialyzer
```

Community Edition single-node live proofs:

```bash
docker compose up -d
mix test.integration.ce
```

Community Edition three-node cluster proofs:

```bash
docker compose -f ../aerospike_driver/docker-compose.yml --profile cluster down -v
docker compose -f ../aerospike_driver/docker-compose.yml --profile cluster up -d aerospike aerospike2 aerospike3
mix test.integration.cluster
```

Enterprise Edition proofs:

```bash
docker compose --profile enterprise down -v
docker compose --profile enterprise up -d aerospike-ee aerospike-ee-tls aerospike-ee-pki aerospike-ee-security aerospike-ee-security-tls
mix test.integration.enterprise
```

All live proofs:

```bash
mix test.integration.all
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
