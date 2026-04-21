# Aerospike Spike

`aerospike_driver_spike/` is the review repo for the Aerospike Elixir client
architecture. It is where startup contracts, runtime instrumentation, and
command-shape decisions are exercised before they are carried into the shipped
library.

This repo is not the publishable client and it does not promise a stable public
API. The narrower review question is whether the codebase presents a coherent,
honest operator and developer surface for the command families it already
proves.

## What This Repo Proves

The current public entry point proves one supervised cluster runtime plus these
command families:

- Unary CRUD: `get/3`, `put/4`, `exists/2`, `touch/2`, `delete/2`
- Unary operate: `operate/4` with a narrow write/read subset plus
  `Aerospike.Op`, `Aerospike.Op.List`, and `Aerospike.Op.Map`
- Batch reads: `batch_get/4`
- Scans: `stream!/3`, `all/3`, `count/3`, and the `*_node` variants
- Secondary-index queries: `query_stream!/3`, `query_all/3`, `query_count/3`,
  `query_page/3`, and the `*_node` variants
- Query admin/runtime helpers: `create_index/4`, `drop_index/4`,
  `query_aggregate/6`, `query_execute/4`, `query_execute_node/5`,
  `query_udf/6`, `query_udf_node/7`
- Transactions: `transaction/2`, `transaction/3`, `commit/2`, `abort/2`,
  `txn_status/2`

The runtime also proves startup validation at `Aerospike.start_link/1`,
supervised node pools and Tender-driven discovery, and telemetry across
checkout, transport, info/login, retry, and tend paths.

## Supported Validation Profiles

Support in this repo is profile-based, not a semver promise over Aerospike
server versions. Both compose files still resolve `:latest`, so the honest
claim is limited to the exact images exercised during validation.

| Profile | Purpose | Where it runs |
| --- | --- | --- |
| CE single-node | default local proof for unary, write-family, and index-query flows | `docker compose up -d` in this repo |
| CE three-node cluster | cluster routing, batch, and scan proofs | `docker compose --profile cluster up -d aerospike aerospike2 aerospike3` in `../aerospike_driver/` |
| EE single-node variants | transactions, TLS, auth, and combined operator-surface smoke | `docker compose --profile enterprise up -d ...` in this repo |

Reviewers should record the resolved image ids when they run the final gate.
This repo does not yet pin an Aerospike version matrix.

## Quick Start

Start the Community Edition single-node profile:

```bash
docker compose up -d
```

Then open `iex -S mix` in this repo and start one cluster manually:

```elixir
{:ok, _sup} =
  Aerospike.start_link(
    name: :spike,
    transport: Aerospike.Transport.Tcp,
    seeds: [{"127.0.0.1", 3000}],
    namespaces: ["test"],
    tend_trigger: :manual,
    pool_size: 2
  )

:ok = Aerospike.Tender.tend_now(:spike)

key = Aerospike.Key.new("test", "demo", "hello")

{:ok, _meta} = Aerospike.put(:spike, key, %{"count" => 1})
{:ok, record} = Aerospike.get(:spike, key)
```

The public startup surface is intentionally small. Required options are
`:name`, `:transport`, `:seeds`, and `:namespaces`. Cluster options such as
retry, pool, breaker, and auth settings are validated synchronously by
`Aerospike.start_link/1` through `Aerospike.Supervisor`.

## Command Boundaries

This repo is deliberately narrower than a full Aerospike client.

- `batch_get/4` supports only `bins: :all` and `:timeout`
- `operate/4` proves simple tuple operations plus the list/map CDT helpers
- query filters use `Aerospike.Filter` values instead of pre-encoded bytes
- broader expression filters, general UDF package management, and a wider
  policy surface remain deferred
- scan/query streams are lazy at the outer `Enumerable` boundary only; the
  current runtime still buffers each node before yielding that node's records
- `query_all/3`, `query_all_node/4`, `query_page/3`, and `query_page_node/4`
  require `query.max_records`
- query cursors resume partition progress; they are not snapshot tokens
- `%Aerospike.Txn{}` is an immutable handle backed by ETS tracking in the
  started cluster, not a transaction owner process

The write-family proof does not claim broad TTL semantics yet. The live
evidence only covers the currently exercised paths.

## Telemetry Contract

The spike emits a fixed telemetry taxonomy under `[:aerospike, ...]`. Use
`Aerospike.Telemetry` as the contract source instead of copying raw lists into
handlers or metrics modules.

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
handler_id = "aerospike-spike-logger"

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

## Review Proof Commands

Run commands from this directory unless the command explicitly says otherwise.

Deterministic baseline:

```bash
mix format --check-formatted
mix compile --warnings-as-errors
mix credo --strict
mix test --seed 0
mix test --cover --seed 0
mix dialyzer
```

Community Edition single-node live proofs:

```bash
mix test --include integration test/integration/get_test.exs --seed 0
mix test --include integration test/integration/write_family_test.exs --seed 0
mix test --include integration test/integration/index_query_test.exs --seed 0
```

Community Edition three-node cluster proofs:

```bash
docker compose -f ../aerospike_driver/docker-compose.yml --profile cluster down -v
docker compose -f ../aerospike_driver/docker-compose.yml --profile cluster up -d aerospike aerospike2 aerospike3
mix test --include integration --include cluster test/integration/batch_get_test.exs --seed 0
mix test --include integration --include cluster test/integration/scan_test.exs --seed 0
```

Enterprise Edition proofs:

```bash
docker compose --profile enterprise down -v
docker compose --profile enterprise up -d aerospike-ee aerospike-ee-tls aerospike-ee-pki aerospike-ee-security aerospike-ee-security-tls
mix test --include integration --include enterprise test/integration/txn_test.exs --seed 0
mix test --include integration --include enterprise test/integration/tls_test.exs --seed 0
mix test --include integration --include enterprise test/integration/auth_test.exs --seed 0
mix test --include integration --include enterprise test/integration/operator_surface_smoke_test.exs --seed 0
```

The deterministic coverage threshold is configured as part of the repo surface.
If `mix test --cover --seed 0` fails, the review gate is still open.

## Where To Look In Code

- `lib/aerospike.ex` for the public entry point and top-level docs
- `lib/aerospike/supervisor.ex` for startup validation and cluster ownership
- `lib/aerospike/telemetry.ex` for the supported telemetry contract
- `test/integration/write_family_test.exs` for the basic CE proof
- `test/integration/index_query_test.exs` for the live index-query proof
- `test/integration/operator_surface_smoke_test.exs` for the combined EE
  operator proof
