# Aerospike Driver

[![Hex.pm](https://img.shields.io/hexpm/v/aerospike_driver.svg)](https://hex.pm/packages/aerospike_driver)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/aerospike_driver/)
[![License](https://img.shields.io/hexpm/l/aerospike_driver.svg)](https://github.com/luisgabrielroldan/aerospike_driver/blob/main/LICENSE)
[![Last Updated](https://img.shields.io/github/last-commit/luisgabrielroldan/aerospike_driver.svg)](https://github.com/luisgabrielroldan/aerospike_driver/commits/main)

`aerospike_driver` is an OTP-native Aerospike client for Elixir. It provides a
supervised cluster runtime, partition-aware routing, pooled TCP/TLS
connections, and a public API for records, batch commands, scans, queries,
CDTs, expressions, UDFs, security administration, and transactions.

## Installation

Add `aerospike_driver` to your dependencies:

```elixir
def deps do
  [
    {:aerospike_driver, "~> 0.3.0"}
  ]
end
```

## Quick Start

Start with an Aerospike node available at `127.0.0.1:3000`.

Define an application Repo module:

```elixir
defmodule MyApp.Repo do
  use Aerospike.Repo, otp_app: :my_app
end
```

Configure and supervise it from your application:

```elixir
# config/config.exs
config :my_app, MyApp.Repo,
  transport: Aerospike.Transport.Tcp,
  hosts: ["127.0.0.1:3000"],
  namespaces: ["test"],
  pool_size: 2

# lib/my_app/application.ex
children = [
  MyApp.Repo
]
```

Then write and read records without repeating the cluster name:

```elixir
key = MyApp.Repo.key("test", "demo", "user:1")

{:ok, _meta} =
  MyApp.Repo.put(key, %{
    "name" => "Ada",
    "visits" => 1
  })

{:ok, record} = MyApp.Repo.get(key)
%{"name" => "Ada", "visits" => 1} = record.bins
```

Required startup options are `:transport`, `:hosts`, and `:namespaces`. The
Repo uses its module name as the default cluster name; pass `:name` to
`use Aerospike.Repo` when you need a different registered name. Startup also
accepts cluster discovery and auth options such as `:cluster_name`,
`:seed_only_cluster`, `:application_id`, `:auth_mode`, `:login_timeout_ms`,
and `:min_connections_per_node`.

Startup validation runs synchronously through the underlying
`Aerospike.start_link/1`, so malformed cluster, retry, pool, auth, and
transport options fail before the runtime is published.

Use `Aerospike.Cluster.ready?(MyApp.Repo.conn())` when your application wants
to wait until the first cluster view is available for routing.

## Feature Overview

The canonical low-level public entry point is `Aerospike`. Applications can
define a thin `Aerospike.Repo` module to bind that API to one supervised
cluster. The main supported areas are:

- Record commands: `get/3`, `get_header/3`, `put/4`, `exists/3`, `touch/3`,
  `delete/3`, `add/4`, `append/4`, and `prepend/4`
- Single-record operation lists through `operate/4`, `Aerospike.Op`,
  `Aerospike.Op.List`, `Aerospike.Op.Map`, `Aerospike.Op.Bit`,
  `Aerospike.Op.HLL`, `Aerospike.Op.Exp`, and `Aerospike.Ctx`
- Batch reads, writes, deletes, operations, and record UDF calls through the
  batch helper functions and `Aerospike.Batch`
- Scans and secondary-index queries through `Aerospike.Scan`,
  `Aerospike.Query`, `Aerospike.Filter`, `scan_stream/3`, `scan_all/3`,
  `scan_page/3`, `query_stream/3`, `query_all/3`, `query_page/3`, and count
  helpers
- Server-side expressions through `Aerospike.Exp`, including command filters,
  expression operations, expression-backed indexes, XDR filters, core
  expression builders, and CDT/bit/HLL expression helpers
- UDF package lifecycle, record UDF execution, query aggregates, and background
  query execution
- Enterprise security administration helpers for users, roles, privileges,
  whitelists, quotas, and PKI users
- Multi-record transactions through `transaction/2`, `transaction/3`,
  `commit/2`, `abort/2`, and `txn_status/2`
- Operator helpers for info commands, node discovery, truncation, runtime
  metrics, pool warm-up, and telemetry

Typed data helpers are available for keys, records, errors, geo values, pages,
partition filters, UDF tasks, index tasks, execution tasks, batch results, and
transaction handles.

## Runtime Model

The client starts a supervised cluster per configured `:name`. Internally, that
cluster owns node discovery, partition maps, per-node connection pools, retry
budgets, circuit-breaker state, and transaction tracking. Most applications
should define a Repo module as their boundary:

```elixir
defmodule MyApp.Repo do
  use Aerospike.Repo, otp_app: :my_app
end
```

The Repo is a thin facade over one cluster. It does not perform schema mapping,
changeset validation, or object reflection. Lower-level code can call the
registered cluster directly:

```elixir
Aerospike.get(:aerospike, key)
Aerospike.query_all(:aerospike, query)
Aerospike.transaction(:aerospike, fn txn -> ... end)
```

Telemetry events are emitted under the `[:aerospike, ...]` prefix. Use
`Aerospike.Telemetry.handler_events/0` as the subscription source instead of
copying event names into your application.

## Guides

Use these guides for task-oriented examples:

- [Getting Started](guides/getting-started.md)
- [Record Operations](guides/record-operations.md)
- [Batch Operations](guides/batch-operations.md)
- [Operate, CDT, And Geo](guides/operate-cdt-and-geo.md)
- [Queries And Scans](guides/queries-and-scans.md)
- [Expressions And Server Features](guides/expressions-and-server-features.md)
- [UDFs And Aggregates](guides/udfs-and-aggregates.md)
- [Operator And Admin Tasks](guides/operator-and-admin-tasks.md)
- [Security And XDR](guides/security-and-xdr.md)
- [Transactions](guides/transactions.md)
- [Telemetry And Runtime Metrics](guides/telemetry-and-runtime-metrics.md)

## Operational Notes

- Public policy options stay keyword-based. Single-record, batch, scan, and
  query helpers expose the implemented timeout, retry, routing, filter,
  consistency, generation, commit, and batch dispatch fields through ordinary
  call opts rather than caller-constructed policy structs.
- Scan and query streams are lazy at the `Enumerable` boundary, but the current
  runtime buffers each node's records before yielding that node downstream.
- `query_all/3` and `query_page/3` require `query.max_records`;
  `scan_page/3` requires `scan.max_records`.
- Query cursors resume partition progress; they are not snapshot tokens.
- Expression-backed secondary indexes require Aerospike 8.1 or newer.
- Enterprise security, transactions, TLS, auth, and XDR helpers require
  appropriately configured Enterprise Edition servers.
- `close/2`, transaction helpers, and transaction status helpers resolve
  runtime resources from the registered atom cluster name.
- `put_payload/4` is intended for callers that already have a
  complete Aerospike single-record write or delete frame. The client uses the
  key for routing and forwards the payload unchanged.

See the module docs and guides for command-specific options and return shapes.
See the [CHANGELOG](CHANGELOG.md) for release history.

## Local Development

Run commands from this repository directory:

```bash
mix test
mix validate
```

The repo also provides profile-aware Make targets:

```bash
make test PROFILE=unit
make test PROFILE=ce
make test PROFILE=cluster
make test PROFILE=enterprise
make coverage PROFILE=unit
make validate
```

Live integration profiles use this repository's `docker-compose.yml`:

```bash
make deps PROFILE=ce
make deps PROFILE=cluster
make deps PROFILE=enterprise
```

For package docs, ExDoc uses this README as the Overview page and groups the
Markdown files under `guides/` as task-focused guides.
