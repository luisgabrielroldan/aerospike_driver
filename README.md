# Aerospike

[![Hex.pm](https://img.shields.io/hexpm/v/aerospike_driver.svg)](https://hex.pm/packages/aerospike_driver)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/aerospike_driver/)
[![License](https://img.shields.io/hexpm/l/aerospike_driver.svg)](https://github.com/luisgabrielroldan/aerospike_driver/blob/main/LICENSE)
[![Last Updated](https://img.shields.io/github/last-commit/luisgabrielroldan/aerospike_driver.svg)](https://github.com/luisgabrielroldan/aerospike_driver/commits/main)

An idiomatic Elixir client for the [Aerospike](https://aerospike.com/) database.
Connects directly over the Aerospike binary wire protocol — pure Elixir, no NIFs.

## Features

- **OTP-native** — starts under a supervisor, pools connections automatically via NimblePool
- **Cluster-aware** — discovers nodes, maintains partition maps, routes operations to the correct node
- **Repo wrapper** — recommended application-facing API via `use Aerospike.Repo`
- **Single-record CRUD** — `put`, `get`, `delete`, `exists`, `touch` with bang variants
- **Operate** — atomic multi-operation per record (`add`, `append`, `prepend`, custom op lists)
- **Batch operations** — `batch_get`, `batch_get_header`, `batch_get_operate`, `batch_exists`, `batch_delete`, `batch_udf`, and `batch_operate` for multi-key round-trips
- **Scan & query** — cluster-wide `stream!`, `all`, `count`, `page`, plus node-targeted `*_node` scan/query reads and explicit `query_stream`, `query_execute`, `query_udf`, and `query_aggregate` flows
- **CDT operations** — List, Map, Bit, HLL, and expression ops with nested context (`Ctx`)
- **Server-side expressions** — filter results with `Aerospike.Exp` expressions
- **Geospatial support** — typed geo helpers plus geo query filters
- **Secondary indexes** — `create_index` / `drop_index` with async `IndexTask` polling
- **UDF management** — `register_udf`, `remove_udf`, `apply_udf`, and `list_udfs` for Lua user-defined functions
- **Transactions** — multi-record transactions (`transaction/2`, `commit/2`, `abort/2`) on Enterprise Edition
- **Write policies** — TTL, generation checks (CAS), create/update/replace semantics, durable delete
- **Read policies** — selective bin projection, header-only reads
- **Policy defaults** — configure defaults for read, write, delete, exists, touch, operate, batch, scan, and query; override per call
- **Admin & info operations** — raw `info`, per-node `info_node`, node listing, and `truncate`
- **Security administration** — manage users, PKI users, roles, privileges, whitelists, and quotas on secured Enterprise clusters
- **Operational APIs** — client-managed metrics (`enable_metrics`, `disable_metrics`, `metrics_enabled?`), runtime stats snapshot (`stats`), connection-pool warmup (`warm_up`), and XDR filter management (`set_xdr_filter`)
- **TLS support** — optional TLS and mTLS for node connections
- **Circuit breaker** — per-node error-rate rejection with telemetry
- **Telemetry** — emits `[:aerospike, :command, :start | :stop | :exception]` events
- **Pure Elixir** — no NIFs; runtime deps stay small (`jason`, `nimble_options`, `nimble_pool`, `telemetry`) and crypto uses Erlang's `:crypto` (RIPEMD-160 digests)

See the [CHANGELOG](CHANGELOG.md) for what shipped in each version.

## Installation

Add `aerospike_driver` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:aerospike_driver, "~> 0.2.0"}
  ]
end
```

## Quick Start

```elixir
defmodule MyApp.Repo do
  use Aerospike.Repo,
    otp_app: :my_app,
    name: :aero
end

# config/runtime.exs
config :my_app, MyApp.Repo,
  hosts: ["localhost:3000"]

# Start under your application supervisor
children = [
  MyApp.Repo
]

# Build a key (namespace, set, user key)
key = MyApp.Repo.key("test", "users", "user:1001")

# Write bins (columns) to the record
:ok = MyApp.Repo.put!(key, %{"name" => "Ada", "lang" => "Elixir", "score" => 42})

# Read the record back
{:ok, record} = MyApp.Repo.get(key)
record.bins["name"]
#=> "Ada"

# Clean up
:ok = MyApp.Repo.delete!(key)
```

## Usage

### Recommended Application Setup

For application code, define a Repo module once and call `MyApp.Repo.*` for
day-to-day operations. The generated module starts the same Aerospike client
supervision tree, but binds one configured connection so the rest of the app
does not pass a connection handle around.

```elixir
defmodule MyApp.Repo do
  use Aerospike.Repo,
    otp_app: :my_app,
    name: :aero
end

# config/runtime.exs
config :my_app, MyApp.Repo,
  hosts: ["node1:3000", "node2:3000"],
  pool_size: 8,
  defaults: [
    write: [timeout: 2_000],
    read: [timeout: 1_500]
  ]

# In your application supervisor
children = [
  MyApp.Repo
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Use `MyApp.Repo.key/3` to build keys and `MyApp.Repo.put/2`, `get/1`, `delete/1`,
`exists/1`, and `touch/2` for the common single-record flow.

#### Repo Config Options

| Option                   | Type             | Default | Description                                        |
|--------------------------|------------------|---------|----------------------------------------------------|
| `:name`                  | atom             | repo module | Registered connection name. Generated repos usually set this in `use Aerospike.Repo`. |
| `:hosts`                 | list of strings  | —       | **Required.** Seed hosts (`"host:port"` or `"host"` for port 3000). |
| `:pool_size`             | pos_integer      | `10`    | Connections per discovered node.                    |
| `:pool_checkout_timeout` | non_neg_integer  | `5000`  | Pool checkout timeout in ms.                        |
| `:connect_timeout`       | non_neg_integer  | `5000`  | TCP connect timeout in ms.                          |
| `:tend_interval`         | non_neg_integer  | `1000`  | Cluster tend interval in ms.                        |
| `:recv_timeout`          | non_neg_integer  | `5000`  | Socket receive timeout in ms.                       |
| `:auth_opts`             | keyword list     | `[]`    | Optional internal authentication (user/credential). |
| `:tls`                   | boolean          | `false` | When `true`, upgrades TCP with TLS (`:ssl.connect/3`). |
| `:tls_opts`              | keyword list     | `[]`    | Options for `:ssl.connect/3` (certs, verify, SNI, etc.). |
| `:defaults`              | keyword list     | `[]`    | Per-command policy defaults (see below).            |

### Direct Connection API

`Aerospike.start_link/1` and direct `Aerospike.*` calls are still available when
you want explicit connection lifecycle control or need to work without a Repo
wrapper. That lower-level form remains the canonical facade API:

```elixir
{:ok, _pid} = Aerospike.start_link(name: :aero, hosts: ["localhost:3000"])

key = Aerospike.key("test", "users", "user:1001")
:ok = Aerospike.put!(:aero, key, %{"name" => "Ada"})
{:ok, record} = Aerospike.get(:aero, key)
```

### Advanced: raw payload writes

[`Aerospike.put_payload/4`](https://hexdocs.pm/aerospike_driver/Aerospike.html#put_payload/4) (and the matching `Repo.put_payload/3`) sends a **caller-built** single-record write or delete wire message. The client only routes the bytes and decodes the standard write response — it does not validate payload contents beyond `is_binary/1`, and it **does not** register keys with the transaction monitor even when `:txn` is passed (callers own any MRT fields inside the payload). This is an advanced escape hatch for replay, proxy, or tooling scenarios; see the [Raw payload writes](guides/raw-payload-write.md) guide and prefer normal `put/4` / `delete/3` for application code.

### Batch Convenience and Node-Targeted Reads

Phase 4 adds first-class wrappers for the common homogeneous batch cases and
for record-read scan/query execution against one resolved cluster node.

```elixir
import Aerospike.Op
alias Aerospike.{Query, Scan}

keys = [
  MyApp.Repo.key("test", "users", "user:1"),
  MyApp.Repo.key("test", "users", "user:2")
]

{:ok, headers} = MyApp.Repo.batch_get_header(keys)
{:ok, records} = MyApp.Repo.batch_get_operate(keys, [get("name"), get("score")])
{:ok, delete_results} = MyApp.Repo.batch_delete(keys)
{:ok, udf_results} = MyApp.Repo.batch_udf(keys, "pkg", "fn", [])

{:ok, [node_name | _]} = MyApp.Repo.node_names()

scan =
  Scan.new("test", "users")
  |> Scan.max_records(100)

{:ok, page} = MyApp.Repo.scan_page_node(node_name, scan)

query =
  Query.new("test", "users")
  |> Query.max_records(100)

{:ok, records} = MyApp.Repo.query_all_node(node_name, query)
```

Use the dedicated batch wrappers when one intent applies uniformly across all
keys. Use `batch_operate/2` plus `Aerospike.Batch.*` builders when per-key work
differs. Node-targeted APIs accept `node_name` values returned by
`node_names/0` or `nodes/0`; they narrow record-read execution to that node,
while `query_execute`, `query_udf`, and `query_aggregate` remain cluster-wide.

### Query UDFs and Aggregation

Phase 2 adds explicit query-wide execution APIs alongside the older generic
scan/query helpers:

```elixir
alias Aerospike.{Filter, Query}

query =
  Query.new("test", "users")
  |> Query.where(Filter.range("score", 10, 100))

{:ok, udfs} = MyApp.Repo.list_udfs()

{:ok, task} =
  MyApp.Repo.query_udf(
    query,
    "leaderboard",
    "mark_active",
    ["gold"]
  )

:ok = Aerospike.ExecuteTask.wait(task, timeout: 15_000)

values =
  MyApp.Repo.query_aggregate(query, "leaderboard", "sum_scores", ["score"])
  |> Enum.to_list()
```

`query_stream/3` yields `%Aerospike.Record{}` structs, `query_execute/4`,
`query_execute_node/5`, `query_udf/6`, and `query_udf_node/7` return
`%Aerospike.ExecuteTask{}` values for background work, and `query_aggregate/6`
yields one or more aggregate values from the server. The current client does
not perform a final client-side Lua reduction across multi-node aggregate
partials, so callers should reduce those values in Elixir when they need one
final answer.

See [Queries and Scanning](guides/queries-and-scanning.md) and
[User Defined Functions](guides/udfs.md) for complete examples.

### Security Administration

Security administration is available on secured Aerospike Enterprise clusters.
For application code, configure the Repo and use the generated
`MyApp.Repo.Admin` submodule:

```elixir
alias Aerospike.Admin.PasswordHash

defmodule MyApp.Repo do
  use Aerospike.Repo,
    otp_app: :my_app,
    name: :aero_admin
end

config :my_app, MyApp.Repo,
  hosts: ["127.0.0.1:3200"],
  auth_opts: [
    user: "admin",
    credential: PasswordHash.hash("admin")
  ]
```

Create and inspect a user:

```elixir
:ok = MyApp.Repo.Admin.create_user("ops-reader", "secret-pass", ["read"])
{:ok, %Aerospike.User{name: "ops-reader", roles: ["read"]}} =
  MyApp.Repo.Admin.query_user("ops-reader")
```

Create and manage a role with scoped privileges:

```elixir
alias Aerospike.Privilege

role_privileges = [
  %Privilege{code: :read, namespace: "test", set: "reports"}
]

:ok =
  MyApp.Repo.Admin.create_role(
    "report_reader",
    role_privileges,
    whitelist: ["10.0.0.0/24"],
    read_quota: 100
  )

:ok =
  MyApp.Repo.Admin.grant_privileges(
    "report_reader",
    [%Privilege{code: :read_write, namespace: "test", set: "scratch"}]
  )
```

The full surface includes `create_pki_user/4`, `drop_user/3`, `change_password/4`,
`grant_roles/4`, `revoke_roles/4`, `query_users/2`, `drop_role/3`,
`revoke_privileges/4`, `set_whitelist/4`, `set_quotas/5`, and `query_roles/2`.
After a successful self-password change, the running client rotates its
in-memory credential source for future reconnects, but a restarted client still
depends on the credentials supplied in `:auth_opts`.
See [Security Administration](guides/security-administration.md) for the complete flow
and test-environment requirements.

### Operational APIs

Phase 5 adds client-managed metrics, a runtime stats snapshot, connection-pool
warmup, and XDR filter management.

Metrics collection is opt-in and separate from the always-active `[:aerospike, :command]`
Telemetry events:

```elixir
:ok = MyApp.Repo.enable_metrics()
true = MyApp.Repo.metrics_enabled?()
stats = MyApp.Repo.stats()
# %{
#   metrics_enabled: true,
#   cluster_ready: true,
#   nodes_total: 3,
#   nodes_active: 3,
#   open_connections: 30,
#   commands_total: 500,
#   commands_ok: 495,
#   commands_error: 5,
#   cluster: %{...},
#   nodes: %{"BB90000000A4202" => %{...}, ...}
# }
:ok = MyApp.Repo.disable_metrics()
```

Warmup exercises the current discovered node pools to reduce first-request latency:

```elixir
{:ok, result} = MyApp.Repo.warm_up()
# %{status: :ok, total_requested: 30, total_warmed: 30, nodes_ok: 3, ...}

{:ok, result} = MyApp.Repo.warm_up(count: 5)
```

XDR filter management requires Aerospike Enterprise Edition with XDR configured.
Pass `nil` to clear the current server-side filter:

```elixir
import Aerospike.Exp

filter = and_(eq(int_bin("status"), int_val(1)), gt(int_bin("score"), int_val(50)))

:ok = MyApp.Repo.set_xdr_filter("dc-west", "test", filter)
:ok = MyApp.Repo.set_xdr_filter("dc-west", "test", nil)
```

See [Observability and Runtime Operations](guides/observability.md) for the full API
reference, stats shape, warmup semantics, and XDR coverage notes.

### Writing Records

```elixir
key = MyApp.Repo.key("test", "users", "user:42")

# Simple put — merges bins into the record
:ok = MyApp.Repo.put!(key, %{"name" => "Grace", "age" => 36})

# Atom keys are accepted and normalized to strings
:ok = MyApp.Repo.put!(key, %{name: "Grace", age: 36})

# Set a TTL of 1 hour (in seconds)
:ok = MyApp.Repo.put!(key, %{"name" => "Grace"}, ttl: 3600)
```

### Reading Records

```elixir
# Read all bins
{:ok, record} = MyApp.Repo.get(key)
record.bins       #=> %{"name" => "Grace", "age" => 36}
record.generation #=> 1
record.ttl        #=> server-reported TTL

# Read specific bins only
{:ok, record} = MyApp.Repo.get(key, bins: ["name"])
record.bins #=> %{"name" => "Grace"}

# Read header only (generation + TTL, no bin data)
{:ok, record} = MyApp.Repo.get(key, header_only: true)
record.bins #=> %{}
```

### Deleting Records

```elixir
# Returns whether the record existed before deletion
{:ok, true} = MyApp.Repo.delete(key)

# Deleting a non-existent key is not an error
{:ok, false} = MyApp.Repo.delete(key)
```

### Checking Existence

```elixir
{:ok, true} = MyApp.Repo.exists(key)
{:ok, false} = MyApp.Repo.exists(missing_key)
```

### Refreshing TTL (Touch)

```elixir
# Reset TTL to the namespace default
:ok = MyApp.Repo.touch!(key)

# Set a specific TTL (10 minutes)
:ok = MyApp.Repo.touch!(key, ttl: 600)
```

### Write Policies

Control write behavior with per-call options:

```elixir
# Create only — fails if the record already exists
:ok = MyApp.Repo.put!(key, bins, exists: :create_only)

# Update only — fails if the record does not exist
:ok = MyApp.Repo.put!(key, bins, exists: :update_only)

# Replace — like update, but wipes all existing bins first
:ok = MyApp.Repo.put!(key, bins, exists: :replace_only)

# Create or replace — upsert that wipes old bins on update
:ok = MyApp.Repo.put!(key, bins, exists: :create_or_replace)
```

### Optimistic Concurrency (CAS)

Use generation checks for compare-and-swap semantics:

```elixir
# Read current state
{:ok, record} = MyApp.Repo.get(key)
gen = record.generation

# Write only if no one else has modified the record
case MyApp.Repo.put(key, %{"counter" => 1},
       generation: gen,
       gen_policy: :expect_gen_equal) do
  :ok -> :updated
  {:error, %Aerospike.Error{code: :generation_error}} -> :conflict
end
```

### Integer and String Keys

User keys can be strings or 64-bit integers:

```elixir
string_key = Aerospike.key("test", "users", "user:alice")
integer_key = Aerospike.key("test", "counters", 12345)
```

### Digest-Only Keys

If you already have the 20-byte RIPEMD-160 digest (e.g., from a secondary index or
another client), construct a key directly:

```elixir
digest_key = MyApp.Repo.key_digest("test", "users", <<digest::binary-20>>)
{:ok, record} = MyApp.Repo.get(digest_key)
```

## Error Handling

All operations return `{:ok, result}` or `{:error, %Aerospike.Error{}}`. Bang variants
on both `MyApp.Repo` and `Aerospike` unwrap success and raise on error:

```elixir
# Pattern matching on errors
case MyApp.Repo.get(key) do
  {:ok, record} ->
    process(record)

  {:error, %Aerospike.Error{code: :key_not_found}} ->
    create_default()

  {:error, %Aerospike.Error{code: :timeout}} ->
    retry_later()
end

# Bang variant raises Aerospike.Error
record = MyApp.Repo.get!(key)
```

Error codes are atoms for pattern matching: `:key_not_found`, `:key_exists`,
`:generation_error`, `:timeout`, `:parameter_error`, etc.

## Data Representation

Aerospike data types map to Elixir as follows:

| Aerospike Type | Elixir Type          | Notes                                   |
|----------------|----------------------|-----------------------------------------|
| Integer        | `integer()`          | 64-bit signed                           |
| Double         | `float()`            |                                         |
| String         | `String.t()`         | UTF-8 binary                            |
| Bytes          | `binary()`           | Raw bytes (blob particle type)          |
| Boolean        | `boolean()`          |                                         |
| Nil            | `nil`                | Server returns nil for absent bins      |
| List           | `list()`             | Ordered list (MessagePack encoded)      |
| Map            | `map()`              | Unordered map (MessagePack encoded)     |

Bin names are always returned as strings, even if you write them with atom keys.

## Telemetry

The client emits telemetry events for every command:

| Event                              | Measurements        | Metadata                                |
|------------------------------------|---------------------|-----------------------------------------|
| `[:aerospike, :command, :start]`   | `system_time`       | `namespace`, `command`, `node`          |
| `[:aerospike, :command, :stop]`    | `duration`          | `namespace`, `command`, `node`, `result`|
| `[:aerospike, :command, :exception]`| `duration`         | `namespace`, `command`, `kind`, `reason`|

```elixir
:telemetry.attach("my-handler", [:aerospike, :command, :stop], fn
  _event, %{duration: d}, %{command: cmd, result: result}, _config ->
    Logger.info("#{cmd} completed in #{System.convert_time_unit(d, :native, :millisecond)}ms: #{inspect(result)}")
end, nil)
```

## Testing

From the `aerospike_driver` directory:

```bash
mix deps.get
```

### Docker Setup

Tests require an Aerospike server running via Docker Compose. There are three profiles
depending on which tests you want to run:

```bash
# Single node — enough for unit, property, and basic integration tests
docker compose up -d

# 3-node cluster — adds multi-node, partition routing, and peer discovery tests
docker compose --profile cluster up -d

# All services — adds enterprise-only feature tests (durable delete, security admin, etc.)
docker compose --profile cluster --profile enterprise up -d
```

### Running Tests

By default, only unit tests and doctests run. Other test categories are opt-in:

```bash
# Unit tests only (fast, no external dependencies)
mix test

# Include property-based tests
mix test --include property

# Include integration tests (needs single Aerospike node)
mix test --include integration

# Include multi-node cluster tests (needs --profile cluster)
mix test --include cluster

# Include enterprise feature tests (needs --profile enterprise)
mix test --include enterprise

# Full suite (needs all Docker profiles running)
mix test.all
```

### TLS Fixture Generation

TLS unit tests can run with generated certificate fixtures (CA/server/client) instead
of ephemeral in-memory certs:

```bash
# Generate fixture certs/keys under test/support/fixtures/tls
make tls-fixtures

# Run only TLS connection tests
make test.tls
```

The TLS test suite automatically uses these fixtures when present and falls back to
ephemeral certs when missing.

### Coverage

Coverage runs integration tests via Mix aliases:

```bash
mix coveralls          # terminal summary
mix test.coverage      # HTML report in cover/
```

### Quality Checks

```bash
mix format --check-formatted
mix credo --strict
mix dialyzer
```

## Benchmarks

The repository includes a Benchee-based benchmark suite under `bench/` with one baseline
case per benchmark layer:

- `bench/tests/micro/key_construction_bench.exs` (L1)
- `bench/tests/e2e/crud_baseline_bench.exs` (L2)
- `bench/tests/workload/ru_80_20_bench.exs` (L3)
- `bench/tests/fanout/batch_get_bench.exs` (L4)

### Prerequisites

- Local Aerospike server reachable at `127.0.0.1:3000` by default.
- Default benchmark namespace/set assumptions:
  - namespace: `test`
  - sets: `bench_e2e`, `bench_workload`, `bench_fanout`
- Optional overrides:
  - `AEROSPIKE_HOST`, `AEROSPIKE_PORT`
  - `BENCH_NAMESPACE`, `BENCH_SET`

### Run Modes

All commands below are run from `aerospike_driver/`:

```bash
# Quick smoke run (short duration, low concurrency)
mix bench --quick

# Default baseline run
mix bench

# Extended run (longer duration, wider concurrency)
mix bench --full
```

If you are new to the suite, start with a single L2 run:

```bash
mix bench --quick bench/tests/e2e/crud_baseline_bench.exs
```

Run a single benchmark file when iterating on one layer:

```bash
mix bench bench/tests/micro/key_construction_bench.exs
mix bench bench/tests/e2e/crud_baseline_bench.exs
mix bench bench/tests/workload/ru_80_20_bench.exs
mix bench bench/tests/fanout/batch_get_bench.exs
```

Clean benchmark result directories:

```bash
mix bench.clean
mix bench.clean --all
```

### Artifacts and Comparison

Each benchmark run writes machine-readable JSON artifacts under `bench/results/<run-id>/`.
You can set `BENCH_RUN_ID` to group before/after runs explicitly.

```bash
BENCH_RUN_ID=baseline mix bench
BENCH_RUN_ID=candidate mix bench
```

Compare result directories by scenario IDs and key statistics (`average`, `ips`, `std_dev_ratio`)
inside the JSON reports. For reliable comparisons, run the same profile multiple times and
treat small deltas as noise unless the trend is consistent. Compare like-for-like scenarios only
(same benchmark title and `scenario_id`), not cross-scenario pairs such as `put` vs `get`.

For full benchmark setup and environment controls, see `bench/README.md`.
`BENCH_PROFILE` remains supported for CI/scripts, but local usage can prefer `--quick`, `--default`, or `--full`.

## Disclaimer

This is an independent, community-developed project. It is **not** affiliated with,
endorsed by, or sponsored by Aerospike, Inc. "Aerospike" is a trademark of Aerospike, Inc.

## License

Copyright 2024–present Gabriel Roldan

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
