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
- **Single-record CRUD** — `put`, `get`, `delete`, `exists`, `touch` with bang variants
- **Operate** — atomic multi-operation per record (`add`, `append`, `prepend`, custom op lists)
- **Batch operations** — `batch_get`, `batch_exists`, `batch_operate` for multi-key round-trips
- **Scan & query** — full-table scans and secondary-index queries via `stream!`, `all`, `count`, `page`
- **CDT operations** — List, Map, Bit, and HLL operations with nested context (`Ctx`)
- **Server-side expressions** — filter results with `Aerospike.Exp` expressions
- **Secondary indexes** — `create_index` / `drop_index` with async `IndexTask` polling
- **UDF management** — `register_udf`, `remove_udf`, `apply_udf` for Lua user-defined functions
- **Transactions** — multi-record transactions (`transaction/2`, `commit/2`, `abort/2`) on Enterprise Edition
- **Write policies** — TTL, generation checks (CAS), create/update/replace semantics, durable delete
- **Read policies** — selective bin projection, header-only reads
- **Policy defaults** — set read/write timeouts and options once at connection time; override per call
- **TLS support** — optional TLS and mTLS for node connections
- **Telemetry** — emits `[:aerospike, :command, :start | :stop | :exception]` events
- **Pure Elixir** — only runtime dependencies are `nimble_options`, `nimble_pool`, and `telemetry`; crypto uses Erlang's `:crypto` (RIPEMD-160 digests)

See the [CHANGELOG](CHANGELOG.md) for what shipped in each version.

## Installation

Add `aerospike` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:aerospike, "~> 0.1.0"}
  ]
end
```

## Quick Start

```elixir
# Start the client (connects to a local Aerospike instance)
{:ok, _pid} = Aerospike.start_link(name: :aero, hosts: ["localhost:3000"])

# Build a key (namespace, set, user key)
key = Aerospike.key("test", "users", "user:1001")

# Write bins (columns) to the record
:ok = Aerospike.put!(:aero, key, %{"name" => "Ada", "lang" => "Elixir", "score" => 42})

# Read the record back
{:ok, record} = Aerospike.get(:aero, key)
record.bins["name"]
#=> "Ada"

# Clean up
:ok = Aerospike.delete!(:aero, key)
```

## Usage

### Starting the Client

The client starts a supervision tree that manages cluster discovery and connection pools.
Add it to your application supervisor or start it directly:

```elixir
# In your application supervisor
children = [
  {Aerospike,
   name: :aero,
   hosts: ["node1:3000", "node2:3000"],
   pool_size: 8,
   defaults: [
     write: [timeout: 2_000],
     read: [timeout: 1_500]
   ]}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

#### Connection Options

| Option                   | Type             | Default | Description                                        |
|--------------------------|------------------|---------|----------------------------------------------------|
| `:name`                  | atom             | —       | **Required.** Registered name for this connection.  |
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

### Writing Records

```elixir
key = Aerospike.key("test", "users", "user:42")

# Simple put — merges bins into the record
:ok = Aerospike.put!(:aero, key, %{"name" => "Grace", "age" => 36})

# Atom keys are accepted and normalized to strings
:ok = Aerospike.put!(:aero, key, %{name: "Grace", age: 36})

# Set a TTL of 1 hour (in seconds)
:ok = Aerospike.put!(:aero, key, %{"name" => "Grace"}, ttl: 3600)
```

### Reading Records

```elixir
# Read all bins
{:ok, record} = Aerospike.get(:aero, key)
record.bins       #=> %{"name" => "Grace", "age" => 36}
record.generation #=> 1
record.ttl        #=> server-reported TTL

# Read specific bins only
{:ok, record} = Aerospike.get(:aero, key, bins: ["name"])
record.bins #=> %{"name" => "Grace"}

# Read header only (generation + TTL, no bin data)
{:ok, record} = Aerospike.get(:aero, key, header_only: true)
record.bins #=> %{}
```

### Deleting Records

```elixir
# Returns whether the record existed before deletion
{:ok, true} = Aerospike.delete(:aero, key)

# Deleting a non-existent key is not an error
{:ok, false} = Aerospike.delete(:aero, key)
```

### Checking Existence

```elixir
{:ok, true} = Aerospike.exists(:aero, key)
{:ok, false} = Aerospike.exists(:aero, missing_key)
```

### Refreshing TTL (Touch)

```elixir
# Reset TTL to the namespace default
:ok = Aerospike.touch!(:aero, key)

# Set a specific TTL (10 minutes)
:ok = Aerospike.touch!(:aero, key, ttl: 600)
```

### Write Policies

Control write behavior with per-call options:

```elixir
# Create only — fails if the record already exists
:ok = Aerospike.put!(:aero, key, bins, exists: :create_only)

# Update only — fails if the record does not exist
:ok = Aerospike.put!(:aero, key, bins, exists: :update_only)

# Replace — like update, but wipes all existing bins first
:ok = Aerospike.put!(:aero, key, bins, exists: :replace_only)

# Create or replace — upsert that wipes old bins on update
:ok = Aerospike.put!(:aero, key, bins, exists: :create_or_replace)
```

### Optimistic Concurrency (CAS)

Use generation checks for compare-and-swap semantics:

```elixir
# Read current state
{:ok, record} = Aerospike.get(:aero, key)
gen = record.generation

# Write only if no one else has modified the record
case Aerospike.put(:aero, key, %{"counter" => 1},
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
digest_key = Aerospike.key_digest("test", "users", <<digest::binary-20>>)
{:ok, record} = Aerospike.get(:aero, digest_key)
```

## Error Handling

All operations return `{:ok, result}` or `{:error, %Aerospike.Error{}}`. Bang variants
(`put!`, `get!`, `delete!`, `exists!`, `touch!`) unwrap success and raise on error:

```elixir
# Pattern matching on errors
case Aerospike.get(:aero, key) do
  {:ok, record} ->
    process(record)

  {:error, %Aerospike.Error{code: :key_not_found}} ->
    create_default()

  {:error, %Aerospike.Error{code: :timeout}} ->
    retry_later()
end

# Bang variant raises Aerospike.Error
record = Aerospike.get!(:aero, key)
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

# All services — adds enterprise-only feature tests (durable delete, etc.)
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

## Disclaimer

This is an independent, community-developed project. It is **not** affiliated with,
endorsed by, or sponsored by Aerospike, Inc. "Aerospike" is a trademark of Aerospike, Inc.

## License

Copyright 2024–present Gabriel Roldan

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
