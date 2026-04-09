# Getting Started

This guide walks you through connecting to Aerospike, performing basic CRUD operations,
and understanding key concepts in the Elixir client.

## Prerequisites

- An Aerospike server running on `localhost:3000` (see [Docker setup](#docker-setup) below)
- Elixir 1.15+

## Installation

Add `aerospike_driver` to your `mix.exs` dependencies:

```elixir
def deps do
  [
    {:aerospike_driver, "~> 0.1.0"}
  ]
end
```

## Docker Setup

The quickest way to get a local Aerospike instance:

```bash
docker run -d --name aerospike -p 3000:3000 aerospike/aerospike-server
```

## Connecting

Start a named client supervision tree with [`Aerospike.start_link/1`](Aerospike.html#start_link/1). The `:name` atom
becomes your connection handle for all subsequent calls:

```elixir
{:ok, _pid} =
  Aerospike.start_link(
    name: :aero,
    hosts: ["127.0.0.1:3000"],
    pool_size: 4
  )
```

In a supervised application, add it to your supervision tree:

```elixir
children = [
  {Aerospike,
   name: :aero,
   hosts: ["127.0.0.1:3000"],
   pool_size: 4}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

## Keys and Records

Every record in Aerospike is identified by a **key** composed of three parts:

- **Namespace** — the top-level data container (like a database)
- **Set** — a grouping within the namespace (like a table)
- **User key** — a string or integer that uniquely identifies the record

Build a key with [`Aerospike.key/3`](Aerospike.html#key/3):

```elixir
key = Aerospike.key("test", "users", "user:42")
```

The client computes a 20-byte RIPEMD-160 **digest** from these components. The server
uses this digest for record lookup — the user key itself is not stored unless you
pass `send_key: true`.

## Writing Records

Records contain named **bins** (like columns). Pass a map of bin names to values:

```elixir
:ok = Aerospike.put(:aero, key, %{
  "name" => "Ada Lovelace",
  "age" => 36,
  "active" => true
})
```

Aerospike bins support integers, floats, strings, booleans, lists, maps, and binary blobs.

## Reading Records

```elixir
{:ok, record} = Aerospike.get(:aero, key)
record.bins["name"]    # => "Ada Lovelace"
record.generation      # => 1 (increments on each write)
record.ttl             # => time-to-live in seconds
```

## Checking Existence

```elixir
{:ok, true} = Aerospike.exists(:aero, key)
```

## Deleting Records

```elixir
{:ok, true} = Aerospike.delete(:aero, key)
```

Returns `{:ok, true}` if a record was removed, `{:ok, false}` if the key was already absent.

## Refreshing TTL

Touch a record to reset its time-to-live without modifying bins:

```elixir
:ok = Aerospike.touch(:aero, key, ttl: 3600)
```

## Error Handling

All functions return `{:ok, result}` or `{:error, %Aerospike.Error{}}` (see [`Aerospike.Error`](Aerospike.Error.html)).
Bang variants ([`put!/4`](Aerospike.html#put!/4), [`get!/3`](Aerospike.html#get!/3), etc.) unwrap the success or raise:

```elixir
# Pattern matching
case Aerospike.get(:aero, key) do
  {:ok, record} -> process(record)
  {:error, %Aerospike.Error{code: :key_not_found}} -> handle_missing()
  {:error, error} -> handle_error(error)
end

# Bang variant — raises on error
record = Aerospike.get!(:aero, key)
```

Error codes are atoms: `:key_not_found`, `:timeout`, `:generation_mismatch`, etc.

## Policy Options

Every CRUD function accepts keyword options to control behavior per call:

```elixir
# Write with a 2-second timeout and generation check
:ok = Aerospike.put(:aero, key, bins,
  timeout: 2_000,
  generation: 3,
  gen_policy: :expect_gen_equal
)

# Read from a replica node
{:ok, record} = Aerospike.get(:aero, key, replica: :any)
```

Common options:

| Option | Description |
|--------|-------------|
| `:timeout` | Per-call timeout in milliseconds |
| `:ttl` | Record time-to-live in seconds (`-1` = don't change, `0` = namespace default) |
| `:generation` | Expected generation for optimistic locking |
| `:gen_policy` | `:none`, `:expect_gen_equal`, `:expect_gen_gt` |
| `:exists` | `:create_only`, `:update_only`, `:replace_only` |
| `:send_key` | `true` to store the user key on the server |
| `:replica` | `:master`, `:any` |
| `:durable_delete` | `true` for tombstone-based delete |

## Shutting Down

```elixir
:ok = Aerospike.close(:aero)
```

## Next Steps

- [Working with Operations](operate-and-cdt.md) — atomic multi-op and CDT operations
- [Batch Operations](batch-operations.md) — multi-key reads, writes, and mixed ops in one round-trip
- [`Aerospike.Op.Map`](Aerospike.Op.Map.html) — full map operation reference
- [`Aerospike.Op.List`](Aerospike.Op.List.html) — full list operation reference
