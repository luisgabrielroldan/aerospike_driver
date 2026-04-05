# Working with Operations

The `operate` command executes multiple bin operations on a single record in one
atomic round-trip. This is the foundation for working with Aerospike's Collection
Data Types (CDTs) — lists, maps, bitwise blobs, and HyperLogLog structures.

## The Operate Command

`Aerospike.operate/4` takes a list of operations and executes them atomically under
a record lock. Operations run in the order you specify.

```elixir
import Aerospike.Op

key = Aerospike.key("test", "users", "user:42")

{:ok, record} =
  Aerospike.operate(:aero, key, [
    put("status", "active"),
    add("login_count", 1),
    get("login_count"),
    get("status")
  ])

record.bins["login_count"]  # => incremented value
record.bins["status"]       # => "active"
```

## Primitive Operations

`Aerospike.Op` provides the basic bin operations:

| Function | Description |
|----------|-------------|
| `put(bin, value)` | Write a bin value |
| `get(bin)` | Read a bin value |
| `get_header()` | Read only record metadata (generation, TTL) |
| `add(bin, value)` | Increment/decrement a numeric bin |
| `append(bin, value)` | Append to a string bin |
| `prepend(bin, value)` | Prepend to a string bin |

```elixir
import Aerospike.Op

# Atomic counter increment + read
{:ok, rec} = Aerospike.operate(:aero, key, [
  add("views", 1),
  get("views")
])
```

## How Results Are Returned

The `operate` command groups results by bin name. This is important to understand:

- If a bin has **one operation** that returns a result, you get the value directly
- If a bin has **multiple operations** returning results, you get a list

```elixir
# Single read per bin — direct values
{:ok, rec} = Aerospike.operate(:aero, key, [
  get("name"),
  get("age")
])
rec.bins["name"]  # => "Ada"
rec.bins["age"]   # => 36

# Multiple results for same bin — becomes a list
{:ok, rec} = Aerospike.operate(:aero, key, [
  add("counter", 1),       # returns new value
  add("counter", 5),       # returns new value again
  get("counter")           # returns current value
], respond_per_each_op: true)
rec.bins["counter"]  # => [original+1, original+6, original+6]
```

Use the `respond_per_each_op: true` option when you need per-operation results
for the same bin.

## Collection Data Types

Aerospike bins can hold complex data structures manipulated server-side through
CDT operations. Each CDT type has its own operation module:

| Module | Data Type | Use Cases |
|--------|-----------|-----------|
| `Aerospike.Op.List` | Ordered/unordered lists | Queues, time series, tags, membership |
| `Aerospike.Op.Map` | Key-value maps | Documents, event stores, preferences |
| `Aerospike.Op.Bit` | Binary blobs | Flags, bitmasks, compact encodings |
| `Aerospike.Op.HLL` | HyperLogLog | Cardinality estimation, unique counts |

CDT operations are passed to `operate/4` just like primitive ops:

```elixir
alias Aerospike.Op.Map
alias Aerospike.Op.List

{:ok, rec} =
  Aerospike.operate(:aero, key, [
    Map.put("profile", "email", "ada@example.com"),
    List.append("roles", "admin"),
    List.size("roles")
  ])
```

## Mixing Read and Write Operations

A single `operate` call can mix reads and writes across different bins and data types.
All operations execute atomically:

```elixir
alias Aerospike.Op.Map
import Aerospike.Op

{:ok, rec} =
  Aerospike.operate(:aero, key, [
    put("updated_at", System.system_time(:second)),
    add("version", 1),
    Map.put("settings", "theme", "dark"),
    Map.get_by_key("settings", "lang"),
    get("version")
  ])
```

## Return Types

Many CDT operations accept a `return_type:` option that controls what the server
returns. Each module provides helper functions:

### List return types

| Helper | Returns |
|--------|---------|
| `List.return_none()` | Nothing (fastest for write-only ops) |
| `List.return_value()` | The value(s) |
| `List.return_count()` | Count of affected items |
| `List.return_index()` | Index position(s) |

### Map return types

| Helper | Returns |
|--------|---------|
| `Map.return_none()` | Nothing |
| `Map.return_key()` | Key(s) of affected entries |
| `Map.return_value()` | Value(s) of affected entries |
| `Map.return_key_value()` | Key/value pairs |

```elixir
alias Aerospike.Op.List

# Remove items but don't return them (faster)
Aerospike.operate(:aero, key, [
  List.remove_by_value("tags", "expired", return_type: List.return_none())
])

# Remove and return the count of removed items
Aerospike.operate(:aero, key, [
  List.remove_by_value("tags", "expired", return_type: List.return_count())
])
```

## Operate Options

| Option | Description |
|--------|-------------|
| `:timeout` | Per-call timeout in milliseconds |
| `:ttl` | Record TTL after the operation |
| `:generation` | Expected generation for optimistic locking |
| `:gen_policy` | `:none`, `:expect_gen_equal`, `:expect_gen_gt` |
| `:respond_per_each_op` | Return per-operation results (default: grouped by bin) |
| `:durable_delete` | Tombstone-based delete if the operation includes a delete |

## Next Steps

- [Map Patterns](map-patterns.md) — event containers, document stores, leaderboards
- [List Patterns](list-patterns.md) — queues, time series, bounded lists
- [Nested Operations](nested-operations.md) — operating on deeply nested structures
- `Aerospike.Op.Map` — complete map operation reference
- `Aerospike.Op.List` — complete list operation reference
- `Aerospike.Op.Bit` — bitwise operation reference
- `Aerospike.Op.HLL` — HyperLogLog operation reference
