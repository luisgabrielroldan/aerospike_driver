# Record Operations

The `Aerospike` facade is the supported entry point for single-record work.
Helpers return `{:ok, result}` or `{:error, %Aerospike.Error{}}`; bang helpers
exist only where the API documents them.

## Keys

An Aerospike key is a namespace, set, and user key. The client computes the
digest used for routing.

```elixir
key = Aerospike.key("test", "profiles", "user:42")
```

If the digest is already known, use `Aerospike.key_digest/3`:

```elixir
key = Aerospike.key_digest("test", "profiles", <<0::160>>)
```

## Create Or Replace Bins

`put/4` writes the supplied bins and returns record metadata.

```elixir
{:ok, metadata} =
  Aerospike.put(:aerospike, key, %{
    "name" => "Grace",
    "score" => 10,
    "active" => true,
    "tags" => ["admin", "mentor"],
    "profile" => %{"city" => "Arlington"}
  })

metadata.generation
```

Read the complete record or select bins explicitly:

```elixir
{:ok, record} = Aerospike.get(:aerospike, key)
{:ok, projected} = Aerospike.get(:aerospike, key, ["name", "score"])
```

Check existence or metadata without fetching bin data:

```elixir
{:ok, true} = Aerospike.exists(:aerospike, key)
{:ok, header} = Aerospike.get_header(:aerospike, key)
```

## Write Helpers

The unary write helpers map to server-side write operations for common bin
updates:

```elixir
{:ok, _metadata} = Aerospike.add(:aerospike, key, %{"score" => 5})
{:ok, _metadata} = Aerospike.append(:aerospike, key, %{"name" => " Hopper"})
{:ok, _metadata} = Aerospike.prepend(:aerospike, key, %{"name" => "Dr. "})
{:ok, _metadata} = Aerospike.touch(:aerospike, key)
```

Delete returns whether the server deleted an existing record:

```elixir
{:ok, deleted?} = Aerospike.delete(:aerospike, key)
```

Use `:exists` when a write should require or replace an existing record in a
specific way:

```elixir
{:ok, _metadata} =
  Aerospike.put(:aerospike, key, %{"score" => 11}, exists: :replace_only)

{:ok, _metadata} =
  Aerospike.put(:aerospike, new_key, %{"score" => 1}, exists: :create_only)
```

Deletes and write-bearing operation lists also accept `durable_delete: true`
when the target namespace and server support tombstones.

## Command Options

The public option surface uses keyword opts at facade calls. Use the documented
options on each function rather than passing policy names that are not part of
this client's public API.

Common read options:

```elixir
{:ok, record} =
Aerospike.get(:aerospike, key, :all,
    timeout: 2_000,
    socket_timeout: 500,
    read_mode_ap: :one,
    read_mode_sc: :session,
    read_touch_ttl_percent: 0,
    filter:
      Aerospike.Exp.eq(
        Aerospike.Exp.str_bin("status"),
        Aerospike.Exp.str("active")
      )
  )
```

`:timeout` is the total command budget in milliseconds. `:socket_timeout` is
the per-attempt socket idle deadline, capped by the remaining total budget.
Read consistency fields are `:read_mode_ap` (`:one` or `:all`) and
`:read_mode_sc` (`:session`, `:linearize`, `:allow_replica`, or
`:allow_unavailable`). `:read_touch_ttl_percent` controls server read-touch
behavior, and `:filter` is a non-empty `%Aerospike.Exp{}` used as a
server-side expression filter.

Common write options:

```elixir
{:ok, metadata} =
  Aerospike.put(:aerospike, key, %{"score" => 12},
    timeout: 2_000,
    ttl: 3_600,
    generation: previous_generation,
    generation_policy: :expect_equal,
    exists: :update_only,
    commit_level: :all,
    respond_per_op: false,
    send_key: true,
    durable_delete: false
  )
```

`:ttl` is a server TTL in seconds. It also accepts `:default`,
`:never_expire`, and `:dont_update`. `:generation_policy` is `:none`,
`:expect_equal`, or `:expect_gt`; when omitted, a non-zero `:generation`
preserves the existing equality check behavior. `:exists` is one of `:update`,
`:update_only`, `:create_or_replace`, `:replace_only`, or `:create_only`.
`:commit_level` is `:all` or `:master`. `:durable_delete` asks compatible
namespaces to retain a tombstone for delete-shaped writes.

Transaction-aware write helpers also accept `txn: txn` when used inside the
transaction API:

```elixir
Aerospike.transaction(:aerospike, fn txn ->
  Aerospike.put(:aerospike, key, %{"status" => "locked"}, txn: txn)
end)
```

Admin/info helpers use `:pool_checkout_timeout` because they are one-node pool
operations:

```elixir
{:ok, stats} =
  Aerospike.info(:aerospike, "statistics", pool_checkout_timeout: 1_000)
```

Batch helpers expose parent dispatch options such as `:timeout`,
`:socket_timeout`, `:max_concurrent_nodes`, and `:allow_partial_results`.
`Aerospike.Batch` entries can also carry encodable per-entry read/write policy
options.

## Operation Lists

Use `operate/4` when multiple server-side operations should run against the
same record in one command. Primitive operations live in `Aerospike.Op`.

```elixir
ops = [
  Aerospike.Op.add("score", 1),
  Aerospike.Op.get("score"),
  Aerospike.Op.get("name")
]

{:ok, record} = Aerospike.operate(:aerospike, key, ops)
record.bins["score"]
```

Operation results are accumulated into `%Aerospike.Record{bins: map}` by bin
name. When several read operations target the same bin, the final returned bin
value follows the server response order.

## Expression Filters

Unary and write-family commands accept a server-side expression filter with
`:filter`.

```elixir
adult_filter =
  Aerospike.Exp.gte(
    Aerospike.Exp.int_bin("age"),
    Aerospike.Exp.int(18)
  )

{:ok, _metadata} =
  Aerospike.put(:aerospike, key, %{"verified" => true}, filter: adult_filter)
```

Expression builders include scalar, metadata, comparison, logical, arithmetic,
regex, geo, conditional, variable, CDT, bit, and HyperLogLog helper families.

## Raw Payload Writes

`put_payload/4` is for callers that already have a complete Aerospike
single-record write or delete frame.

```elixir
:ok = Aerospike.put_payload(:aerospike, key, payload)
```

The client uses the key only for partition routing. It does not rewrite TTL,
generation, delete flags, filters, or transaction fields inside the payload.
Most callers should prefer `put/4`, `delete/3`, or `operate/4`.
