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

Expression builders currently cover the shipped `%Aerospike.Exp{}` surface.
Broader expression-builder families remain outside this release surface.

## Raw Payload Writes

`put_payload/4` is for callers that already have a complete Aerospike
single-record write or delete frame.

```elixir
:ok = Aerospike.put_payload(:aerospike, key, payload)
```

The client uses the key only for partition routing. It does not rewrite TTL,
generation, delete flags, filters, or transaction fields inside the payload.
Most callers should prefer `put/4`, `delete/3`, or `operate/4`.
