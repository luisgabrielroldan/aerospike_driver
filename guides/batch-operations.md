# Batch Operations

Batch helpers execute multi-key work while preserving input order in the
returned results. The current public batch option surface is intentionally
narrow: helpers accept only the batch-level `:timeout` option.

## Batch Reads

`batch_get/4` accepts `%Aerospike.Key{}` values and key tuples. Missing records
are returned as per-key errors, not as `nil`.

```elixir
keys = [
  Aerospike.key("test", "users", "user:1"),
  {"test", "users", "user:2"},
  Aerospike.key("test", "users", "missing")
]

{:ok, results} = Aerospike.batch_get(:aerospike, keys, :all, timeout: 5_000)

Enum.each(results, fn
  {:ok, record} ->
    record.bins

  {:error, %Aerospike.Error{code: :key_not_found}} ->
    :missing
end)
```

Pass `:all` to read complete records, or pass a non-empty list of string or
atom bin names to project only those bins:

```elixir
{:ok, projected} = Aerospike.batch_get(:aerospike, keys, ["name", :score])
```

Use `batch_get_header/3` when only metadata is needed:

```elixir
{:ok, headers} = Aerospike.batch_get_header(:aerospike, keys)
```

Check existence across keys with `batch_exists/3`:

```elixir
{:ok, existence} = Aerospike.batch_exists(:aerospike, keys)
```

## Batch Read Operations

`batch_get_operate/4` runs read-only operations against every key.

```elixir
ops = [
  Aerospike.Op.get("name"),
  Aerospike.Op.get("score")
]

{:ok, results} = Aerospike.batch_get_operate(:aerospike, keys, ops)
```

Write operations are rejected by this helper. Use heterogeneous batch entries
when batch work needs writes.

## Batch Writes And UDFs

Helpers that can write or run record UDFs return `%Aerospike.BatchResult{}`
entries. Each result contains the key, status, returned record data when
present, error reason, and whether a write outcome is in doubt.

```elixir
{:ok, delete_results} = Aerospike.batch_delete(:aerospike, keys)

{:ok, udf_results} =
  Aerospike.batch_udf(:aerospike, keys, "records", "mark_seen", [])
```

## Heterogeneous Batch Requests

Use `Aerospike.Batch` constructors for curated mixed work:

```elixir
entries = [
  Aerospike.Batch.read(Aerospike.key("test", "users", "user:1")),
  Aerospike.Batch.put(
    Aerospike.key("test", "users", "user:2"),
    %{"score" => 20}
  ),
  Aerospike.Batch.operate(
    Aerospike.key("test", "users", "user:3"),
    [Aerospike.Op.add("score", 1)]
  ),
  Aerospike.Batch.delete(Aerospike.key("test", "users", "old-user")),
  Aerospike.Batch.udf(
    Aerospike.key("test", "users", "user:4"),
    "records",
    "mark_seen",
    []
  )
]

{:ok, results} = Aerospike.batch_operate(:aerospike, entries, timeout: 5_000)
```

`batch_operate/3` does not expose per-entry write policies or public batch
retry options in the current release surface.
