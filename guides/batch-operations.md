# Batch Operations

Batch helpers execute multi-key work while preserving input order in the
returned results. Parent batch policy options live on the facade call, while
entry-level read/write options can be attached to `Aerospike.Batch` builders
when the batch index protocol can encode the field.

## Batch Reads

`batch_get/4` accepts `%Aerospike.Key{}` values and key tuples. Missing records
are returned as per-key errors, not as `nil`.

```elixir
keys = [
  Aerospike.key("test", "users", "user:1"),
  {"test", "users", "user:2"},
  Aerospike.key("test", "users", "missing")
]

{:ok, results} =
  Aerospike.batch_get(:aerospike, keys, :all,
    timeout: 5_000,
    socket_timeout: 1_000,
    max_concurrent_nodes: 2,
    read_mode_ap: :one
  )

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

{:ok, results} =
  Aerospike.batch_operate(:aerospike, entries,
    timeout: 5_000,
    allow_partial_results: true
  )
```

Parent batch options include `:timeout`, `:socket_timeout`,
`:max_concurrent_nodes`, `:allow_partial_results`, `:respond_all_keys`,
`:allow_inline`, and `:allow_inline_ssd`. Batch read helpers also accept
encodable read fields such as `:filter`, `:read_mode_ap`, `:read_mode_sc`, and
`:read_touch_ttl_percent`.

Per-entry read options on `Aerospike.Batch.read/2` are limited to `:filter`,
`:read_mode_ap`, `:read_mode_sc`, and `:read_touch_ttl_percent`. Per-entry
write options on put/delete/UDF entries include `:ttl`, `:generation`,
`:generation_policy`, `:exists`, `:commit_level`, `:durable_delete`,
`:respond_per_op`, `:send_key`, `:read_mode_ap`, `:read_mode_sc`,
`:read_touch_ttl_percent`, and `:filter`.
