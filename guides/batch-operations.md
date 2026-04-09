# Batch Operations

Batch APIs pack many keys into one request per server node. The client groups keys by partition owner, encodes a single wire message per node, and merges results back into **the same order as your input list**.

## Homogeneous reads: [`batch_get/3`](Aerospike.html#batch_get/3) and [`batch_exists/3`](Aerospike.html#batch_exists/3)

Use [`Aerospike.batch_get/3`](Aerospike.html#batch_get/3) when every operation is a read with the same options (namespace/set can differ per key; routing is per key digest).

- Missing keys appear as `nil` in the result list.
- Pass read options such as `:bins` or `:header_only` alongside batch options (`:timeout`, `:replica`, etc.). Batch options are validated separately from read options.

```elixir
keys = Enum.map(1..100, &Aerospike.key("test", "users", "user:#{&1}"))

{:ok, records} =
  Aerospike.batch_get(:aero, keys, timeout: 500, bins: ["name", "score"])

# records[i] corresponds to keys[i]; missing keys are nil
Enum.zip(keys, records)
|> Enum.reject(fn {_k, r} -> is_nil(r) end)
|> Enum.each(fn {_k, r} -> IO.inspect(r.bins) end)
```

### Header-only reads

When you only need generation and TTL (no bin data):

```elixir
{:ok, records} = Aerospike.batch_get(:aero, keys, header_only: true)

Enum.each(records, fn
  nil -> :skip
  rec -> IO.puts("gen=#{rec.generation} ttl=#{rec.ttl}")
end)
```

### [`batch_exists/3`](Aerospike.html#batch_exists/3)

[`Aerospike.batch_exists/3`](Aerospike.html#batch_exists/3) returns a list of booleans aligned with `keys`:

```elixir
keys = [key1, key2, key3]
{:ok, exists?} = Aerospike.batch_exists(:aero, keys)
# exists?: [true, false, true]

# Filter to only existing keys
existing_keys =
  Enum.zip(keys, exists?)
  |> Enum.filter(fn {_k, e} -> e end)
  |> Enum.map(fn {k, _} -> k end)
```

## Heterogeneous ops: [`batch_operate/3`](Aerospike.html#batch_operate/3)

Use [`Aerospike.batch_operate/3`](Aerospike.html#batch_operate/3) when each key can have a different operation (read, put, delete, [`operate/4`](Aerospike.html#operate/4), UDF). Build operations with [`Aerospike.Batch`](Aerospike.Batch.html):

```elixir
alias Aerospike.Batch

{:ok, results} =
  Aerospike.batch_operate(:aero, [
    Batch.read(key1, bins: ["a"]),
    Batch.put(key2, %{"x" => 1}),
    Batch.delete(key3),
    Batch.operate(key4, [Aerospike.Op.add("c", 1)])
  ])
```

Each element of `results` is an [`Aerospike.BatchResult`](Aerospike.BatchResult.html):

- On success, `status` is `:ok`. For reads, `record` may hold bins; for puts/deletes, `record` is usually `nil`.
- On failure, `status` is `:error` and `error` is set (`in_doubt` reflects uncertain writes).

For a missing key, [`Batch.read/2`](Aerospike.Batch.html#read/2) still yields `status: :ok` with `record: nil` (similar to [`batch_get/3`](Aerospike.html#batch_get/3)), so a mixed batch is not aborted when some keys are absent.

### Atomic operations in a batch

[`Batch.operate/3`](Aerospike.Batch.html#operate/3) takes the same operation list as [`Aerospike.operate/4`](Aerospike.html#operate/4) — useful for
atomic increments, list/map CDT ops, or mixed read+write on a single key within a batch:

```elixir
import Aerospike.Op

{:ok, [result]} =
  Aerospike.batch_operate(:aero, [
    Batch.operate(counter_key, [add("hits", 1), get("hits")])
  ])

result.record.bins["hits"]  # updated count
```

### UDF invocations in a batch

[`Batch.udf/5`](Aerospike.Batch.html#udf/5) calls a server-side Lua function on one key. Combine it with other
operations in the same batch:

```elixir
{:ok, results} =
  Aerospike.batch_operate(:aero, [
    Batch.udf(key1, "mymodule", "transform", [1, "x"]),
    Batch.read(key2, bins: ["result"]),
    Batch.udf(key3, "aggregate", "sum", ["score"])
  ])
```

> #### Note {: .info}
>
> Ensure the UDF module is registered on the server before calling [`Batch.udf/5`](Aerospike.Batch.html#udf/5).
> UDF registration is covered in a later phase.

## Pattern matching on [`BatchResult`](Aerospike.BatchResult.html)

[`Aerospike.BatchResult`](Aerospike.BatchResult.html) encodes both success and failure per key. Pattern match on
`status` to branch your logic:

```elixir
alias Aerospike.BatchResult

{:ok, results} = Aerospike.batch_operate(:aero, ops)

Enum.each(results, fn
  %BatchResult{status: :ok, record: %Aerospike.Record{} = rec} ->
    IO.inspect(rec.bins, label: "read success")

  %BatchResult{status: :ok, record: nil} ->
    IO.puts("write/delete succeeded")

  %BatchResult{status: :error, error: err, in_doubt: true} ->
    Logger.warning("ambiguous write: #{err.message}")

  %BatchResult{status: :error, error: err} ->
    Logger.error("key failed: #{err.code} — #{err.message}")
end)
```

### Counting successes and failures

```elixir
{ok, errors} = Enum.split_with(results, &(&1.status == :ok))
IO.puts("#{length(ok)} succeeded, #{length(errors)} failed")
```

## [`batch_get/3`](Aerospike.html#batch_get/3) vs [`batch_operate/3`](Aerospike.html#batch_operate/3)

| Use case | API |
|----------|-----|
| Many reads, same policy | [`batch_get/3`](Aerospike.html#batch_get/3) |
| Existence checks | [`batch_exists/3`](Aerospike.html#batch_exists/3) |
| Mix of reads, writes, deletes, CDT ops | [`batch_operate/3`](Aerospike.html#batch_operate/3) |
| Per-key errors as [`BatchResult`](Aerospike.BatchResult.html) | [`batch_operate/3`](Aerospike.html#batch_operate/3) with [`Batch.read/2`](Aerospike.Batch.html#read/2) |

**Rule of thumb**: if every operation is a plain read, prefer [`batch_get/3`](Aerospike.html#batch_get/3) — it returns
`[Record.t() | nil]` directly without the [`BatchResult`](Aerospike.BatchResult.html) wrapper. Use [`batch_operate/3`](Aerospike.html#batch_operate/3) when
you need heterogeneous operations or per-key error information.

## Error handling

Batch functions return `{:error, %Aerospike.Error{}}` for top-level failures (connection
issues, invalid options). Per-key errors only appear in [`batch_operate/3`](Aerospike.html#batch_operate/3) results:

```elixir
case Aerospike.batch_get(:aero, keys) do
  {:ok, records} ->
    process_records(records)

  {:error, %Aerospike.Error{code: :timeout}} ->
    Logger.error("batch timed out")

  {:error, %Aerospike.Error{} = err} ->
    Logger.error("batch failed: #{err.message}")
end
```

Bang variants ([`batch_get!/3`](Aerospike.html#batch_get!/3), [`batch_exists!/3`](Aerospike.html#batch_exists!/3), [`batch_operate!/3`](Aerospike.html#batch_operate!/3)) raise
[`Aerospike.Error`](Aerospike.Error.html) on top-level failures:

```elixir
records = Aerospike.batch_get!(:aero, keys, bins: ["name"])
```

## Chunked bulk ingest

The library does not auto-chunk. For very large writes, split your list (for example `Enum.chunk_every/2`) and run chunks under `Task.async_stream/3` or similar, handling errors per chunk:

```elixir
records
|> Stream.map(fn {id, data} ->
  Batch.put(Aerospike.key("test", "events", id), data)
end)
|> Stream.chunk_every(500)
|> Task.async_stream(
  fn batch -> Aerospike.batch_operate(:aero, batch) end,
  max_concurrency: 10,
  ordered: false
)
|> Stream.each(fn
  {:ok, {:ok, results}} ->
    errors = Enum.count(results, &(&1.status == :error))
    if errors > 0, do: Logger.warning("#{errors} keys failed in chunk")

  {:ok, {:error, err}} ->
    Logger.error("chunk failed: #{err.message}")

  {:exit, reason} ->
    Logger.error("task crashed: #{inspect(reason)}")
end)
|> Stream.run()
```

## Policies and defaults

Batch options (`:timeout`, `:pool_checkout_timeout`, `:replica`, `:respond_all_keys`, `:filter`) can be set in `defaults: [batch: ...]` at [`Aerospike.start_link/1`](Aerospike.html#start_link/1). **Per-call options win:** keywords passed to [`batch_get/3`](Aerospike.html#batch_get/3) (and other batch functions) are merged on top of those defaults, so a call-level `:timeout` overrides the default.

```elixir
Aerospike.start_link(
  name: :aero,
  hosts: ["127.0.0.1:3000"],
  defaults: [batch: [timeout: 2_000], read: [bins: ["name"]]]
)

# Effective batch timeout is 500; read still uses default bins unless overridden.
Aerospike.batch_get(:aero, keys, timeout: 500)
```

[`batch_get/3`](Aerospike.html#batch_get/3) also merges `defaults: [read: ...]` for read-specific options (`:bins`, `:header_only`, `:read_touch_ttl_percent`). **Replica** can be an atom (`:master`, `:sequence`, `:any`) or a non-negative integer replica index. **Filter** uses [`Aerospike.Exp.from_wire/1`](Aerospike.Exp.html#from_wire/1) with pre-encoded expression bytes until the expression builder API ships.

## Telemetry

Batch commands emit `:telemetry.span/3` under `[:aerospike, :command]` with `command` set to `:batch_get`, `:batch_exists`, or `:batch_operate`. Stop metadata includes `result` and `batch_size`.

```elixir
:telemetry.attach("batch-logger", [:aerospike, :command, :stop], fn
  _event, measurements, %{command: cmd, batch_size: size, result: result}, _config ->
    IO.puts("#{cmd} #{size} keys in #{measurements.duration / 1_000_000}ms — #{inspect(result)}")
end, nil)
```

## Next steps

- [Getting Started](getting-started.md) — connecting, basic CRUD, policy options
- [Working with Operations](operate-and-cdt.md) — atomic multi-op and CDT operations
- [`Aerospike.Batch`](Aerospike.Batch.html) — constructor function reference
- [`Aerospike.BatchResult`](Aerospike.BatchResult.html) — per-key result struct reference
