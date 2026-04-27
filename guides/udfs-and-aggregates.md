# UDFs And Aggregates

The public UDF surface covers package list/register/remove, single-record UDF
execution, batch record UDF execution, background query UDF jobs, and aggregate
queries. It does not claim broad UDF package tooling beyond those commands.

## Lua Package Example

A UDF package is a `.lua` file with one or more named functions. Record UDFs
receive the target record as the first argument, followed by the arguments
passed from Elixir.

```lua
-- priv/udf/records.lua

function mark_seen(rec, source)
  local visits = rec["visits"]

  if visits == nil then
    visits = 0
  end

  rec["visits"] = visits + 1
  rec["last_seen_source"] = source

  aerospike:update(rec)

  return rec["visits"]
end
```

After registering this file as `records.lua`, call it with package name
`"records"` and function name `"mark_seen"`.

Aggregate UDFs receive a stream instead of one record. The same package can
contain record UDF functions and aggregate functions, but aggregate functions
must return stream operations.

```lua
-- priv/udf/user_stats.lua

local function add_bin(total, rec, bin_name)
  return total + (rec[bin_name] or 0)
end

local function add_age(total, rec)
  return add_bin(total, rec, "age")
end

local function merge_sum(left, right)
  return left + right
end

function sum_age(stream, bin_name)
  return stream : aggregate(0, function(total, rec)
    return add_bin(total, rec, bin_name)
  end) : reduce(merge_sum)
end

function sum_summary(stream)
  return stream : aggregate(0, add_age) : reduce(merge_sum) : map(function(total)
    return map{sum = total, labels = {"total", tostring(total)}}
  end)
end
```

## Package Lifecycle

Upload a Lua package from a readable path or inline source. The server filename
usually includes `.lua`; record and query execution use the package name
without that suffix.

```elixir
server_name = "records.lua"
package = "records"

{:ok, task} =
  Aerospike.register_udf(:aerospike, "priv/udf/records.lua", server_name)

:ok = Aerospike.RegisterTask.wait(task, timeout: 10_000, poll_interval: 200)
{:ok, udfs} = Aerospike.list_udfs(:aerospike)

Enum.find(udfs, &(&1.filename == server_name))
```

Remove by server filename:

```elixir
:ok = Aerospike.remove_udf(:aerospike, server_name)
```

## Single-Record UDFs

`apply_udf/6` executes one function against one record key.

```elixir
key = Aerospike.key("test", "users", "user:42")

{:ok, _metadata} =
  Aerospike.put(:aerospike, key, %{"visits" => 1})

{:ok, result} =
  Aerospike.apply_udf(:aerospike, key, package, "mark_seen", ["web"])
```

The helper accepts the narrow write-family option set, including `:timeout`,
`:ttl`, `:generation`, `:filter`, and `:txn`. Once a record UDF request is on
the wire, transport failures are not retried automatically because server-side
effects may already have occurred.

## Background Query UDF Jobs

`query_udf/6` starts a background job and returns an `Aerospike.ExecuteTask`.

```elixir
query =
  Aerospike.Query.new("test", "users")
  |> Aerospike.Query.where(Aerospike.Filter.equal("status", "inactive"))

{:ok, task} =
  Aerospike.query_udf(:aerospike, query, package, "deactivate", [])

:ok = Aerospike.ExecuteTask.wait(task, timeout: 30_000, poll_interval: 500)
```

Pass `node: node_name` when the background job should target one active node.

## Aggregate Streams

`query_aggregate/6` returns the partial values emitted by the server. The
client does not run local Lua finalization on this path.

```elixir
server_name = "user_stats.lua"
package = "user_stats"

{:ok, task} =
  Aerospike.register_udf(:aerospike, "priv/udf/user_stats.lua", server_name)

:ok = Aerospike.RegisterTask.wait(task, timeout: 10_000, poll_interval: 200)

query =
  Aerospike.Query.new("test", "users")
  |> Aerospike.Query.where(Aerospike.Filter.range("age", 18, 65))

{:ok, partials} =
  Aerospike.query_aggregate(:aerospike, query, package, "sum_age", ["age"],
    timeout: 10_000
  )

total = partials |> Enum.to_list() |> Enum.sum()
```

In this example, the server package was registered as `"user_stats.lua"`, the
package argument is `"user_stats"`, the function argument is `"sum_age"`, and
`["age"]` is passed to the Lua function after the stream.

## Finalized Aggregate Results

Use `query_aggregate_result/6` when the caller wants one locally finalized
result. The local Lua source is required even when the package is already
registered on the server.

```elixir
{:ok, total} =
  Aerospike.query_aggregate_result(
    :aerospike,
    query,
    package,
    "sum_age",
    ["age"],
    source_path: "priv/udf/user_stats.lua",
    timeout: 10_000
  )
```

Pass exactly one of `source: lua_source` or `source_path: path`. Missing
source, both source options, unreadable files, unsupported local arguments, or
`node: node_name` return an invalid-argument error before the server query is
opened.

The registered server package and the local source are related but separate:

- Register `"user_stats.lua"` on the server before calling
  `query_aggregate/6` or `query_aggregate_result/6`.
- Pass package name `"user_stats"` and function name `"sum_age"` to the query
  call.
- Pass `source_path: "priv/udf/user_stats.lua"` or `source: lua_source` only
  for `query_aggregate_result/6`, because local finalization needs the Lua
  source available to the Elixir client.
- Keep the local source aligned with the registered package. The client does
  not fetch Lua source back from the server or infer a path from the package
  name.

The local reducer runs in a bounded Lua state. Supported stream helpers are
`map`, `filter`, `aggregate`, and `reduce`; logging helpers are no-ops. Local
filesystem, OS, package loading, dynamic loading, debug access, `require`,
`groupby`, `list`, `bytes`, and record/database mutation or lookup helpers
fail explicitly.

Values crossing the local Lua boundary are limited to `nil`, booleans,
integers, floats, binaries, lists, and maps with scalar keys. Empty
finalization returns `{:ok, nil}`; multiple final values return an error.

```elixir
{:ok, summary} =
  Aerospike.query_aggregate_result(
    :aerospike,
    query,
    package,
    "sum_summary",
    [],
    source_path: "priv/udf/user_stats.lua",
    timeout: 10_000
  )

%{"labels" => labels, "sum" => sum} = summary
```
