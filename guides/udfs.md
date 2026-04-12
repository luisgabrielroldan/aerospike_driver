# User Defined Functions (UDFs)

User Defined Functions are server-side scripts written in Lua. They execute inside the
Aerospike server process, co-located with the data, making them suitable for atomic
read-modify-write operations, data transformations, and computations that would otherwise
require multiple round-trips.

## Lua Basics

A UDF package is a `.lua` source file containing one or more named functions. Each function
receives a record as its first argument and may accept additional user-supplied arguments:

```lua
-- multiply.lua

-- Multiplies the value in bin "n" by a factor and returns the result.
function multiply(rec, factor)
  local val = rec["n"]
  if val == nil then return nil end
  rec["n"] = val * factor
  aerospike:update(rec)
  return rec["n"]
end

-- Returns the bin value unchanged (useful for testing registration).
function echo(rec, arg)
  return arg
end
```

## Registering a UDF

[`Aerospike.register_udf/3`](Aerospike.html#register_udf/3) uploads the package to the cluster. Pass either a filesystem
path ending in `.lua` or the raw Lua source as a string:

```elixir
# From a file path
{:ok, task} = MyApp.Repo.register_udf("/path/to/multiply.lua", "multiply.lua")

# From a string
lua = ~S"""
function echo(rec, arg)
  return arg
end
"""
{:ok, task} = MyApp.Repo.register_udf(lua, "echo.lua")
```

`server_name` (the third argument) is the package name as stored on the server. Use the
filename convention (`"module.lua"`) for clarity — this is the value you pass to
[`apply_udf/5`](Aerospike.html#apply_udf/5) as the `package` argument (without the `.lua` extension).

Registration is asynchronous. The server propagates the package across cluster nodes in the
background.

## Waiting for Registration to Complete

[`register_udf/3`](Aerospike.html#register_udf/3) returns an [`Aerospike.RegisterTask`](Aerospike.RegisterTask.html). Block until registration is
propagated to all nodes:

```elixir
:ok = Aerospike.RegisterTask.wait(task, timeout: 15_000)
```

Or poll without blocking:

```elixir
case Aerospike.RegisterTask.status(task) do
  {:ok, :complete}    -> IO.puts("registered on all nodes")
  {:ok, :in_progress} -> IO.puts("still propagating...")
  {:error, err}       -> IO.puts("status check failed: #{err.message}")
end
```

[`wait/2`](Aerospike.AsyncTask.html#c:wait/2) options (see [`Aerospike.AsyncTask`](Aerospike.AsyncTask.html)):

| Option | Default | Description |
|--------|---------|-------------|
| `:timeout` | no limit | Maximum time to wait in milliseconds |
| `:poll_interval` | 1000 | Milliseconds between status checks |

## Applying a UDF to a Record

[`Aerospike.apply_udf/5`](Aerospike.html#apply_udf/5) executes a Lua function on a single record:

```elixir
key = MyApp.Repo.key("test", "items", "item:1")
:ok = MyApp.Repo.put(key, %{"n" => 10})

{:ok, result} = MyApp.Repo.apply_udf(key, "multiply", "multiply", [3])
# result => 30
```

Arguments:

| Argument | Description |
|----------|-------------|
| `key` | [`Aerospike.Key`](Aerospike.Key.html) for the record to operate on |
| `package` | Lua module name **without** the `.lua` extension |
| `function` | Lua function name |
| `args` | List of arguments passed to the function (after the record) |

The Lua function's return value becomes the Elixir return value. Aerospike's MessagePack
encoding maps Lua types to Elixir terms:

| Lua type | Elixir type |
|----------|-------------|
| `nil` | `nil` |
| `number` (integer) | `integer()` |
| `number` (float) | `float()` |
| `string` | `binary()` |
| `boolean` | `boolean()` |
| `table` (array) | `list()` |
| `table` (map) | `map()` |

### Options

```elixir
{:ok, result} =
  MyApp.Repo.apply_udf(key, "multiply", "multiply", [3],
    timeout: 500,
    replica: :master
  )
```

The above call uses [`apply_udf/6`](Aerospike.html#apply_udf/6) (extra keyword options). Available options: `:timeout`, `:pool_checkout_timeout`, `:filter`, `:replica`.

## Full Lifecycle Example

```elixir
alias Aerospike.RegisterTask

# 1. Write the Lua source
lua = ~S"""
function transform(rec, factor)
  local val = rec["value"]
  if val == nil then return nil end
  rec["value"] = val * factor
  aerospike:update(rec)
  return rec["value"]
end
"""

# 2. Register and wait
{:ok, task} = MyApp.Repo.register_udf(lua, "transform.lua")
:ok = RegisterTask.wait(task, timeout: 15_000)

# 3. Prepare test data
key = MyApp.Repo.key("test", "demo", "k1")
:ok = MyApp.Repo.put(key, %{"value" => 5})

# 4. Apply the UDF
{:ok, result} = MyApp.Repo.apply_udf(key, "transform", "transform", [6])
IO.puts("result: #{result}")   # => "result: 30"

# 5. Remove the package when done
:ok = MyApp.Repo.remove_udf("transform.lua")
```

## Removing a UDF

[`Aerospike.remove_udf/2`](Aerospike.html#remove_udf/2) deletes the package from all cluster nodes. Returns `:ok` whether or not
the package was registered:

```elixir
:ok = MyApp.Repo.remove_udf("multiply.lua")
```

## Error Handling

### UDF Runtime Errors

When the Lua function raises an error, [`apply_udf/5`](Aerospike.html#apply_udf/5) returns `{:error, %Aerospike.Error{code: :udf_bad_response}}`. The `message` field contains the Lua error string:

```elixir
case MyApp.Repo.apply_udf(key, "mymodule", "risky_fn", [arg]) do
  {:ok, result} ->
    process(result)

  {:error, %Aerospike.Error{code: :udf_bad_response, message: msg}} ->
    Logger.error("UDF runtime error: #{msg}")

  {:error, %Aerospike.Error{} = err} ->
    Logger.error("request failed: #{err.message}")
end
```

### Record Not Found

If the key does not exist, the Lua function still runs — `rec` is a new, empty record. You
can distinguish this case inside Lua:

```lua
function safe_read(rec, bin_name)
  if not aerospike:exists(rec) then
    return nil
  end
  return rec[bin_name]
end
```

Or guard in Elixir:

```elixir
case MyApp.Repo.exists(key) do
  {:ok, true} ->
    MyApp.Repo.apply_udf(key, "module", "fn", [])

  {:ok, false} ->
    {:error, :not_found}
end
```

## Notes

- **Packages are cluster-wide**: a registered package is available on all nodes and
  persists across server restarts.
- **Package name**: the `server_name` in [`register_udf/3`](Aerospike.html#register_udf/3) and the `package` argument to
  [`apply_udf/5`](Aerospike.html#apply_udf/5) are both the filename **without** the `.lua` extension. That is, if you
  register with `server_name: "my_module.lua"`, call [`apply_udf/5`](Aerospike.html#apply_udf/5) with `package: "my_module"`.
- **Lua version**: Aerospike uses Lua 5.1.
- **Thread safety**: the server runs UDFs in a single-threaded context per record —
  concurrent `apply_udf` calls on different keys are safe.
- **No network calls inside UDFs**: Lua code cannot make external calls; UDFs are purely
  in-process transformations.

## Next Steps

- [`Aerospike.RegisterTask`](Aerospike.RegisterTask.html) — polling task reference
- [`Aerospike`](Aerospike.html) — facade functions: [`register_udf/3`](Aerospike.html#register_udf/3), [`apply_udf/5`](Aerospike.html#apply_udf/5), [`apply_udf/6`](Aerospike.html#apply_udf/6), [`remove_udf/2`](Aerospike.html#remove_udf/2)
- [Batch Operations](batch-operations.md) — [`Aerospike.Batch.udf/5`](Aerospike.Batch.html#udf/5) for multi-key UDF invocations
