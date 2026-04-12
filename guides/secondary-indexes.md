# Secondary Indexes

A secondary index (SI) lets you query records by bin value rather than primary key. The server
maintains the index on a per-namespace, per-set, per-bin basis and updates it automatically as
records change.

Secondary indexes are complementary to [`Aerospike.Query`](Aerospike.Query.html) — once an index exists, pass
[`Aerospike.Filter`](Aerospike.Filter.html) predicates in your query to use it.

This client ships three practical SI families:

- Standard bin-backed indexes created with [`create_index/4`](Aerospike.html#create_index/4)
- Advanced CDT indexes that add `:collection` and optional nested `:ctx`
- Expression-backed indexes created with [`create_expression_index/5`](Aerospike.html#create_expression_index/5)

## Creating an Index

[`Aerospike.create_index/4`](Aerospike.html#create_index/4) sends the creation command to the server. Index building happens
asynchronously in the background:

```elixir
{:ok, task} =
  MyApp.Repo.create_index("test", "users",
    bin: "age",
    name: "users_age_idx",
    type: :numeric
  )
```

Required options:

| Option | Type | Description |
|--------|------|-------------|
| `:bin` | `string` | The bin name to index |
| `:name` | `string` | A unique name for this index |
| `:type` | atom | `:numeric`, `:string`, or `:geo2dsphere` |

## Waiting for the Index to Be Ready

[`create_index/4`](Aerospike.html#create_index/4) returns an [`%Aerospike.IndexTask{}`](Aerospike.IndexTask.html). Use [`IndexTask.wait/2`](Aerospike.IndexTask.html#wait/2) to block until
the server reports the index is fully built:

```elixir
:ok = Aerospike.IndexTask.wait(task, timeout: 30_000)
```

Or poll without blocking:

```elixir
case Aerospike.IndexTask.status(task) do
  {:ok, :complete}     -> IO.puts("index ready")
  {:ok, :in_progress}  -> IO.puts("still building...")
  {:error, err}        -> IO.puts("check failed: #{err.message}")
end
```

[`IndexTask.wait/2`](Aerospike.IndexTask.html#wait/2) polls [`status/1`](Aerospike.IndexTask.html#status/1) in a loop. Options:

| Option | Default | Description |
|--------|---------|-------------|
| `:timeout` | no limit | Maximum time to wait in milliseconds |
| `:poll_interval` | 1000 | Milliseconds between status checks |

## Index Types

### Numeric

Index integer or float bins. Use with [`Filter.range/3`](Aerospike.Filter.html#range/3) or [`Filter.equal/2`](Aerospike.Filter.html#equal/2) (integers only):

```elixir
{:ok, task} =
  MyApp.Repo.create_index("test", "metrics",
    bin: "score",
    name: "metrics_score_idx",
    type: :numeric
  )

:ok = Aerospike.IndexTask.wait(task)
```

### String

Index string bins. Use with [`Filter.equal/2`](Aerospike.Filter.html#equal/2):

```elixir
{:ok, task} =
  MyApp.Repo.create_index("test", "users",
    bin: "email",
    name: "users_email_idx",
    type: :string
  )

:ok = Aerospike.IndexTask.wait(task)
```

### Geo2DSphere

Index GeoJSON point or region bins for geospatial queries:

```elixir
{:ok, task} =
  MyApp.Repo.create_index("test", "locations",
    bin: "coords",
    name: "locations_coords_idx",
    type: :geo2dsphere
  )

:ok = Aerospike.IndexTask.wait(task)
```

Once the index is ready, geospatial predicates can use typed
[`Aerospike.Geo`](Aerospike.Geo.html) structs:

```elixir
alias Aerospike.{Filter, Geo, Query}

point = Geo.point(-122.5, 45.5)
region = Geo.circle(-122.5, 45.5, 5_000)

{:ok, matches} =
  Query.new("test", "locations")
  |> Query.where(Filter.geo_within("coords", region))
  |> Query.max_records(10_000)
  |> then(&MyApp.Repo.all(&1))

{:ok, containing_regions} =
  Query.new("test", "regions")
  |> Query.where(Filter.geo_contains("shape", point))
  |> Query.max_records(10_000)
  |> then(&MyApp.Repo.all(&1))
```

Raw GeoJSON strings remain supported for backward compatibility:

```elixir
region_json = "{\"type\":\"AeroCircle\",\"coordinates\":[[-122.5,45.5],5000]}"

Query.new("test", "locations")
|> Query.where(Filter.geo_within("coords", region_json))
|> Query.max_records(10_000)
```

## Collection Index Types

When the indexed bin holds a List, Map key, or Map value, pass `:collection` to index the
inner values rather than the bin container itself:

```elixir
# Index every integer in a list bin named "tags"
{:ok, task} =
  MyApp.Repo.create_index("test", "articles",
    bin: "tags",
    name: "articles_tags_idx",
    type: :numeric,
    collection: :list
  )

# Index map keys (strings)
{:ok, task} =
  MyApp.Repo.create_index("test", "profiles",
    bin: "preferences",
    name: "profiles_prefs_keys_idx",
    type: :string,
    collection: :mapkeys
  )

# Index map values (integers)
{:ok, task} =
  MyApp.Repo.create_index("test", "scores",
    bin: "game_scores",
    name: "scores_game_idx",
    type: :numeric,
    collection: :mapvalues
  )
```

| `:collection` value | What is indexed |
|---------------------|----------------|
| `:list` | Every element in a list bin |
| `:mapkeys` | Every key in a map bin |
| `:mapvalues` | Every value in a map bin |

## Nested CDT Indexes

Top-level `:collection` support is enough for flat CDT bins, but nested CDT indexes need an
additional `:ctx` path so the server knows which inner list or map to index.

```elixir
alias Aerospike.Ctx

ctx = [Ctx.map_key("roles")]

{:ok, task} =
  MyApp.Repo.create_index("test", "profiles",
    bin: "profile",
    name: "profiles_roles_idx",
    type: :string,
    collection: :list,
    ctx: ctx
  )

:ok = Aerospike.IndexTask.wait(task)
```

Use the same context on the query filter so the predicate targets the nested path that was indexed:

```elixir
alias Aerospike.{Filter, Query}

ctx = [Aerospike.Ctx.map_key("roles")]

{:ok, records} =
  Query.new("test", "profiles")
  |> Query.where(
    Filter.contains("profile", :list, "admin")
    |> Filter.with_ctx(ctx)
  )
  |> Query.max_records(10_000)
  |> then(&MyApp.Repo.all(&1))
```

## Expression-Backed Indexes

Use [`create_expression_index/5`](Aerospike.html#create_expression_index/5) when the indexed value
comes from a server expression instead of a single stored bin.

Expression-backed index creation requires Aerospike server `8.1.0` or newer. On older servers the
client returns a parameter error instead of sending an ambiguous info command.

```elixir
alias Aerospike.{Exp, Filter, Query}

expression =
  Exp.int_bin("age")
  |> Exp.gt(Exp.val(17))

{:ok, task} =
  MyApp.Repo.create_expression_index("test", "users", expression,
    name: "adult_users_idx",
    type: :numeric
  )

:ok = Aerospike.IndexTask.wait(task)

{:ok, records} =
  Query.new("test", "users")
  |> Query.where(Filter.range("age", 18, 120) |> Filter.using_index("adult_users_idx"))
  |> Query.max_records(10_000)
  |> then(&MyApp.Repo.all(&1))
```

The query predicate still carries the value range or equality test, but
[`Filter.using_index/2`](Aerospike.Filter.html#using_index/2) makes the named advanced index the
explicit lookup target.

## Querying with an Index

Pair secondary indexes with [`Aerospike.Query`](Aerospike.Query.html) and [`Aerospike.Filter`](Aerospike.Filter.html). Execute with [`Aerospike.all/3`](Aerospike.html#all/3) (or [`stream!/3`](Aerospike.html#stream!/3), [`page/3`](Aerospike.html#page/3), etc.); [`all/3`](Aerospike.html#all/3) requires [`Query.max_records/2`](Aerospike.Query.html#max_records/2).

```elixir
alias Aerospike.{Filter, Query}

# Range query — requires a numeric index on "age"
{:ok, records} =
  Query.new("test", "users")
  |> Query.where(Filter.range("age", 18, 65))
  |> Query.max_records(10_000)
  |> then(&MyApp.Repo.all(&1))

# Equality query on a string index
{:ok, records} =
  Query.new("test", "users")
  |> Query.where(Filter.equal("email", "user@example.com"))
  |> Query.max_records(10_000)
  |> then(&MyApp.Repo.all(&1))
```

### Combining Index Filters with Expression Filters

Index filters narrow the server-side record set; expression filters refine it further without
requiring an index:

```elixir
alias Aerospike.{Exp, Filter, Query}

query =
  Query.new("test", "users")
  |> Query.where(Filter.range("age", 18, 65))
  |> Query.filter(Exp.eq(Exp.str_bin("status"), Exp.val("active")))
  |> Query.max_records(10_000)

{:ok, records} = MyApp.Repo.all(query)
```

The index filter runs server-side first (fast path), then the expression filter applies to
the smaller result set.

For advanced indexes, use the filter annotations instead of mutating `%Aerospike.Filter{}` directly:

- [`Filter.using_index/2`](Aerospike.Filter.html#using_index/2) targets a named index, including expression-backed indexes.
- [`Filter.with_ctx/2`](Aerospike.Filter.html#with_ctx/2) attaches the nested CDT path for context-aware indexes.

## Dropping an Index

```elixir
:ok = MyApp.Repo.drop_index("test", "users_age_idx")
```

[`drop_index/3`](Aerospike.html#drop_index/3) returns `:ok` whether or not the index existed.

## Full Example: Build, Query, Drop

```elixir
alias Aerospike.{Filter, IndexTask, Query}

# Insert test data
for i <- 1..1000 do
  key = MyApp.Repo.key("test", "users", "user:#{i}")
  MyApp.Repo.put(key, %{"age" => i, "name" => "user#{i}"})
end

# Create and wait for index
{:ok, task} =
  MyApp.Repo.create_index("test", "users",
    bin: "age",
    name: "users_age_idx",
    type: :numeric
  )

:ok = IndexTask.wait(task, timeout: 60_000)

# Query using the index
{:ok, records} =
  Query.new("test", "users")
  |> Query.where(Filter.range("age", 18, 30))
  |> Query.max_records(10_000)
  |> then(&MyApp.Repo.all(&1))

IO.puts("Found #{length(records)} users aged 18–30")

# Tear down the index
:ok = MyApp.Repo.drop_index("test", "users_age_idx")
```

## Error Handling

```elixir
case MyApp.Repo.create_index("test", "users", bin: "age", name: "age_idx", type: :numeric) do
  {:ok, task} ->
    case IndexTask.wait(task, timeout: 30_000) do
      :ok ->
        IO.puts("index ready")

      {:error, %Aerospike.Error{code: :timeout}} ->
        IO.puts("timed out waiting — index may still be building")

      {:error, err} ->
        IO.puts("polling failed: #{err.message}")
    end

  {:error, err} ->
    IO.puts("create_index failed: #{err.message}")
end
```

## Notes

- Index names must be unique within a namespace.
- Creating an index with the same name on a different bin or type returns an error.
- Indexes persist across server restarts.
- Dropping an index that does not exist returns `:ok` (idempotent).
- Background index builds do not block writes — data written during the build is captured.
- Expression-backed index creation requires Aerospike server `8.1.0` or newer.

## Next Steps

- [`Aerospike.IndexTask`](Aerospike.IndexTask.html) — polling task struct reference
- [`Aerospike.Filter`](Aerospike.Filter.html) — index filter predicates ([`equal/2`](Aerospike.Filter.html#equal/2), [`range/3`](Aerospike.Filter.html#range/3))
- [Queries and Scanning](queries-and-scanning.md) — full query API, pagination, partition filters
