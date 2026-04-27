# Operate, CDT, And Geo

`Aerospike.operate/4` executes a list of server-side operations against one
record. Use it for atomic primitive updates, complex data type operations,
expression operations, bit operations, HyperLogLog operations, and readbacks.

## Primitive Operations

Primitive operation builders live in `Aerospike.Op`.

```elixir
key = Aerospike.key("test", "sessions", "session:1")

{:ok, _metadata} =
  Aerospike.put(:aerospike, key, %{
    "visits" => 1,
    "status" => "new"
  })

{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.add("visits", 1),
    Aerospike.Op.put("status", "active"),
    Aerospike.Op.get("visits")
  ])

record.bins["visits"]
```

## List Operations

List operations live in `Aerospike.Op.List`. Selector operations can choose
return shapes with return-type helpers.

```elixir
key = Aerospike.key("test", "sessions", "session:events")

{:ok, _metadata} =
  Aerospike.put(:aerospike, key, %{"events" => ["created"]})

{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.List.append("events", "opened"),
    Aerospike.Op.List.size("events")
  ])

record.bins["events"]
```

Selector helpers can address list entries by index, rank, or value. Index is
position in the list. Rank is position in value order, so `-1` means the
highest-ranked value. `return_type:` controls whether the server returns
values, indexes, ranks, counts, or an existence flag.

```elixir
{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.List.get_by_index("events", 0,
      return_type: Aerospike.Op.List.return_value()
    ),
    Aerospike.Op.List.get_by_rank("events", -1,
      return_type: Aerospike.Op.List.return_value()
    ),
    Aerospike.Op.List.get_by_value("events", "opened",
      return_type: Aerospike.Op.List.return_exists()
    )
  ])
```

List write policies are passed as raw server policy integers. Defaults are the
safest choice; set policy values only when your application needs ordered-list
or write-flag behavior.

```elixir
Aerospike.Op.List.append("events", "clicked", policy: %{order: 0, flags: 0})
```

## Map Operations

Map operations live in `Aerospike.Op.Map`.

```elixir
key = Aerospike.key("test", "profiles", "user:stats")

{:ok, _metadata} =
  Aerospike.put(:aerospike, key, %{"stats" => %{"views" => 1}})

{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.Map.increment("stats", "views", 1),
    Aerospike.Op.Map.put("stats", "updated_by", "worker-1"),
    Aerospike.Op.Map.get_by_key("stats", "views",
      return_type: Aerospike.Op.Map.return_value()
    )
  ])

record.bins["stats"]
```

Map selectors can return keys, values, key/value pairs, indexes, ranks, counts,
or existence flags. Key selectors default to keys, value/rank selectors default
to values or key/value pairs depending on the operation; pass `return_type:`
when the desired shape matters.

```elixir
{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.Map.get_by_key("stats", "views",
      return_type: Aerospike.Op.Map.return_value()
    ),
    Aerospike.Op.Map.get_by_rank("stats", -1,
      return_type: Aerospike.Op.Map.return_key_value()
    ),
    Aerospike.Op.Map.get_by_value("stats", 1,
      return_type: Aerospike.Op.Map.return_key()
    )
  ])

record.bins["stats"]
```

Map write policies are also thin wrappers around server policy integers.
`attr: 0` uses the default unordered map behavior; non-zero values request
server map attributes such as ordered maps. `flags:` applies write flags when
the operation supports them.

```elixir
Aerospike.Op.Map.put_items("stats", %{"likes" => 2}, policy: %{attr: 0, flags: 0})
```

## Nested CDT Paths

Nested CDT operations use `Aerospike.Ctx` path steps through the `:ctx` option.

```elixir
key = Aerospike.key("test", "profiles", "user:nested")

{:ok, _metadata} =
  Aerospike.put(:aerospike, key, %{
    "profile" => %{"events" => []}
  })

{:ok, _record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.List.append("profile", "signed-in",
      ctx: [Aerospike.Ctx.map_key("events")]
    )
  ])
```

Context paths can move through maps and lists by key, value, index, or rank.
The operation bin is the top-level CDT bin; each context step navigates inside
that value before the operation runs.

```elixir
key = Aerospike.key("test", "profiles", "user:nested-scores")

{:ok, _metadata} =
  Aerospike.put(:aerospike, key, %{
    "profile" => %{
      "teams" => [
        %{"name" => "red", "scores" => [10, 20]},
        %{"name" => "blue", "scores" => [15]}
      ]
    }
  })

{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.List.append("profile", 25,
      ctx: [
        Aerospike.Ctx.map_key("teams"),
        Aerospike.Ctx.list_index(0),
        Aerospike.Ctx.map_key("scores")
      ]
    ),
    Aerospike.Op.List.get_by_rank("profile", -1,
      ctx: [
        Aerospike.Ctx.map_key("teams"),
        Aerospike.Ctx.list_index(0),
        Aerospike.Ctx.map_key("scores")
      ],
      return_type: Aerospike.Op.List.return_value()
    )
  ])

record.bins["profile"]
```

See `Aerospike.Op.List`, `Aerospike.Op.Map`, and `Aerospike.Ctx` for the full
operation reference.

## Bit Operations

Bit operations require Aerospike blob bins. Plain Elixir binaries are encoded
as strings by this client, so seed bit bins with `{:blob, binary}`.

```elixir
key = Aerospike.key("test", "profiles", "user:flags")

{:ok, _metadata} =
  Aerospike.put(:aerospike, key, %{"flags" => {:blob, <<0>>}})

{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.Bit.set("flags", 0, 8, <<0b1010_0000>>),
    Aerospike.Op.Bit.count("flags", 0, 8)
  ])

record.bins["flags"]
```

## HyperLogLog Operations

HyperLogLog operations live in `Aerospike.Op.HLL` and require server support
for HLL bins.

```elixir
key = Aerospike.key("test", "profiles", "visitors")

{:ok, _metadata} = Aerospike.put(:aerospike, key, %{"seed" => 0})

{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.HLL.init("visitors", 14, 0),
    Aerospike.Op.HLL.add("visitors", ["ada", "grace"], 14, 0),
    Aerospike.Op.HLL.get_count("visitors")
  ])

record.bins["visitors"]
```

## Expression Operations

Expression operate helpers live in `Aerospike.Op.Exp` and return or write
server-side expression values.

```elixir
{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.Exp.read("projected", Aerospike.Exp.int_bin("score")),
    Aerospike.Op.Exp.write("score_copy", Aerospike.Exp.int_bin("score"))
  ])

record.bins["projected"]
```

## Geo Values And Queries

Use `Aerospike.Geo` typed values for GeoJSON bins. Geo secondary-index queries
use `Aerospike.Filter.geo_within/2` or `Aerospike.Filter.geo_contains/2`.

```elixir
key = Aerospike.key("test", "places", "pdx")
point = Aerospike.Geo.point(-122.6765, 45.5231)

{:ok, _metadata} = Aerospike.put(:aerospike, key, %{"loc" => point})

{:ok, task} =
  Aerospike.create_index(:aerospike, "test", "places",
    bin: "loc",
    name: "places_loc_geo_idx",
    type: :geo2dsphere
  )

:ok = Aerospike.IndexTask.wait(task)

region = Aerospike.Geo.circle(-122.6765, 45.5231, 10_000.0)

query =
  Aerospike.Query.new("test", "places")
  |> Aerospike.Query.where(Aerospike.Filter.geo_within("loc", region))
  |> Aerospike.Query.max_records(100)

{:ok, records} = Aerospike.query_all(:aerospike, query)
```

Geo filters require matching secondary indexes. `query_all/3` and
`query_page/3` require `query.max_records`.
