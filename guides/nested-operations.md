# Nested CDT Operations

Aerospike supports operations on deeply nested structures — maps inside maps,
lists inside maps, maps inside lists, and arbitrary combinations. The `Aerospike.Ctx`
module provides context paths that tell the server exactly where to apply an operation.

## The Context Path

A context path is a list of `Aerospike.Ctx` elements that navigate from the top-level
bin down to the target element. Think of it like a path in a nested data structure:

```elixir
# Data: %{"profile" => %{"geo" => %{"lat" => 0.0, "lng" => 0.0}}}

# To reach "geo" inside "profile":
ctx: [Aerospike.Ctx.map_key("geo")]
# The operation already targets the "profile" bin, so the ctx navigates within it
```

## Context Types

| Function | Navigates By | Use Case |
|----------|-------------|----------|
| `Ctx.map_key(key)` | Map key lookup | Navigate to a specific map key |
| `Ctx.map_index(index)` | Map index position | Navigate to Nth entry by key order |
| `Ctx.map_rank(rank)` | Map value rank | Navigate to entry by value rank |
| `Ctx.map_value(value)` | Map value match | Navigate to entry with matching value |
| `Ctx.list_index(index)` | List index | Navigate to Nth list element |
| `Ctx.list_rank(rank)` | List value rank | Navigate to element by value rank |
| `Ctx.list_value(value)` | List value match | Navigate to element matching value |

## Map Inside Map

The most common nesting pattern. A record has a top-level map bin, with nested
maps for sub-documents:

```elixir
alias Aerospike.Op.Map

key = Aerospike.key("test", "users", "user:42")

# Create nested structure
Aerospike.put(:aero, key, %{
  "profile" => %{
    "geo" => %{"lat" => 40.7128, "lng" => -74.0060},
    "prefs" => %{"theme" => "dark", "lang" => "en"}
  }
})

# Update latitude inside the nested "geo" map
{:ok, _} = Aerospike.operate(:aero, key, [
  Map.put("profile", "lat", 45.5231,
    ctx: [Aerospike.Ctx.map_key("geo")])
])

# Read a value from the nested "geo" map
{:ok, rec} = Aerospike.operate(:aero, key, [
  Map.get_by_key("profile", "lat",
    ctx: [Aerospike.Ctx.map_key("geo")])
])
rec.bins["profile"]  # => 45.5231
```

### Multiple Levels Deep

Chain context elements for deeper nesting:

```elixir
# Data: %{"config" => %{"ui" => %{"sidebar" => %{"width" => 250}}}}

# Navigate: config → ui → sidebar, then operate on "width"
Aerospike.operate(:aero, key, [
  Map.put("config", "width", 300,
    ctx: [
      Aerospike.Ctx.map_key("ui"),
      Aerospike.Ctx.map_key("sidebar")
    ])
])
```

## List Inside Map

A map value that contains a list:

```elixir
alias Aerospike.Op.List

key = Aerospike.key("test", "sightings", 5001)

# Data: %{"report" => %{"shape" => ["circle", "flash", "disc"], "city" => "Ann Arbor"}}
Aerospike.put(:aero, key, %{
  "report" => %{
    "shape" => ["circle", "flash", "disc"],
    "city" => "Ann Arbor"
  }
})

# Append to the "shape" list inside the "report" map
{:ok, _} = Aerospike.operate(:aero, key, [
  List.append("report", "oval",
    ctx: [Aerospike.Ctx.map_key("shape")])
])

# Read the size of the "shape" list
{:ok, rec} = Aerospike.operate(:aero, key, [
  List.size("report",
    ctx: [Aerospike.Ctx.map_key("shape")])
])
rec.bins["report"]  # => 4
```

## Map Inside List

A list where each element is a map:

```elixir
alias Aerospike.Op.Map

key = Aerospike.key("test", "users", "user:42")

# Data: %{"addresses" => [%{"city" => "NYC", "zip" => "10001"}, %{"city" => "LA", "zip" => "90001"}]}
Aerospike.put(:aero, key, %{
  "addresses" => [
    %{"city" => "NYC", "zip" => "10001"},
    %{"city" => "LA", "zip" => "90001"}
  ]
})

# Update the city in the second address (list index 1)
{:ok, _} = Aerospike.operate(:aero, key, [
  Map.put("addresses", "city", "San Francisco",
    ctx: [Aerospike.Ctx.list_index(1)])
])

# Read from the last address (negative index)
{:ok, rec} = Aerospike.operate(:aero, key, [
  Map.get_by_key("addresses", "city",
    ctx: [Aerospike.Ctx.list_index(-1)])
])
rec.bins["addresses"]  # => "San Francisco"
```

## Nested List Inside List

Lists within lists:

```elixir
alias Aerospike.Op.List

key = Aerospike.key("test", "data", "matrix")

# Data: %{"grid" => [[7, 9, 5], [1, 2, 3], [6, 5, 4, 1]]}
Aerospike.put(:aero, key, %{
  "grid" => [[7, 9, 5], [1, 2, 3], [6, 5, 4, 1]]
})

# Append 11 to the last nested list
{:ok, _} = Aerospike.operate(:aero, key, [
  List.append("grid", 11,
    ctx: [Aerospike.Ctx.list_index(-1)])
])
# grid = [[7, 9, 5], [1, 2, 3], [6, 5, 4, 1, 11]]

# Get the size of the first nested list
{:ok, rec} = Aerospike.operate(:aero, key, [
  List.size("grid",
    ctx: [Aerospike.Ctx.list_index(0)])
])
rec.bins["grid"]  # => 3
```

## Combining Nested Operations

Multiple nested operations can be combined in a single atomic `operate` call,
even targeting different paths within the same bin:

```elixir
alias Aerospike.Op.Map
alias Aerospike.Op.List

key = Aerospike.key("test", "orders", "order:1001")

Aerospike.put(:aero, key, %{
  "order" => %{
    "items" => [
      %{"sku" => "A1", "qty" => 2},
      %{"sku" => "B2", "qty" => 1}
    ],
    "meta" => %{"source" => "web"}
  }
})

# Atomically: add an item, update meta, read item count
{:ok, rec} = Aerospike.operate(:aero, key, [
  List.append("order", %{"sku" => "C3", "qty" => 3},
    ctx: [Aerospike.Ctx.map_key("items")]),
  Map.put("order", "updated", true,
    ctx: [Aerospike.Ctx.map_key("meta")]),
  List.size("order",
    ctx: [Aerospike.Ctx.map_key("items")])
])
```

## Tips

- **Context paths are relative to the bin.** The first argument to any Op function is
  the bin name. The `ctx:` navigates within that bin's value.

- **Negative indexes work in context too.** `Ctx.list_index(-1)` targets the last element,
  `Ctx.map_rank(-1)` targets the highest-valued map entry.

- **Context is supported on all CDT operations.** Every function in `Aerospike.Op.List`
  and `Aerospike.Op.Map` accepts `ctx:`.

- **Keep nesting shallow when possible.** While Aerospike supports deep nesting, flatter
  structures with multiple bins are often simpler to query and maintain.

## Next Steps

- [Map Patterns](map-patterns.md) — event containers, document stores, leaderboards
- [List Patterns](list-patterns.md) — queues, time series, bounded lists
- `Aerospike.Ctx` — complete context type reference
