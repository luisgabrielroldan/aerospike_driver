# Map Patterns

Aerospike maps are server-side key-value structures stored in a single bin. They support
rich operations — lookups by key, index, rank, value ranges, and more — all executed
atomically on the server. This guide covers real-world patterns using `Aerospike.Op.Map`.

## Map Basics

Create a map by writing entries with `put/4` or `put_items/3`:

```elixir
alias Aerospike.Op.Map

key = Aerospike.key("test", "users", "user:42")

{:ok, _} = Aerospike.operate(:aero, key, [
  Map.put_items("profile", %{
    "name" => "Ada Lovelace",
    "email" => "ada@example.com",
    "plan" => "pro"
  })
])
```

Read entries back by key:

```elixir
{:ok, rec} = Aerospike.operate(:aero, key, [
  Map.get_by_key("profile", "email")
])
rec.bins["profile"]  # => "ada@example.com"
```

## Map Ordering

Maps can be **unordered** (default), **key-ordered**, or **key-value-ordered**. Key-ordered
maps give O(log N) lookups on in-memory namespaces and are recommended for most use cases.

Set the order when creating the map using `policy:`:

```elixir
# Key-ordered map (attr: 1 = KEY_ORDERED)
Map.put("settings", "theme", "dark", policy: %{attr: 1, flags: 0})
```

| Order Attribute | Value | Description |
|----------------|-------|-------------|
| `0` | `UNORDERED` | Default hash-based storage |
| `1` | `KEY_ORDERED` | Sorted by key — recommended |
| `3` | `KEY_VALUE_ORDERED` | Sorted by key, then by value |

## Write Flags

Map write operations accept flags to control create/update behavior:

| Flag | Value | Description |
|------|-------|-------------|
| `DEFAULT` | `0` | Upsert — create or update |
| `CREATE_ONLY` | `1` | Fail if key already exists |
| `UPDATE_ONLY` | `2` | Fail if key does not exist |
| `NO_FAIL` | `4` | No-op instead of failing on policy violation |
| `DO_PARTIAL` | `8` | With `NO_FAIL`: apply entries that don't violate the policy |

Combine flags with bitwise OR:

```elixir
import Bitwise

# Only insert new keys, skip existing ones without error
Map.put_items("prefs", %{"theme" => "dark", "lang" => "en"},
  policy: %{attr: 1, flags: 1 ||| 4})
```

This is useful for initializing defaults without overwriting user-set values.

## Pattern: Event History Container

Store timestamped events in a single record, keyed by millisecond timestamp.
Each event's value is a list tuple `[event_type, attributes]`:

```elixir
alias Aerospike.Op.Map

key = Aerospike.key("test", "activity", "user:42")

# Record events
Aerospike.operate(:aero, key, [
  Map.put("events", 1_523_474_230_000, ["fav", %{"sku" => 1}]),
  Map.put("events", 1_523_474_231_001, ["comment", %{"sku" => 2, "body" => "Great!"}]),
  Map.put("events", 1_523_474_233_003, ["viewed", %{"sku" => 3}]),
  Map.put("events", 1_523_474_234_004, ["viewed", %{"sku" => 1}]),
  Map.put("events", 1_523_474_235_005, ["comment", %{"sku" => 1, "body" => "Nice"}]),
  Map.put("events", 1_523_474_236_006, ["viewed", %{"sku" => 3}])
])
```

### Querying Events by Time Range

Use `get_by_key_range/4` to retrieve events within a time window:

```elixir
# Get events between two timestamps (inclusive start, exclusive end)
{:ok, rec} = Aerospike.operate(:aero, key, [
  Map.get_by_key_range("events", 1_523_474_231_000, 1_523_474_235_000)
])
```

### Counting Events

Return just the count instead of the full data:

```elixir
{:ok, rec} = Aerospike.operate(:aero, key, [
  Map.size("events")
])
rec.bins["events"]  # => 6
```

### Trimming Old Events

Keep only the last N events to bound record size. Use `remove_by_index_range/4` to
remove everything except the most recent entries:

```elixir
# Keep only the last 1000 events (remove indexes 0 through -1001)
Aerospike.operate(:aero, key, [
  Map.remove_by_index_range("events", 0, max(0, total - 1000),
    return_type: Map.return_none())
])
```

Alternatively, remove events older than a threshold:

```elixir
# Remove events before a cutoff timestamp
cutoff = System.system_time(:millisecond) - 86_400_000  # 24 hours ago
Aerospike.operate(:aero, key, [
  Map.remove_by_key_range("events", nil, cutoff,
    return_type: Map.return_none())
])
```

## Pattern: Document Store

Use nested maps to store semi-structured documents:

```elixir
key = Aerospike.key("test", "sightings", 5001)

Aerospike.put(:aero, key, %{
  "occurred" => 20_220_531,
  "report" => %{
    "shape" => ["circle", "flash", "disc"],
    "summary" => "Large flying disc flashed in the sky",
    "city" => "Ann Arbor",
    "state" => "Michigan",
    "duration" => "5 minutes"
  }
})
```

Atomically update a field inside the document and read another:

```elixir
alias Aerospike.Op.Map
import Aerospike.Op

{:ok, rec} = Aerospike.operate(:aero, key, [
  put("occurred", 20_220_601),
  Map.put("report", "city", "Ypsilanti"),
  get("report")
])
```

## Pattern: Leaderboard

Use rank-based operations to build leaderboards. Map keys are player IDs,
values are scores:

```elixir
alias Aerospike.Op.Map

key = Aerospike.key("test", "games", "leaderboard:daily")

# Record scores
Aerospike.operate(:aero, key, [
  Map.put("scores", "player:1", 2500),
  Map.put("scores", "player:2", 4200),
  Map.put("scores", "player:3", 1800),
  Map.put("scores", "player:4", 3900),
  Map.put("scores", "player:5", 5100)
])

# Get the top 3 scores (highest rank = highest value)
{:ok, rec} = Aerospike.operate(:aero, key, [
  Map.get_by_rank_range("scores", -3, 3,
    return_type: Map.return_key_value())
])

# Increment a player's score
Aerospike.operate(:aero, key, [
  Map.increment("scores", "player:1", 500)
])
```

## Pattern: Atomic Counters per Category

Track counts by category using map increment:

```elixir
alias Aerospike.Op.Map

key = Aerospike.key("test", "metrics", "page:home")

# Increment multiple counters atomically
Aerospike.operate(:aero, key, [
  Map.increment("stats", "views", 1),
  Map.increment("stats", "clicks", 1),
  Map.increment("stats", "shares", 0)
])

# Read all counters
{:ok, rec} = Aerospike.operate(:aero, key, [
  Map.get_by_index_range_from("stats", 0)
])
```

## Pattern: Feature Flags / Preferences

Use `CREATE_ONLY | NO_FAIL` to set defaults without overwriting existing values:

```elixir
alias Aerospike.Op.Map
import Bitwise

# Initialize defaults — won't overwrite if user already set them
Aerospike.operate(:aero, key, [
  Map.put_items("prefs", %{
    "theme" => "light",
    "notifications" => true,
    "lang" => "en"
  }, policy: %{attr: 1, flags: 1 ||| 4})  # CREATE_ONLY | NO_FAIL
])
```

## Next Steps

- [List Patterns](list-patterns.md) — queues, time series, bounded lists
- [Nested Operations](nested-operations.md) — operating on deeply nested maps and lists
- `Aerospike.Op.Map` — complete API reference
