# List Patterns

Aerospike lists are ordered collections stored in a single bin. They support append, insert,
removal by index/value/rank, sorting, and range queries — all executed atomically on the
server. This guide covers common real-world patterns using [`Aerospike.Op.List`](Aerospike.Op.List.html).

## List Basics

Create and manipulate lists with [`append/3`](Aerospike.Op.List.html#append/3), [`get/3`](Aerospike.Op.List.html#get/3), and [`size/2`](Aerospike.Op.List.html#size/2):

```elixir
alias Aerospike.Op.List

key = MyApp.Repo.key("test", "users", "user:42")

# Build a list from scratch
{:ok, _} = MyApp.Repo.operate(key, [
  List.append("tags", "trial"),
  List.append("tags", "beta"),
  List.append("tags", "vip")
])

# Read the list size
{:ok, rec} = MyApp.Repo.operate(key, [List.size("tags")])
rec.bins["tags"]  # => 3

# Read the first item
{:ok, rec} = MyApp.Repo.operate(key, [List.get("tags", 0)])
rec.bins["tags"]  # => "trial"
```

## List Ordering

Lists default to **unordered** (insertion order preserved). You can set an ordered policy
when creating the list:

```elixir
# Ordered list (order: 1 = ORDERED)
List.append("scores", 42, policy: %{order: 1, flags: 0})
```

Ordered lists maintain sorted order automatically. Appending to an ordered list
inserts the value at its sorted position.

| Order | Value | Description |
|-------|-------|-------------|
| `0` | `UNORDERED` | Preserves insertion order (default) |
| `1` | `ORDERED` | Maintains sorted order |

## Pattern: Queue (FIFO)

Use an unordered list as a first-in, first-out queue. Append to enqueue,
remove from the front to dequeue:

```elixir
alias Aerospike.Op.List

key = MyApp.Repo.key("test", "jobs", "queue:emails")

# Enqueue items
MyApp.Repo.operate(key, [
  List.append("queue", %{"to" => "ada@example.com", "subject" => "Welcome"}),
  List.append("queue", %{"to" => "bob@example.com", "subject" => "Verify"})
])

# Dequeue: pop the first item (returns and removes it)
{:ok, rec} = MyApp.Repo.operate(key, [
  List.pop("queue", 0)
])
rec.bins["queue"]  # => %{"to" => "ada@example.com", "subject" => "Welcome"}

# Atomic enqueue + dequeue in one round-trip
{:ok, _} = MyApp.Repo.operate(key, [
  List.append("queue", %{"to" => "carol@example.com", "subject" => "Reset"}),
  List.pop("queue", 0)
])
```

### Batch Dequeue

Pop multiple items at once for batch processing:

```elixir
# Dequeue up to 10 items
{:ok, rec} = MyApp.Repo.operate(key, [
  List.pop_range("queue", 0, 10)
])
jobs = rec.bins["queue"]  # => list of up to 10 items
```

## Pattern: Time Series Data

Store timestamped data points as `[timestamp, value]` pairs in a list.
Use value-range queries to retrieve data within a time window:

```elixir
alias Aerospike.Op.List

key = MyApp.Repo.key("test", "sensors", "temp:room-1")

# Record temperature readings
MyApp.Repo.operate(key, [
  List.append_items("readings", [
    [1_523_474_230_000, 39.04],
    [1_523_474_231_001, 39.78],
    [1_523_474_233_003, 40.89],
    [1_523_474_234_004, 40.93],
    [1_523_474_235_005, 41.18],
    [1_523_474_236_006, 40.07]
  ])
])

# Query readings in a time range
# Uses Aerospike's list comparison: [start_ts, nil] <= [ts, val] < [end_ts, nil]
{:ok, rec} = MyApp.Repo.operate(key, [
  List.get_by_value_range("readings",
    [1_523_474_231_000, nil],
    [1_523_474_234_000, nil])
])
# => [[1_523_474_231_001, 39.78], [1_523_474_233_003, 40.89]]
```

> **How range comparison works:** Aerospike compares list values element by element.
> `nil` sorts lower than any other value, so `[timestamp, nil]` acts as a lower
> bound at that timestamp regardless of the data value.

## Pattern: Bounded List (Keep Last N)

Cap the list size to prevent unbounded growth. Use [`trim/4`](Aerospike.Op.List.html#trim/4) to keep only
the most recent entries:

```elixir
alias Aerospike.Op.List

key = MyApp.Repo.key("test", "users", "user:42")
max_entries = 100

# Append a new entry and trim in one atomic operation
MyApp.Repo.operate(key, [
  List.append("history", %{"page" => "/settings", "ts" => System.system_time(:second)}),
  List.trim("history", -max_entries, max_entries)
])
```

`trim/4` keeps items in the range `[index, index + count)` and removes everything else.
Using `-100, 100` keeps the last 100 items.

## Pattern: Tag / Membership List

Manage a set of tags or memberships. Use value-based operations for lookups:

```elixir
alias Aerospike.Op.List

key = MyApp.Repo.key("test", "users", "user:42")

# Add a tag
MyApp.Repo.operate(key, [
  List.append("tags", "premium")
])

# Remove a specific tag
MyApp.Repo.operate(key, [
  List.remove_by_value("tags", "trial", return_type: List.return_none())
])

# Remove multiple tags at once
MyApp.Repo.operate(key, [
  List.remove_by_value_list("tags", ["expired", "legacy"],
    return_type: List.return_count())
])
```

## Pattern: Sorted Unique List

Use an ordered list with `DROP_DUPLICATES` sort flag to maintain a sorted
set of unique values:

```elixir
alias Aerospike.Op.List

key = MyApp.Repo.key("test", "data", "scores")

# Sort and deduplicate (sort_flags: 2 = DROP_DUPLICATES)
MyApp.Repo.operate(key, [
  List.append_items("scores", [95, 87, 95, 72, 87, 100]),
  List.sort("scores", 2)
])

# Result: [72, 87, 95, 100]
```

Sort flags can be combined:
- `0` — ascending order
- `1` — descending order
- `2` — drop duplicates

## Pattern: Top-N / Bottom-N by Value

Use rank-based operations to get the highest or lowest values:

```elixir
alias Aerospike.Op.List

key = MyApp.Repo.key("test", "data", "measurements")

# Get the 3 highest values (rank -3 = third from highest, count 3)
{:ok, rec} = MyApp.Repo.operate(key, [
  List.get_by_rank_range("measurements", -3, 3)
])

# Get the single lowest value
{:ok, rec} = MyApp.Repo.operate(key, [
  List.get_by_rank("measurements", 0)
])
```

## Pattern: Relative Rank Queries

Find values near a reference point using relative rank operations:

```elixir
alias Aerospike.Op.List

# Given an ordered list [0, 4, 5, 9, 11, 15]

# Get 2 items nearest to value 5, starting from its position
{:ok, rec} = MyApp.Repo.operate(key, [
  List.get_by_value_rel_rank_range_count("scores", 5, 0, 2)
])
# => [5, 9]

# Get items starting from one rank below value 5
{:ok, rec} = MyApp.Repo.operate(key, [
  List.get_by_value_rel_rank_range_count("scores", 5, -1, 3)
])
# => [4, 5, 9]
```

## Next Steps

- [Map Patterns](map-patterns.md) — event containers, document stores, leaderboards
- [Nested Operations](nested-operations.md) — operating on lists inside maps and vice versa
- [`Aerospike.Op.List`](Aerospike.Op.List.html) — complete API reference
