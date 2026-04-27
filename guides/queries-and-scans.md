# Queries And Scans

Scans walk records by namespace, set, or partition. Queries add a
secondary-index predicate through `Aerospike.Filter`. Both surfaces use pure
builder structs and run through explicit `Aerospike` facade calls.

The current stream helpers are lazy at the outer `Enumerable` boundary, but
the runtime buffers each node's response before yielding that node's records.
There is no public cancellation API.

## Scan Streams

Use `Aerospike.Scan` when no secondary index predicate is needed.

```elixir
scan =
  Aerospike.Scan.new("test", "events")
  |> Aerospike.Scan.select(["type", "created_at"])
  |> Aerospike.Scan.filter(
    Aerospike.Exp.gte(
      Aerospike.Exp.int_bin("created_at"),
      Aerospike.Exp.int(1_700_000_000)
    )
  )
  |> Aerospike.Scan.records_per_second(1_000)

{:ok, stream} = Aerospike.scan_stream(:aerospike, scan)

Enum.each(stream, fn record ->
  record.bins["type"]
end)
```

`Scan.filter/2` appends server-side expression filters. Use
`Aerospike.Exp` there, not `Aerospike.Filter`.

## Query Predicates

Use `Aerospike.Query.where/2` for one secondary-index predicate.

```elixir
{:ok, task} =
  Aerospike.create_index(:aerospike, "test", "users",
    bin: "age",
    name: "users_age_idx",
    type: :numeric
  )

:ok = Aerospike.IndexTask.wait(task)

query =
  Aerospike.Query.new("test", "users")
  |> Aerospike.Query.where(Aerospike.Filter.range("age", 18, 65))
  |> Aerospike.Query.filter(
    Aerospike.Exp.eq(Aerospike.Exp.str_bin("status"), Aerospike.Exp.str("active"))
  )
  |> Aerospike.Query.select(["name", "age"])

{:ok, stream} = Aerospike.query_stream(:aerospike, query)
names = stream |> Enum.map(& &1.bins["name"])
```

Secondary-index filters and expression filters are separate lanes:
`Query.where/2` carries the index predicate and `Query.filter/2` carries
server-side expression filters. They can be combined where the server command
supports both.

## Collection And Paging

`query_all/3`, `scan_page/3`, and `query_page/3` require an explicit
`max_records` budget. That budget bounds each page walk; it is not a stable
snapshot size guarantee.

```elixir
paged_query =
  Aerospike.Query.new("test", "users")
  |> Aerospike.Query.where(Aerospike.Filter.equal("status", "active"))
  |> Aerospike.Query.max_records(100)

{:ok, first_page} = Aerospike.query_page(:aerospike, paged_query)

next_cursor =
  if first_page.cursor do
    Aerospike.Cursor.encode(first_page.cursor)
  end

if next_cursor do
  {:ok, next_page} =
    Aerospike.query_page(:aerospike, paged_query, cursor: next_cursor)

  next_page.records
end
```

Cursors resume partition progress. They are suitable for continuing a query
walk, but they are not snapshot tokens.

Scan pages use the same cursor shape:

```elixir
paged_scan =
  Aerospike.Scan.new("test", "events")
  |> Aerospike.Scan.max_records(100)

{:ok, first_page} = Aerospike.scan_page(:aerospike, paged_scan)
```

## Partition And Node Targeting

Use `Aerospike.PartitionFilter` for advanced partial scans or queries.

```elixir
scan =
  Aerospike.Scan.new("test", "events")
  |> Aerospike.Scan.partition_filter(Aerospike.PartitionFilter.by_range(0, 128))
  |> Aerospike.Scan.max_records(1_000)

{:ok, count} = Aerospike.scan_count(:aerospike, scan)
```

Helpers that support node targeting take `node: node_name` in `opts`.
Discover names with `Aerospike.node_names/1` or `Aerospike.nodes/1`.

```elixir
{:ok, [node_name | _]} = Aerospike.node_names(:aerospike)
{:ok, stream} = Aerospike.query_stream(:aerospike, query, node: node_name)
```

Node targeting is available for scan/query streams, counts, collected pages,
and background query jobs. Finalized aggregate queries do not support
`node: node_name` because they must consume all server partials needed for one
local result.
