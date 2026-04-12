# Queries and Scanning

This guide covers full-set **scans**, secondary-index **queries**, and how to execute them with bounded collection, lazy streams, counts, and cursor-based pagination.

## Overview

A **scan** walks records in a namespace or set. It does not require a secondary index; the server iterates partitions and returns matching records.

A **query** targets a namespace and set using a **secondary index (SI)** predicate. The index narrows the candidate records; optional expression filters can refine results further on the server.

Both flows use the same mental model as `Ecto.Query`: build a plain-data description ([`%Aerospike.Scan{}`](Aerospike.Scan.html) or [`%Aerospike.Query{}`](Aerospike.Query.html)), then pass it to an execution function on [`Aerospike`](Aerospike.html) with the connection name as the first argument. No I/O happens until execution.

## Scans

Start with [`Aerospike.Scan.new/1`](Aerospike.Scan.html#new/1) for a namespace-wide scan (no set filter), or [`Aerospike.Scan.new/2`](Aerospike.Scan.html#new/2) for a single set.

```elixir
alias Aerospike.Scan

namespace_scan = Scan.new("test")

set_scan =
  Scan.new("test", "users")
  |> Scan.select(["name", "age", "city"])
```

### Bin projection

[`Scan.select/2`](Aerospike.Scan.html#select/2) limits which bins are read. Omit it (or pass `[]` depending on builder defaults) to request all bins for each record; with an explicit list, only those bins are returned.

### Expression filters

[`Scan.filter/2`](Aerospike.Scan.html#filter/2) attaches a server-side filter expression (`%Aerospike.Exp{}`). Multiple calls append filters that are **AND**-ed at execution time.

The full expression builder (`Exp.gt/2`, `Exp.int_bin/1`, and similar) is not shipped yet. Until then, wrap pre-encoded wire bytes with [`Aerospike.Exp.from_wire/1`](Aerospike.Exp.html#from_wire/1) (same approach as batch `:filter` in [Batch Operations](batch-operations.md)):

```elixir
# Replace <<...>> with real encoded expression bytes from your tooling or captures.
encoded = <<0x01, 0x02>>

Scan.new("test", "users")
|> Scan.filter(Aerospike.Exp.from_wire(encoded))
```

### Throttling and limits

- [`Scan.records_per_second/2`](Aerospike.Scan.html#records_per_second/2) sets a server-side throttle. Use `0` for no throttle.
- [`Scan.max_records/2`](Aerospike.Scan.html#max_records/2) caps how many records the operation will return. It is **required** for [`Aerospike.all/3`](Aerospike.html#all/3) and [`Aerospike.page/3`](Aerospike.html#page/3) (see below). For unbounded iteration, use [`Aerospike.stream!/3`](Aerospike.html#stream!/3) instead of [`Aerospike.all/3`](Aerospike.html#all/3).

### Partition filters

Attach [`Aerospike.PartitionFilter`](Aerospike.PartitionFilter.html) values with [`Scan.partition_filter/2`](Aerospike.Scan.html#partition_filter/2) (see [Partition filters](#partition-filters)).

## Queries (secondary index)

Queries always need a namespace and a non-empty set: [`Aerospike.Query.new/2`](Aerospike.Query.html#new/2).

Execution uses the wire query path, which **requires** a secondary-index predicate from [`Query.where/2`](Aerospike.Query.html#where/2). Each new `where/2` **replaces** the previous SI filter (only one predicate is kept).

```elixir
alias Aerospike.{Filter, Query}

query =
  Query.new("test", "users")
  |> Query.where(Filter.range("age", 18, 65))
  |> Query.select(["name", "age", "city"])
```

### SI predicates: `Filter`

Common constructors:

- [`Filter.range/3`](Aerospike.Filter.html#range/3) — inclusive numeric range on a bin (int64 endpoints).
- [`Filter.equal/2`](Aerospike.Filter.html#equal/2) — equality on an integer or UTF-8 string bin.

Other helpers ([`Filter.contains/3`](Aerospike.Filter.html#contains/3) for CDT indexes, [`Filter.geo_within/2`](Aerospike.Filter.html#geo_within/2), [`Filter.geo_contains/2`](Aerospike.Filter.html#geo_contains/2)) build SI predicates the same way; pass the result to [`Query.where/2`](Aerospike.Query.html#where/2).

### Geospatial filters with `Aerospike.Geo`

[`Aerospike.Geo`](Aerospike.Geo.html) provides typed geometry builders:

- [`Geo.point/2`](Aerospike.Geo.html#point/2) for GeoJSON `Point`
- [`Geo.polygon/1`](Aerospike.Geo.html#polygon/1) for GeoJSON `Polygon`
- [`Geo.circle/3`](Aerospike.Geo.html#circle/3) for Aerospike `AeroCircle`

Simple geo query example:

```elixir
alias Aerospike.{Filter, Geo, Query}

{:ok, records} =
  Query.new("test", "locations")
  |> Query.where(Filter.geo_within("coords", Geo.circle(-122.5, 45.5, 5_000)))
  |> Query.max_records(100)
  |> then(&MyApp.Repo.all(&1))
```

Use them directly with [`Filter.geo_within/2`](Aerospike.Filter.html#geo_within/2) and
[`Filter.geo_contains/2`](Aerospike.Filter.html#geo_contains/2):

```elixir
alias Aerospike.{Filter, Geo, Query}

region =
  Geo.polygon([
    [{-122.6, 45.4}, {-122.3, 45.4}, {-122.3, 45.7}, {-122.6, 45.7}, {-122.6, 45.4}]
  ])

query =
  Query.new("test", "locations")
  |> Query.where(Filter.geo_within("coords", region))
  |> Query.max_records(1_000)
```

Backward-compatible raw GeoJSON strings still work:

```elixir
region_json = "{\"type\":\"AeroCircle\",\"coordinates\":[[-122.5,45.5],5000]}"

Query.new("test", "locations")
|> Query.where(Filter.geo_within("coords", region_json))
|> Query.max_records(1_000)
```

### `where` vs `filter`

- **[`Query.where/2`](Aerospike.Query.html#where/2)** — secondary-index lookup. The server uses the index to find candidate records.
- **[`Query.filter/2`](Aerospike.Query.html#filter/2)** — expression filter evaluated in addition to the SI predicate. Like [`Scan.filter/2`](Aerospike.Scan.html#filter/2), repeated `filter/2` calls are **AND**-ed. Use [`Aerospike.Exp.from_wire/1`](Aerospike.Exp.html#from_wire/1) until the expression builder is available.

```elixir
encoded = <<0x03, 0x04>>

Query.new("test", "users")
|> Query.where(Filter.equal("country", "US"))
|> Query.filter(Aerospike.Exp.from_wire(encoded))
```

The SI predicate does **not** bound memory use by itself: a wide range can still match many records. Use `max_records` ([`Scan.max_records/2`](Aerospike.Scan.html#max_records/2) or [`Query.max_records/2`](Aerospike.Query.html#max_records/2)), streaming, pagination, or [`count/3`](Aerospike.html#count/3) intentionally.

## Execution

For application code, execute the scan or query through `MyApp.Repo` and pass
optional keywords such as `:timeout`, `:pool_checkout_timeout`, and `:replica`
(merged with `defaults: [scan: ...]` or `defaults: [query: ...]` where
configured).

### `Aerospike.all/3` and `all!/3`

[`all/3`](Aerospike.html#all/3) eagerly collects matching records into a list. **`max_records` must be set** on the builder ([`Scan.max_records/2`](Aerospike.Scan.html#max_records/2) or [`Query.max_records/2`](Aerospike.Query.html#max_records/2)); otherwise you get `{:error, %Aerospike.Error{code: :max_records_required}}`.

```elixir
{:ok, records} =
  MyApp.Repo.all(Scan.new("test", "users") |> Scan.max_records(10_000))

records = MyApp.Repo.all!(Scan.new("test", "users") |> Scan.max_records(10_000))
```

### `Aerospike.stream!/3`

[`stream!/3`](Aerospike.html#stream!/3) returns a lazy `Stream` of `%Aerospike.Record{}` structs. **No `max_records` ([`Scan.max_records/2`](Aerospike.Scan.html#max_records/2) or [`Query.max_records/2`](Aerospike.Query.html#max_records/2)) is required** for streaming; use it for large or unknown result sizes.

The `!` means consumption can raise `Aerospike.Error` on server or network failure mid-stream.
By default, `stream!/3` fans out across all nodes concurrently. Use
`max_concurrent_nodes: 1` to force sequential node processing, or set a
larger positive bound to cap concurrent node streams.

```elixir
MyApp.Repo.stream!(Scan.new("test", "users"))
|> Stream.filter(fn r -> r.bins["age"] > 21 end)
|> Enum.take(100)
```

### `Aerospike.count/3` and `count!/3`

[`count/3`](Aerospike.html#count/3) issues a server-side scan/query that omits bin payloads and returns a total count. Prefer it over [`all/3`](Aerospike.html#all/3) or streaming when you only need cardinality.

```elixir
{:ok, n} = MyApp.Repo.count(Scan.new("test", "users"))
n = MyApp.Repo.count!(Query.new("test", "users") |> Query.where(Filter.equal("active", 1)))
```

### Bang variants

[`all!/3`](Aerospike.html#all!/3), [`count!/3`](Aerospike.html#count!/3), and [`page!/3`](Aerospike.html#page!/3) unwrap `{:ok, _}` and raise `Aerospike.Error` on failure, consistent with CRUD bang functions.

## Streaming

[`stream!/3`](Aerospike.html#stream!/3) is implemented with `Stream.resource/3`: it checks out a connection from the pool, reads scan/query frames as the consumer pulls, then runs cleanup when the stream ends.

- **Early termination** (for example `Enum.take/2`) triggers cleanup that **closes** that connection instead of returning it to the pool, because the socket may be mid-operation. The pool will open a fresh connection when needed.
- **Normal completion** returns the connection to the pool.

You can compose standard `Stream` functions (`Stream.map/2`, `Stream.filter/2`, `Stream.chunk_every/2`, etc.) on the enumerable returned by `stream!/3`.

### Errors

Wrap consumption in `try/rescue` if you need to recover or log mid-stream failures:

```elixir
try do
  MyApp.Repo.stream!(Scan.new("test", "users"))
  |> Enum.to_list()
rescue
  e in Aerospike.Error ->
    Logger.error("scan stopped: #{Exception.message(e)}")
    []
end
```

### Connection pressure

Each active [`stream!/3`](Aerospike.html#stream!/3) holds **one pool connection per node** for the
stream's entire lifetime — from the first `Enum` call through completion
or early halt. With `pool_size: 4` and 4 concurrent streams to the same
node, all other operations (CRUD, batch) will block waiting for a pool
checkout.

Recommendations:

- Use a dedicated [`Aerospike.start_link/1`](Aerospike.html#start_link/1) with a larger `pool_size` for
  streaming workloads.
- Limit concurrent streams to well below your `pool_size`.
- Prefer shorter scans with [`records_per_second/2`](Aerospike.Scan.html#records_per_second/2) when possible.

## Pagination

[`Aerospike.page/3`](Aerospike.html#page/3) returns `{:ok, %Aerospike.Page{}}`. Fields:

| Field | Meaning |
|-------|---------|
| `records` | Records for this page |
| `cursor` | Opaque `%Aerospike.Cursor{}` to pass to the next `page/3`, or `nil` when finished |
| `done?` | `true` when no further pages remain |

[`page/3`](Aerospike.html#page/3) requires `max_records` on the scan or query ([`Scan.max_records/2`](Aerospike.Scan.html#max_records/2) or [`Query.max_records/2`](Aerospike.Query.html#max_records/2)), the same as [`all/3`](Aerospike.html#all/3). Pass the previous page’s cursor:

```elixir
{:ok, page} = MyApp.Repo.page(scan)
{:ok, page2} = MyApp.Repo.page(scan, cursor: page.cursor)
```

`cursor:` may be a `%Aerospike.Cursor{}` or the Base64 string produced by [`Cursor.encode/1`](Aerospike.Cursor.html#encode/1) (the client decodes binary cursor tokens internally).

### Multi-page example

The following collects every record from a paginated set scan by threading the cursor until `done?` is true:

```elixir
defmodule UserScanPages do
  alias Aerospike.{PartitionFilter, Scan}

  @doc """
  Fetches all records from \"test\"/\"users\" using fixed-size pages.
  """
  def fetch_all do
    scan =
      Scan.new("test", "users")
      |> Scan.partition_filter(PartitionFilter.all())
      |> Scan.max_records(500)

    collect_pages(scan, nil, [])
  end

  defp collect_pages(scan, cursor, acc) do
    {:ok, page} =
      if cursor do
        MyApp.Repo.page(scan, cursor: cursor)
      else
        MyApp.Repo.page(scan)
      end

    acc2 = acc ++ page.records

    cond do
      page.done? -> {:ok, acc2}
      page.cursor == nil -> {:ok, acc2}
      true -> collect_pages(scan, page.cursor, acc2)
    end
  end
end
```

### Cursor serialization for HTTP APIs

Serialize cursors for query strings or JSON with [`Aerospike.Cursor.encode/1`](Aerospike.Cursor.html#encode/1) and restore with [`Cursor.decode/1`](Aerospike.Cursor.html#decode/1). You can pass the encoded string directly to [`page/3`](Aerospike.html#page/3) as `cursor:`; the client validates and decodes it.

```elixir
alias Aerospike.Cursor

{:ok, page} = MyApp.Repo.page(scan)
token = Cursor.encode(page.cursor)

# On a later request, after validating user input:
{:ok, _cursor} = Cursor.decode(token)
{:ok, next} = MyApp.Repo.page(scan, cursor: token)
```

Treat cursor strings as opaque capability tokens: verify authorization before resuming a scan or query on behalf of a client.

## Partition filters

[`Aerospike.PartitionFilter`](Aerospike.PartitionFilter.html) describes which of the 4_096 partitions participate:

- [`PartitionFilter.all/0`](Aerospike.PartitionFilter.html#all/0) — full partition range (explicit form; `nil` partition filter on the builder also means a full fan-out).
- [`PartitionFilter.by_id/1`](Aerospike.PartitionFilter.html#by_id/1) — single partition id `0..4095`.
- [`PartitionFilter.by_range/2`](Aerospike.PartitionFilter.html#by_range/2) — contiguous range starting at `begin` with length `count`.

```elixir
alias Aerospike.{PartitionFilter, Scan}

Scan.new("test", "users")
|> Scan.partition_filter(PartitionFilter.by_range(0, 1024))
```

[`PartitionFilter.by_digest/1`](Aerospike.PartitionFilter.html#by_digest/1) is available for advanced resume scenarios (20-byte record digest); it is mainly useful when combining digest-based routing with server pagination behavior.

Compose partition filters on queries the same way:

```elixir
Query.new("test", "users")
|> Query.where(Aerospike.Filter.range("age", 0, 120))
|> Query.partition_filter(PartitionFilter.all())
|> Query.max_records(200)
```

## Short vs long queries

For secondary-index queries, the client may set a **short-query** wire hint when each node’s record budget for that request is a **positive** integer **not above 100_000**. That aligns with common Aerospike client behavior: small, bounded per-node work can be optimized on the server.

When budgets are larger, or when per-node caps are zero (typical for unbounded `stream!/3` fan-out), that hint is not applied and the server treats the work as a longer-running query. This affects server scheduling and resource usage, not your Elixir data shapes.

Scans use the scan path; query-specific short-query hints apply to `Query` execution only.

## Known limitations

- **`count/3` still has to walk matching records.** It skips bin data, but
  the server still sends one lightweight result per record and the client
  tallies them.
- **For plain "how many records are in this set?" checks, use info stats when possible.**
  The raw info command (`sets/<ns>/<set>`) is usually faster because it
  returns pre-aggregated set metadata instead of streaming per-record results.
  Use [`Aerospike.info/3`](Aerospike.html#info/3) (or
  [`Aerospike.info_node/4`](Aerospike.html#info_node/4)) to issue raw info
  commands from this client.

## Best practices

- **Always set `max_records` ([`Scan.max_records/2`](Aerospike.Scan.html#max_records/2) or [`Query.max_records/2`](Aerospike.Query.html#max_records/2)) for [`all/3`](Aerospike.html#all/3) and [`page/3`](Aerospike.html#page/3).** If you forget, you get `{:error, %Aerospike.Error{code: :max_records_required}}`.
- **Use [`stream!/3`](Aerospike.html#stream!/3) for open-ended iteration** instead of raising `max_records` ([`Scan.max_records/2`](Aerospike.Scan.html#max_records/2) or [`Query.max_records/2`](Aerospike.Query.html#max_records/2)) to an arbitrary huge number.
- **Use [`count/3`](Aerospike.html#count/3) when you only need a total**, not a full record list.
- **Mind `pool_size` and concurrency** when many streams or long scans run in parallel.
- **Use [`records_per_second/2`](Aerospike.Scan.html#records_per_second/2)** on large scans to reduce cluster load.
- **Distinguish SI predicates from expression filters** on queries: index with [`Query.where/2`](Aerospike.Query.html#where/2), extra server-side logic with [`Query.filter/2`](Aerospike.Query.html#filter/2) once expression bytes are available.

## Next steps

- [Batch Operations](batch-operations.md) — applying `Exp.from_wire/1` in batch `:filter` options
- [Getting Started](getting-started.md) — connection setup and CRUD
- [`Aerospike.Scan`](Aerospike.Scan.html), [`Aerospike.Query`](Aerospike.Query.html), [`Aerospike.Filter`](Aerospike.Filter.html) — API reference
- [`Aerospike.Page`](Aerospike.Page.html), [`Aerospike.Cursor`](Aerospike.Cursor.html), [`Aerospike.PartitionFilter`](Aerospike.PartitionFilter.html) — pagination and routing structs
