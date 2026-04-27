# Expressions And Server Features

`Aerospike.Exp` builds server-side expressions. The same expression values can
be used by filter expressions, expression operate operations,
expression-backed secondary indexes, and Enterprise XDR filters.

## Filter Expressions

Unary commands accept one `%Aerospike.Exp{}` through the `:filter` option.

```elixir
adult =
  Aerospike.Exp.gte(
    Aerospike.Exp.int_bin("age"),
    Aerospike.Exp.int(18)
  )

{:ok, record} = Aerospike.get(:aerospike, key, :all, filter: adult)
```

Compose boolean expressions with `Exp.and_/1`, `Exp.or_/1`, and `Exp.not_/1`.
The trailing underscores avoid Elixir reserved words.

```elixir
eligible =
  Aerospike.Exp.and_([
    Aerospike.Exp.gte(Aerospike.Exp.int_bin("age"), Aerospike.Exp.int(18)),
    Aerospike.Exp.or_([
      Aerospike.Exp.eq(Aerospike.Exp.str_bin("status"), Aerospike.Exp.str("active")),
      Aerospike.Exp.eq(Aerospike.Exp.str_bin("status"), Aerospike.Exp.str("trial"))
    ]),
    Aerospike.Exp.not_(
      Aerospike.Exp.eq(Aerospike.Exp.bool_bin("blocked"), Aerospike.Exp.bool(true))
    )
  ])

{:ok, record} = Aerospike.get(:aerospike, key, :all, filter: eligible)
```

Metadata expressions read record metadata instead of bin values. The current
builder surface includes TTL, last-update time, stored-key presence, set name,
and storage size.

```elixir
recent_persistent_user =
  Aerospike.Exp.and_([
    Aerospike.Exp.gt(Aerospike.Exp.ttl(), Aerospike.Exp.int(0)),
    Aerospike.Exp.gt(Aerospike.Exp.last_update(), Aerospike.Exp.int(0)),
    Aerospike.Exp.key_exists(),
    Aerospike.Exp.eq(Aerospike.Exp.set_name(), Aerospike.Exp.str("users")),
    Aerospike.Exp.lt(Aerospike.Exp.record_size(), Aerospike.Exp.int(16_384))
  ])
```

Scans and queries use builder-level filters:

```elixir
scan =
  Aerospike.Scan.new("test", "users")
  |> Aerospike.Scan.filter(adult)

query =
  Aerospike.Query.new("test", "users")
  |> Aerospike.Query.where(Aerospike.Filter.equal("status", "active"))
  |> Aerospike.Query.filter(adult)
```

Multiple scan or query filters are combined server-side with boolean AND when
encoded. Expressions can currently be used in these places:

- Command filters through options like `filter: exp` on supported record
  commands.
- Scan and query filters through `Aerospike.Scan.filter/2` and
  `Aerospike.Query.filter/2`.
- Expression operate operations through `Aerospike.Op.Exp`.
- Expression-backed secondary indexes through
  `Aerospike.create_expression_index/5`.
- Enterprise XDR filters through `Aerospike.set_xdr_filter/4`.

## Expression Operations

Expression operate helpers live in `Aerospike.Op.Exp`.

```elixir
{:ok, record} =
  Aerospike.operate(:aerospike, key, [
    Aerospike.Op.Exp.read("projected_score", Aerospike.Exp.int_bin("score")),
    Aerospike.Op.Exp.write("score_copy", Aerospike.Exp.int_bin("score"))
  ])

record.bins["projected_score"]
```

Expression operation support uses the shipped `Aerospike.Exp` and
`Aerospike.Op.Exp` builders. CDT, bit, and HyperLogLog expression helpers live
under `Aerospike.Exp.List`, `Aerospike.Exp.Map`, `Aerospike.Exp.Bit`, and
`Aerospike.Exp.HLL` so they stay separate from ordinary `operate/4` CDT
operation builders.

## Expression-Backed Indexes

Expression-backed secondary indexes use an expression as the source instead
of a bin name. Servers older than Aerospike 8.1 reject creation before the
create command is sent.

```elixir
{:ok, task} =
  Aerospike.create_expression_index(:aerospike, "test", "users",
    Aerospike.Exp.int_bin("age"),
    name: "users_age_expr_idx",
    type: :numeric
  )

:ok = Aerospike.IndexTask.wait(task, timeout: 30_000, poll_interval: 200)

query =
  Aerospike.Query.new("test", "users")
  |> Aerospike.Query.where(
    Aerospike.Filter.range("age", 18, 40)
    |> Aerospike.Filter.using_index("users_age_expr_idx")
  )
  |> Aerospike.Query.max_records(100)

{:ok, records} = Aerospike.query_all(:aerospike, query)
```

Use `Filter.using_index/2` when the query should target a named index.

## XDR Filters

Enterprise XDR filters also use `%Aerospike.Exp{}` values, but they require an
Enterprise server with XDR configured. The checked-in local profiles exercise
security and transaction surfaces; XDR filter validation is opt-in against a
separately configured XDR topology.

```elixir
filter =
  Aerospike.Exp.eq(
    Aerospike.Exp.int_bin("replicate"),
    Aerospike.Exp.int(1)
  )

:ok = Aerospike.set_xdr_filter(:aerospike, "dc-west", "test", filter)
:ok = Aerospike.set_xdr_filter(:aerospike, "dc-west", "test", nil)
```

Passing `nil` clears the existing filter. Datacenter and namespace values must
be non-empty info-command identifiers.
