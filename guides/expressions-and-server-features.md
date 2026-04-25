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
encoded.

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

Expression operation support is limited to the shipped `Aerospike.Exp` and
`Aerospike.Op.Exp` builders.

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
