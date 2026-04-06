# Expressions

Aerospike expressions evaluate predicates and computations **on the server**, before results are sent to your client. They serve two distinct purposes:

1. **Filter expressions** — attached to an operation's `filter:` option, they cause the server to silently discard non-matching records before the response is sent. Used on `get`, `exists`, `delete`, `put`, `batch_get`, scans, and queries.

2. **Expression operations** — executed inside `operate/4` as computation steps. They read a computed value into a synthetic bin or write a computed result to a real bin, all in a single round-trip.

Both share the same builder API: `Aerospike.Exp`.

## Building Expressions

Expressions are composed from typed constructor functions. Each function returns an `%Aerospike.Exp{}` struct containing the pre-encoded wire representation. Expressions build up a tree by passing `%Exp{}` values as arguments to other builders.

```elixir
alias Aerospike.Exp

# Integer comparison — both sides typed
expr = Exp.gt(Exp.int_bin("age"), Exp.int(21))

# Boolean AND — at least two elements
expr = Exp.and_([
  Exp.gte(Exp.int_bin("age"), Exp.val(18)),
  Exp.lt(Exp.int_bin("age"), Exp.val(65)),
  Exp.eq(Exp.str_bin("status"), Exp.val("active"))
])

# Negation
expr = Exp.not_(Exp.eq(Exp.str_bin("status"), Exp.val("banned")))
```

### Typed literal constructors

All values on the right-hand side of a comparison must be wrapped in a typed expression constructor:

| Constructor | Elixir type | Purpose |
|-------------|-------------|---------|
| `Exp.int(n)` | `integer()` | Integer literal |
| `Exp.float(f)` | `float()` | Float literal |
| `Exp.str(s)` | `binary()` | String literal |
| `Exp.bool(b)` | `boolean()` | Boolean literal |
| `Exp.blob(b)` | `binary()` | Raw binary literal (not UTF-8 string) |
| `Exp.nil_()` | — | Nil literal |

### `Exp.val/1` — type-inferring convenience

`Exp.val/1` infers the constructor from the Elixir term, following the same mapping as official Aerospike clients:

| Elixir term | Expression type |
|-------------|----------------|
| `integer()` | `Exp.int/1` |
| `float()` | `Exp.float/1` |
| `binary()` | `Exp.str/1` |
| `boolean()` | `Exp.bool/1` |
| `nil` | `Exp.nil_/0` |

**All binaries are treated as strings.** If you need blob (raw binary) semantics, use `Exp.blob/1` explicitly; `Exp.val/1` maps every binary to `Exp.str/1`.

```elixir
# These are equivalent
Exp.gt(Exp.int_bin("age"), Exp.int(21))
Exp.gt(Exp.int_bin("age"), Exp.val(21))
```

### Bin reads

Read the value of a specific bin from the current record:

```elixir
Exp.int_bin("age")      # integer bin
Exp.float_bin("score")  # float bin
Exp.str_bin("city")     # string bin
Exp.bool_bin("active")  # boolean bin
Exp.blob_bin("payload") # blob (raw binary) bin
Exp.geo_bin("location") # geo bin
```

### Record metadata

Access server-side record metadata without bin reads:

```elixir
Exp.ttl()          # remaining TTL in seconds
Exp.void_time()    # absolute expiration epoch timestamp
Exp.last_update()  # epoch timestamp of the last write
Exp.key_exists()   # true if a user key was stored with the record
Exp.set_name()     # the set name as a string
Exp.tombstone?()   # true when the record is a tombstone
Exp.record_size()  # storage size in bytes on the device
```

```elixir
# Filter records that expire within the next hour
Exp.lt(Exp.ttl(), Exp.int(3_600))

# Records updated since a specific epoch time
Exp.gte(Exp.last_update(), Exp.int(cutoff_ts))
```

### Comparisons

All comparison functions take two `%Exp{}` arguments:

```elixir
Exp.eq(left, right)   # left == right
Exp.ne(left, right)   # left != right
Exp.gt(left, right)   # left > right
Exp.gte(left, right)  # left >= right
Exp.lt(left, right)   # left < right
Exp.lte(left, right)  # left <= right
```

### Boolean composition

```elixir
# AND — requires at least two expressions
Exp.and_([
  Exp.gt(Exp.int_bin("age"), Exp.val(18)),
  Exp.eq(Exp.str_bin("country"), Exp.val("US"))
])

# OR — requires at least two expressions
Exp.or_([
  Exp.eq(Exp.str_bin("plan"), Exp.val("premium")),
  Exp.eq(Exp.str_bin("plan"), Exp.val("enterprise"))
])

# NOT — single expression
Exp.not_(Exp.eq(Exp.str_bin("status"), Exp.val("banned")))
```

> **Naming convention.** `Exp.and_/1`, `Exp.or_/1`, and `Exp.not_/1` use a trailing underscore because `and`, `or`, and `not` are Elixir reserved words and cannot be used as bare function names. The trailing underscore is the standard Elixir convention for avoiding keyword collisions.

## Filter Expressions

Attach an expression to any operation using the `filter:` option. The server evaluates the expression before returning a result — if the expression is false, the operation returns `{:error, %Aerospike.Error{code: :filtered_out}}`.

### Single-record CRUD

```elixir
alias Aerospike.{Exp, Key}

key = Aerospike.key("test", "users", "user:42")
only_adults = Exp.gte(Exp.int_bin("age"), Exp.int(18))

# Filtered get — returns {:error, %Aerospike.Error{code: :filtered_out}} if age < 18
{:ok, record} = Aerospike.get(:aero, key, filter: only_adults)

# Filtered exists check
{:ok, true} = Aerospike.exists(:aero, key, filter: only_adults)

# Filtered delete — only delete if the record matches the expression
{:ok, _} = Aerospike.delete(:aero, key, filter: only_adults)

# Filtered put — only write if the expression matches
{:ok, _} = Aerospike.put(:aero, key, %{"score" => 99}, filter: only_adults)
```

### Scans

`Scan.filter/2` attaches an expression to the scan. Multiple `filter/2` calls **AND** the expressions together — the server returns only records that satisfy all attached filters.

```elixir
alias Aerospike.{Exp, Scan}

active_adults =
  Exp.and_([
    Exp.gte(Exp.int_bin("age"), Exp.val(18)),
    Exp.eq(Exp.str_bin("status"), Exp.val("active"))
  ])

{:ok, records} =
  Scan.new("test", "users")
  |> Scan.filter(active_adults)
  |> Scan.max_records(10_000)
  |> then(&Aerospike.all(:aero, &1))
```

Multiple separate filter calls compose with AND:

```elixir
Scan.new("test", "users")
|> Scan.filter(Exp.gte(Exp.int_bin("age"), Exp.val(18)))
|> Scan.filter(Exp.eq(Exp.str_bin("country"), Exp.val("US")))
# Both must match — equivalent to Exp.and_([...])
```

### Queries (secondary index)

`Query.filter/2` works the same way as `Scan.filter/2`. It applies an additional expression filter on top of the secondary-index predicate from `Query.where/2`:

```elixir
alias Aerospike.{Exp, Filter, Query}

result =
  Query.new("test", "users")
  |> Query.where(Filter.range("age", 18, 65))
  |> Query.filter(Exp.eq(Exp.str_bin("country"), Exp.val("US")))
  |> then(&Aerospike.stream!(:aero, &1))
  |> Enum.to_list()
```

### `filter:` vs `Query.where/2`

These serve different purposes and are both valuable:

| | `Query.where/2` | `filter:` / `Query.filter/2` |
|---|---|---|
| Mechanism | Secondary index lookup | Expression evaluated per record |
| Requires index | Yes | No |
| Can replace | Never (index narrows candidates) | Can add on top of SI predicate |
| Result | Candidate set | Accepted records |

Use `Query.where/2` to narrow candidates with an index, then `filter:` to refine the result with logic the index cannot express (multi-condition, metadata comparisons, or cross-bin logic).

## Expression Operations

Expression operations execute inside `Aerospike.operate/4`. They compute a value **server-side** and either return the result in the response record's bins or write the result to a real bin — without an extra round-trip.

Use `Aerospike.Op.Exp`:

```elixir
alias Aerospike.{Exp, Op}

key = Aerospike.key("test", "stats", "user:42")

{:ok, record} =
  Aerospike.operate(:aero, key, [
    # Read: compute a value and return it as a synthetic bin
    Op.Exp.read("is_adult", Exp.gte(Exp.int_bin("age"), Exp.int(18))),

    # Write: compute a value and store it in a real bin
    Op.Exp.write("age_bucket",
      Exp.and_([
        Exp.gte(Exp.int_bin("age"), Exp.val(18)),
        Exp.lt(Exp.int_bin("age"), Exp.val(65))
      ]))
  ])

record.bins["is_adult"]   #=> true
```

### `Op.Exp.read/3`

Returns the expression result as a synthetic bin in the response record. The bin appears in `record.bins` under the name you provide but is never persisted.

```elixir
Op.Exp.read("ttl_seconds", Exp.ttl())
Op.Exp.read("over_limit", Exp.gt(Exp.int_bin("count"), Exp.int(1_000)))
```

### `Op.Exp.write/3`

Evaluates the expression and **stores the result in a real bin**. Subsequent reads of that bin return the stored value.

```elixir
Op.Exp.write("flagged", Exp.gt(Exp.int_bin("violations"), Exp.int(3)))
```

Both `read/3` and `write/3` accept an optional `flags:` integer keyword (default `0`).

## Runtime Composition

Because expressions are plain structs, you can build them dynamically at runtime — this is the Aerospike equivalent of `Ecto.Query.dynamic/2`:

```elixir
alias Aerospike.{Exp, Scan}

def build_user_scan(params) do
  filters =
    for {key, builder} <- [
          min_age: &Exp.gte(Exp.int_bin("age"), Exp.val(&1)),
          max_age: &Exp.lte(Exp.int_bin("age"), Exp.val(&1)),
          city:    &Exp.eq(Exp.str_bin("city"), Exp.val(&1)),
          active:  fn _ -> Exp.eq(Exp.str_bin("status"), Exp.val("active")) end
        ],
        value = params[key],
        not is_nil(value),
        do: builder.(value)

  expr =
    case filters do
      [] -> nil
      [single] -> single
      many -> Exp.and_(many)
    end

  scan = Scan.new("test", "users")

  if expr do
    Scan.filter(scan, expr)
  else
    scan
  end
end

# At the call site
scan = build_user_scan(%{min_age: 21, city: "Portland"})
{:ok, records} = Aerospike.all(:aero, Scan.max_records(scan, 5_000))
```

Since each expression builder returns a plain struct, you can also store partial expressions in module attributes, pass them as function arguments, or accumulate them in a `for` comprehension — no macro restrictions apply.

## Best Practices

- **Prefer typed constructors over `Exp.val/1`** when the type matters (especially for blobs — `Exp.val/1` maps all binaries to strings).
- **Combine server-side and client-side filters intentionally.** Push as much as possible into expressions to reduce data transfer; use `Stream.filter/2` client-side only for logic the expression system cannot express.
- **Use `Exp.and_/1` for clarity** when combining multiple conditions, rather than nesting repeated `filter/2` calls.
- **On queries, combine `where/2` with `filter:`** — the SI predicate narrows candidates cheaply; the expression applies fine-grained logic that the index cannot express.
- **Expression operations are not CDT operations.** `Op.Exp.write/3` writes the expression result as a scalar value. For list/map mutations, use `Op.List` and `Op.Map`.

## Next Steps

- [Queries and Scanning](queries-and-scanning.md) — `Scan.filter/2`, `Query.filter/2`, execution patterns
- [Batch Operations](batch-operations.md) — `filter:` option on `Batch.read/2` and `Batch.operate/3`
- `Aerospike.Exp` — full expression builder API reference
- `Aerospike.Op.Exp` — expression operation constructors
- `Aerospike.Filter` — secondary-index predicates for `Query.where/2`
