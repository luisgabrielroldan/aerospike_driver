# Demo

Examples for the Aerospike Elixir client. Each example is a module under `Demo.Examples.*` with a `run/0` function.

## Prerequisites

An Aerospike server running on `localhost:3000`. From the repository root:

```bash
docker compose up -d
```

## Running All Examples

```bash
cd aerospike_driver/examples/demo
mix deps.get
mix run run_all.exs
```

## Running a Single Example

```bash
cd aerospike_driver/examples/demo
mix deps.get
iex -S mix
```

```elixir
iex> Demo.Examples.Add.run()
iex> Demo.Examples.Batch.run()
```

## Examples

### Implemented

| Module | Description |
|--------|-------------|
| `Simple` | Full lifecycle: put, get, add, prepend, append, delete bin, exists, delete |
| `Put` | Write records with string and integer keys |
| `Get` | Read records by key |
| `PutGet` | Multi-bin and header-only reads |
| `Add` | Atomic integer add + operate add+get |
| `Append` | Atomic string append |
| `Prepend` | Atomic string prepend |
| `Operate` | Multi-operation: add + put + get in one round-trip |
| `Replace` | Replace mode and replace-only (fails if missing) |
| `Generation` | Optimistic concurrency (compare-and-swap) |
| `Expire` | Record TTL expiration |
| `Touch` | Refresh TTL without modifying data |
| `Batch` | Batch exists, reads, and header reads |
| `BatchOperate` | Heterogeneous batch: reads, writes, deletes, atomic operate |
| `ListMap` | List/Map CDT operations |
| `ScanSerial` | Eager scan with `all/2`, bin projection, namespace-wide scan |
| `ScanParallel` | Lazy streaming scan with `stream!/2`, pipelines, early termination |
| `ScanPaginate` | Paginated scan with `page/3` and cursor serialization |
| `CountSetObjects` | Count records in a set using scan-based `count/2` |
| `Expressions` | Server-side filter expressions: filter on get/scan, `Op.Exp.read`, `Op.Exp.write` |
| `QueryAggregate` | Secondary-index query with range filter, expression filter, client-side aggregation |
| `GeojsonQuery` | GeoJSON queries with geo2dsphere indexes, `geo_within`, `geo_contains` |
| `Udf` | UDF registration, execution (`apply_udf`), and removal |
| `TxnConcurrent` | Multi-record transactions: `transaction/2` wrapper, abort rollback, manual commit |

### Stubs (Require Special Infrastructure)

| Module | Reason |
|--------|--------|
| `TlsSecureConnection` | Requires TLS-configured server |
| `PkiAuth` | Requires Enterprise + PKI auth |
