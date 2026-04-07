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
| `Delete` | Delete records, idempotent deletes, delete with filter expressions |
| `Exists` | Single-key and batch exists checks |
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
| `BatchUdf` | Batch UDF execution with `Batch.udf/5` in `batch_operate` |
| `ListMap` | List/Map CDT round-trip: strings, complex types, combined structures |
| `ListOps` | List CDT operations: append, insert, pop, remove, sort, trim, rank, increment |
| `MapOps` | Map CDT operations: put, get_by_key, increment, remove, get_by_rank_range |
| `BitOps` | Bitwise CDT operations: set, count, get_int, set_int, OR |
| `HllOps` | HyperLogLog operations: add, cardinality, union, intersection, describe |
| `NestedCdt` | Nested CDT operations with `Ctx` context paths (map→map, map→list, deep nesting) |
| `ScanSerial` | Eager scan with `all/2`, bin projection, namespace-wide scan |
| `ScanParallel` | Lazy streaming scan with `stream!/2`, pipelines, early termination |
| `ScanPaginate` | Paginated scan with `page/3` and cursor serialization |
| `CountSetObjects` | Count records in a set using scan-based `count/2` |
| `Expressions` | Server-side filter expressions: filter on get/scan, `Op.Exp.read`, `Op.Exp.write` |
| `SecondaryIndex` | Index lifecycle: `create_index`, `IndexTask` polling, `Filter.equal` query, `drop_index` |
| `QueryAggregate` | Secondary-index query with range filter, expression filter, client-side aggregation |
| `QueryPaginate` | Paginated queries with `page/3` and streaming queries with `stream!/2` |
| `GeojsonQuery` | GeoJSON queries with geo2dsphere indexes, `geo_within`, `geo_contains` |
| `PartitionFilter` | Partition-targeted scans: `by_id`, `by_range` for parallel fan-out |
| `Udf` | UDF registration, execution (`apply_udf`), and removal |
| `Info` | Cluster info: nodes, node names, namespaces, server build version |
| `Truncate` | Truncate a set and verify record count drops to zero |
| `TxnConcurrent` | Multi-record transactions: `transaction/2` wrapper, abort rollback, manual commit |

### Stubs (Require Special Infrastructure)

| Module | Reason |
|--------|--------|
| `TlsSecureConnection` | Requires TLS-configured server |
| `PkiAuth` | Requires Enterprise + PKI auth |
