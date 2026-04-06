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
| `ListMap` | List/Map CDT operations |

### Stubs (API Not Yet Available)

| Module | Reason |
|--------|--------|
| `Expressions` | Exp builder API not yet available |
| `ScanSerial` | Scan API not yet available |
| `ScanParallel` | Scan API not yet available |
| `ScanPaginate` | Scan/partition API not yet available |
| `QueryAggregate` | Query/aggregation API not yet available |
| `GeojsonQuery` | Query/secondary index API not yet available |
| `Udf` | UDF register/execute API not yet available |
| `CountSetObjects` | Info command API not yet available |
| `TlsSecureConnection` | Requires TLS-configured server |
| `PkiAuth` | Requires Enterprise + PKI auth |
| `TxnConcurrent` | Transaction API not yet available |
