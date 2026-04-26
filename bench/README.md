# Benchmark Suite Guide

This benchmark suite ports the useful, current benchmarks from `../aerospike_driver_old/bench/`
into the current driver repo so performance work can happen against the implementation that is
actually being built.

## Current Scope

The first pass intentionally covers only benchmarks that match the current driver's public API:

- L1 microbench: `bench/tests/micro/key_construction_bench.exs`
- L1 protocol codec microbench: `bench/tests/micro/protocol_message_bench.exs`
- L2 single-node E2E: `bench/tests/e2e/crud_baseline_bench.exs`
- L4 multi-node fan-out: `bench/tests/fanout/batch_get_bench.exs`

Not every legacy benchmark was copied. Some depend on behavior or internal surfaces that are
not equivalent in the current driver yet, so copying them now would create misleading comparisons.

## Start Here

From `aerospike_driver/`:

```bash
mix deps.get
mix bench --quick bench/tests/e2e/crud_baseline_bench.exs
```

## Prerequisites

Single-node benchmarks expect a local Aerospike node on `127.0.0.1:3000`.

```bash
docker compose up -d
```

Cluster fan-out benchmarks expect the long-lived three-node cluster described by the repo test
workflow and require `localhost:3000`, `localhost:3010`, and `localhost:3020`.

## Profiles

Profiles are defined in `bench/scenarios/default.exs`:

- `quick`
- `default`
- `full`

Use:

```bash
mix bench --quick
mix bench
mix bench --full
```

## Common Commands

Run all benchmarks:

```bash
mix bench
```

Run one benchmark:

```bash
mix bench bench/tests/micro/key_construction_bench.exs
mix bench bench/tests/micro/protocol_message_bench.exs
mix bench bench/tests/e2e/crud_baseline_bench.exs
mix bench bench/tests/fanout/batch_get_bench.exs
```

Clean results:

```bash
mix bench.clean
mix bench.clean --all
```

## Artifacts

Each run writes JSON artifacts under:

`bench/results/<run-id>/<benchmark-title>.json`

Some microbenchmarks batch tiny operations inside each Benchee sample to reduce scheduler and
timer noise. When an artifact contains `iterations_per_sample`, divide the reported average by
that value to estimate per-operation cost.

Use explicit run IDs when comparing before/after changes:

```bash
BENCH_RUN_ID=baseline mix bench --default bench/tests/e2e/crud_baseline_bench.exs
BENCH_RUN_ID=candidate mix bench --default bench/tests/e2e/crud_baseline_bench.exs
```
