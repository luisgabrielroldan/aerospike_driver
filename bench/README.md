# Benchmark Suite Guide

This guide documents how to run the benchmark suite in `bench/` and how to compare
results across runs.

## Scope

The current suite provides one baseline benchmark per layer:

- L1 microbench: `bench/tests/micro/key_construction_bench.exs`
- L2 single-op E2E: `bench/tests/e2e/crud_baseline_bench.exs`
- L2 stream concurrency E2E: `bench/tests/e2e/stream_concurrency_bench.exs`
- L3 mixed workload: `bench/tests/workload/ru_80_20_bench.exs`
- L4 fan-out: `bench/tests/fanout/batch_get_bench.exs`

All benchmark entrypoints match `bench/tests/**/*_bench.exs` and can be executed with `mix bench`.

## Start Here

If you are new to the suite, start with one command and one file:

```bash
mix bench --quick bench/tests/e2e/crud_baseline_bench.exs
```

Then compare only the same benchmark title and scenario IDs across runs.

## Folder Map

- `bench/tests/micro/` - CPU-only local code paths (no Aerospike network I/O)
- `bench/tests/e2e/` - single operation network round-trip baselines (`put`, `get`)
- `bench/tests/e2e/stream_concurrency_bench.exs` - multi-node stream fan-out (`max_concurrent_nodes`) with TTFR + throughput reporting
- `bench/tests/workload/` - mixed read/write workload patterns
- `bench/tests/fanout/` - multi-key operations like batch get
- `bench/support/` - shared setup helpers (connection lifecycle, env parsing, metadata printing)
- `bench/scenarios/` - profile definitions (`quick`, `default`, `full`)
- `bench/results/` - JSON artifacts for before/after comparisons

## Prerequisites

From the workspace root, start a local Aerospike server:

```bash
docker compose up -d
```

Then run benchmark commands from `aerospike_driver/`.

Default benchmark connection assumptions:

- host: `127.0.0.1`
- port: `3000`
- namespace: `test`
- set names:
  - `bench_e2e` for L2 benchmarks
  - `bench_workload` for L3 benchmarks
  - `bench_fanout` for L4 benchmarks

If the server is unavailable, benchmark scripts fail fast with an explicit message.

`bench/tests/e2e/stream_concurrency_bench.exs` requires a multi-node cluster (`Aerospike.nodes/1` size >= 2).
The script fails early on single-node topology, and auto-skips invalid `max_concurrent_nodes` settings
(for example `2` on a two-node cluster).

## Run Profiles

Profiles are defined in `bench/scenarios/default.exs`:

- `quick`: fast smoke profile (`duration_s=3`, `warmup_s=1`, low concurrency)
- `default`: baseline profile (`duration_s=10`, `warmup_s=3`)
- `full`: extended profile (`duration_s=20`, `warmup_s=5`, wider concurrency)

Use CLI flags to pick a profile:

```bash
mix bench --quick
mix bench
mix bench --full
```

`BENCH_PROFILE` is still supported for CI/scripts.

## Common Commands

Run all benchmark scripts:

```bash
mix bench
```

Run a single benchmark script:

```bash
mix bench bench/tests/micro/key_construction_bench.exs
mix bench bench/tests/e2e/crud_baseline_bench.exs
mix bench --quick bench/tests/e2e/stream_concurrency_bench.exs
mix bench bench/tests/e2e/stream_concurrency_bench.exs
mix bench bench/tests/workload/ru_80_20_bench.exs
mix bench bench/tests/fanout/batch_get_bench.exs
```

Clean result directories:

```bash
# keep only the newest run directory
mix bench.clean

# remove every run directory under bench/results/
mix bench.clean --all
```

## Environment Overrides

Connection and dataset controls:

- `AEROSPIKE_HOST`
- `AEROSPIKE_PORT`
- `BENCH_NAMESPACE`
- `BENCH_SET` (shared override when a benchmark script reads one set value)

Profile/scenario controls:

- `BENCH_PROFILE` (`quick`, `default`, `full`)
- `BENCH_DURATION_S`
- `BENCH_WARMUP_S`
- `BENCH_CONCURRENCY` (comma-separated list, example: `1,4,16`)
- `BENCH_PAYLOAD_SIZES` (comma-separated list, example: `256,4096`)
- `BENCH_STREAM_RECORD_COUNT` (records inserted/drained per stream concurrency run)
- `BENCH_STREAM_TTFR_TRIALS` (TTFR sample count per setting)
- `BENCH_STREAM_MAX_CONCURRENT_NODES` (comma-separated list, allows `0`, example: `1,2,0`)

Artifact controls:

- `BENCH_RUN_ID` (explicit run grouping directory name under `bench/results/`)
- `AEROSPIKE_SERVER_VERSION` (metadata override when auto-detection is unavailable)

Workload-specific controls:

- `BENCH_READ_RATIO` (L3 RU workload read ratio, default `80`)
- `BENCH_WRITE_RATIO` (L3 RU workload write ratio, default `20`)

## Artifacts

Each run writes JSON artifacts under:

`bench/results/<run-id>/<benchmark-title>.json`

Stream concurrency runs also write a summary sidecar in the same run directory:

`bench/results/<run-id>/l2-stream-multi-node-concurrency-summary.json`

Each artifact includes:

- run ID and generation timestamp
- benchmark title and input names
- run metadata (git SHA, Elixir/OTP, host summary, profile config)
- per-scenario statistics for run time, memory usage, and reductions
- stream sidecar: per-setting TTFR (`min`, `median`, `p95`) and throughput (`records/sec`) summaries

## Stream Concurrency Interpretation

When comparing two or more `max_concurrent_nodes` settings, treat this as the minimum acceptance readout
for a healthy multi-node run:

- TTFR (`min`/`median`/`p95`) for `max_concurrent_nodes=0` is lower than `max_concurrent_nodes=1`
- throughput (`records/sec`) for `max_concurrent_nodes=0` is higher than `max_concurrent_nodes=1`
- comparisons are made within the same run profile (`quick` vs `quick`, `default` vs `default`)

## Comparing Before/After Runs

1. Run a baseline with an explicit run ID:

   ```bash
   BENCH_RUN_ID=baseline mix bench --default
   ```

2. Run a candidate with the same profile and environment:

   ```bash
   BENCH_RUN_ID=candidate mix bench --default
   ```

3. Compare files under `bench/results/baseline/` and `bench/results/candidate/`:
   - same benchmark title file names
   - same scenario IDs
   - throughput and latency-oriented fields in statistics
   - avoid cross-scenario comparisons (`put` vs `get`, small vs large payload)

4. Repeat each profile at least 2-3 times before concluding regressions or improvements.

## Variance Notes

- Run benchmarks on an otherwise idle machine when possible.
- Keep Aerospike server configuration and dataset assumptions constant across runs.
- Do not compare `quick` profile results against `default` or `full`; compare like-for-like.
