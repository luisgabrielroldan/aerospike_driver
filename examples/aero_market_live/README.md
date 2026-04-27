# AeroMarketLive

A self-contained Phoenix LiveView application that generates synthetic stock-market
data, persists it to Aerospike using CDT map operations, and renders a real-time
dashboard — all without an external data feed, a JS build toolchain, or fixture
downloads.

## What it demonstrates

| Aerospike feature | Where it appears |
|---|---|
| CDT ordered map writes (`KEY_VALUE_ORDERED`) | `Generator` — every price tick |
| `map_put` with `CREATE_ONLY` write flag | `Generator` — prevents overwriting ticks |
| Sliding-window trim with `map_remove_by_index_range` | `Generator` — keeps last 120 ticks per key |
| `map_get_by_rank` for server-side extrema | `MarketReader.fetch_day/3` — max/min price |
| `map_get_by_index` for first/last values | `MarketReader.fetch_day/3` — open/close price |
| `map_size` | `MarketReader.fetch_day/3` — tick count |
| Expression reads (`Op.Exp.read`) | `MarketReader.fetch_day/3` — `is_positive` flag |
| Multi-op `operate/4` (9 ops, single round-trip) | `MarketReader.fetch_day/3` |
| Repo-style application boundary | `AeroMarketLive.Aero` — supervised `Aerospike.Repo` |
| Leaderboard write + rank read across tickers | `MarketReader.write_overall_diff/4` + `fetch_leaderboard/3` |

## Architecture

```
Generator (GenServer)
  ├── writes CDT map ticks ──────────────────► Aerospike (test.timeseries)
  └── broadcasts {:tick, sym, day_ms, idx, price} ─► Phoenix.PubSub "ticks"

DashboardLive (LiveView)
  ├── subscribes to "ticks" ◄──────────────── Phoenix.PubSub
  │     └── updates in-memory ring buffer + sparkline SVG (no Aerospike I/O)
  └── sends :refresh_summary every 1 s
        └── MarketReader.fetch_day/3 ────────► Aerospike (9-op operate)
              MarketReader.write_overall_diff ► Aerospike (overallsummary)
              MarketReader.fetch_leaderboard ─► Aerospike (rank reads)
```

The two update paths are intentionally independent:

- **High-frequency ticks** flow through PubSub directly into sparklines without
  touching Aerospike. The LiveView ring buffer holds the last 120 prices per
  symbol, matching the sliding window kept in Aerospike.
- **Periodic summary** (every 1 s) calls `MarketReader` against Aerospike to
  derive day stats (open, close, max, min, avg, count) and the leaderboard.

## Data model

### `test.timeseries` — per-ticker, per-day records

Primary key: `"#{symbol}#{midnight_ms}"` (e.g. `"AAPL1744329600000"`)

| Bin | Type | Content |
|---|---|---|
| `stock` | CDT map (`KEY_VALUE_ORDERED`) | `minute_index → price` (sliding 120-tick window) |
| `sum` | float | Running total of all prices written today |

The window trim (`map_remove_by_index_range` with `trim_count`) removes the
oldest entry once the map exceeds 120 items, keeping memory bounded.

### `test.overallsummary` — leaderboard record

Primary key: `midnight_ms` (stable across refreshes within a day)

| Bin | Type | Content |
|---|---|---|
| `difference` | CDT map (`KEY_VALUE_ORDERED`) | `symbol → (close - open)` |

The leaderboard record is updated after each summary refresh by
`write_overall_diff/4`. `fetch_leaderboard/3` reads back the top-5 symbols by
net position using `map_get_by_rank` with `return_type: KEY` and `VALUE`.

## Synthetic price model

Six symbols ship by default (`TickerConfig.defaults/0`):

| Symbol | Initial price | Daily drift | Daily volatility |
|---|---|---|---|
| AAPL | $175 | 0.03% | 1.2% |
| MSFT | $415 | 0.04% | 1.3% |
| NVDA | $900 | 0.08% | 2.5% |
| TSLA | $180 | 0.02% | 2.8% |
| GOOG | $160 | 0.03% | 1.4% |
| AMZN | $185 | 0.04% | 1.6% |

Prices follow geometric Brownian motion using the Box-Muller transform. A
deterministic seed (`@rand_seed`) makes repeated runs produce the same price
trajectory. The simulated clock advances 12 minutes per wall-clock second,
covering the 390-minute US equity session in about 32 seconds.

## Prerequisites

Run Aerospike locally (from the workspace root):

```bash
docker compose up -d
```

Aerospike must be reachable at `localhost:3000`.

## Setup and run

```bash
cd examples/aero_market_live
mix deps.get
mix assets.setup   # copies phoenix.js and phoenix_live_view.js into priv/static
mix phx.server
```

Open http://localhost:4100.

`mix assets.setup` vendors the two JS files from the compiled deps — no Node.js
or npm required.

## Dashboard

The dashboard at `/` shows a live-updating table with one row per symbol:

- **Symbol** — ticker name
- **Price** — latest tick from PubSub (updates every ~1 s wall clock)
- **Change** — `(close - open) / open` as a percentage, green/red
- **Sparkline** — inline SVG polyline of the last 120 ticks
- **Max / Min / Avg** — server-computed from Aerospike rank ops, refreshed every 1 s

Below the table a **Top Performers** leaderboard ranks symbols by net dollar
change, read from the `overallsummary` record via `map_get_by_rank`.

Row backgrounds are coloured green (positive day) or red (negative day) using the
`is_positive` expression-read bin returned by `MarketReader.fetch_day/3`.
