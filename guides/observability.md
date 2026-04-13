# Observability and Runtime Operations

This guide covers the Phase 5 operational APIs: client-managed metrics and
runtime stats, connection-pool warmup, and XDR filter management.

These APIs are distinct from the `[:aerospike, :command]` Telemetry events that
the client already emits. Telemetry is always active and requires no
configuration. The metrics APIs in this guide toggle an additional layer of
client-owned counters that track command outcomes and connection events over
time.

## Client-Managed Metrics

Metrics collection is disabled by default. Enable it explicitly after starting
your connection:

```elixir
:ok = Aerospike.enable_metrics(:aero)
true = Aerospike.metrics_enabled?(:aero)
```

Disable metrics when you no longer need the counters:

```elixir
:ok = Aerospike.disable_metrics(:aero)
false = Aerospike.metrics_enabled?(:aero)
```

To reset previously collected counters when re-enabling:

```elixir
:ok = Aerospike.enable_metrics(:aero, reset: true)
```

When metrics are disabled, command counters are not incremented, but cluster and
connection counters remain available through `stats/1` regardless of the
toggle.

### Repo API

```elixir
:ok = MyApp.Repo.enable_metrics()
:ok = MyApp.Repo.enable_metrics(reset: true)
true = MyApp.Repo.metrics_enabled?()
:ok = MyApp.Repo.disable_metrics()
```

## Runtime Stats

`stats/1` returns a snapshot of runtime state. The shape is stable and always
returns the same top-level keys, even when the cluster is not yet ready:

```elixir
stats = Aerospike.stats(:aero)
```

Top-level fields:

| Field | Description |
|-------|-------------|
| `:metrics_enabled` | Whether client-managed metrics are currently enabled |
| `:cluster_ready` | Whether the cluster has completed at least one successful tend |
| `:nodes_total` | Number of nodes currently tracked in the cluster registry |
| `:nodes_active` | Number of nodes currently marked active |
| `:open_connections` | Aggregate open-connection count across all nodes |
| `:commands_total` | Total commands executed since metrics were last enabled |
| `:commands_ok` | Commands that completed without error |
| `:commands_error` | Commands that returned an error |
| `:cluster` | Nested cluster counters and config (see below) |
| `:nodes` | Per-node stats keyed by node name (see below) |

The `:cluster` section:

```elixir
stats.cluster
# %{
#   config: %{pool_size: 10, tend_interval_ms: 1000},
#   tends: %{total: 42, successful: 41, failed: 1},
#   nodes: %{added: 3, removed: 0},
#   partition_map_updates: 41,
#   connections: %{open: 20, attempts: 30, successful: 30, ...},
#   commands: %{total: 500, ok: 495, error: 5, by_command: %{...}, by_error_code: %{...}}
# }
```

The `:nodes` section maps each known node name to per-node connection and
command counters:

```elixir
stats.nodes["BB90000000A4202"
# %{
#   host: "127.0.0.1",
#   port: 3000,
#   active: true,
#   connections: %{open: 10, attempts: 15, successful: 15, ...},
#   commands: %{total: 200, ok: 198, error: 2, by_command: %{...}, by_error_code: %{...}}
# }
```

Only nodes currently present in the cluster registry appear in `:nodes`.
Counters for removed nodes roll into the cluster-level totals but do not appear
as standalone node entries.

Command metrics (`by_command` and `by_error_code`) are populated only when
metrics are enabled. They remain empty maps when metrics are disabled.

### Repo API

```elixir
stats = MyApp.Repo.stats()
```

## Connection Pool Warmup

Warmup exercises the current discovered node pools to reduce first-request
latency. It performs real `NimblePool` checkout/checkin cycles against each
active node, verifying or refreshing pooled connections in the current
eager-pool design:

```elixir
{:ok, result} = Aerospike.warm_up(:aero)
```

Warmup options:

| Option | Default | Description |
|--------|---------|-------------|
| `:count` | `0` | Connections to warm per node. `0` means the full configured pool size. |
| `:pool_checkout_timeout` | `5000` | Checkout timeout in milliseconds. |

`count: 0` and counts above the configured pool size are both clamped to the
per-node pool size.

The returned map exposes aggregate and per-node outcomes:

```elixir
{:ok, %{
  status: :ok,           # :ok | :partial | :error
  requested_per_node: 10,
  total_requested: 30,
  total_warmed: 30,
  nodes_ok: 3,
  nodes_partial: 0,
  nodes_error: 0,
  nodes: %{
    "BB90000000A4202" => %{requested: 10, warmed: 10, status: :ok},
    "BB90000000A4203" => %{requested: 10, warmed: 10, status: :ok},
    "BB90000000A4204" => %{requested: 10, warmed: 10, status: :ok}
  }
}} = Aerospike.warm_up(:aero)
```

Partial success is represented explicitly: if some nodes succeed and others
fail, `:status` is `:partial` and the failing nodes include an `%Aerospike.Error{}`
in their per-node entry.

Warmup targets the nodes discovered by the current cluster topology, not the
original seed list. If the cluster has not finished its initial tend, warmup
returns an error immediately rather than hanging.

### Repo API

```elixir
{:ok, result} = MyApp.Repo.warm_up()
{:ok, result} = MyApp.Repo.warm_up(count: 5)
```

## XDR Filter Management

Cross-Datacenter Replication (XDR) filter management requires Aerospike
Enterprise Edition with XDR configured. The `set_xdr_filter/4` API sends the
`xdr-set-filter` info command to one node; that node distributes the change
cluster-wide.

Set an expression filter for a datacenter/namespace pair:

```elixir
import Aerospike.Exp

filter = and_(eq(int_bin("status"), int_val(1)), gt(int_bin("score"), int_val(50)))

:ok = Aerospike.set_xdr_filter(:aero, "dc-west", "test", filter)
```

Clear the current server-side filter by passing `nil`:

```elixir
:ok = Aerospike.set_xdr_filter(:aero, "dc-west", "test", nil)
```

Validation is local and explicit before any command is sent:

- `datacenter` and `namespace` must be non-empty strings without info-command
  delimiters (`;`, `\t`, `\n`)
- `filter` must be `nil` or a non-empty `%Aerospike.Exp{}`

Invalid inputs return `{:error, %Aerospike.Error{code: :parameter_error}}`.

### Repo API

```elixir
:ok = MyApp.Repo.set_xdr_filter("dc-west", "test", filter)
:ok = MyApp.Repo.set_xdr_filter("dc-west", "test", nil)
```

### XDR Integration Coverage

End-to-end XDR integration requires an Enterprise Edition cluster with XDR
configured. The current local test profile proves command encoding and wrapped
error handling via unit tests, but does not verify server-side filter
application. XDR integration is tested only when an enterprise XDR profile is
active.

## Telemetry vs Client-Managed Metrics

| Mechanism | Configured by | Always active? | What it measures |
|-----------|--------------|---------------|-----------------|
| `[:aerospike, :command]` Telemetry | Caller attaches handlers | Yes | Per-command lifecycle events |
| `enable_metrics/1` + `stats/1` | Caller calls `enable_metrics/1` | No | Aggregate command counters and connection state |

Both mechanisms can coexist. Disabling client-managed metrics does not affect
Telemetry emission.
