# Telemetry And Runtime Metrics

The driver emits telemetry events under the `[:aerospike, ...]` namespace and
exposes opt-in runtime counters through the `Aerospike` facade.
`Aerospike.Telemetry` owns the event-name contract. Subscribe through
`handler_events/0`, `span_prefixes/0`, `instant_event_names/0`, or the
individual helper functions instead of copying raw event lists into handlers.

```elixir
handler_id = "aerospike-driver-logger"

:ok =
  :telemetry.attach_many(
    handler_id,
    Aerospike.Telemetry.handler_events(),
    fn event, measurements, metadata, _config ->
      IO.inspect({event, measurements, metadata}, label: "aerospike")
    end,
    nil
  )
```

## Event Families

Span helpers return prefixes. The concrete emitted names are the prefix plus
`:start`, `:stop`, or `:exception`.

- `Aerospike.Telemetry.pool_checkout_span/0`:
  `[:aerospike, :pool, :checkout, :start | :stop | :exception]`
- `Aerospike.Telemetry.command_send_span/0`:
  `[:aerospike, :command, :send, :start | :stop | :exception]`
- `Aerospike.Telemetry.command_recv_span/0`:
  `[:aerospike, :command, :recv, :start | :stop | :exception]`
- `Aerospike.Telemetry.info_rpc_span/0`:
  `[:aerospike, :info, :rpc, :start | :stop | :exception]`
- `Aerospike.Telemetry.tend_cycle_span/0`:
  `[:aerospike, :tender, :tend_cycle, :start | :stop | :exception]`
- `Aerospike.Telemetry.partition_map_refresh_span/0`:
  `[:aerospike, :tender, :partition_map_refresh, :start | :stop | :exception]`

Instant-event helpers return complete event names.

- `Aerospike.Telemetry.node_transition/0`:
  `[:aerospike, :node, :transition]`
- `Aerospike.Telemetry.retry_attempt/0`:
  `[:aerospike, :retry, :attempt]`

## Measurements And Metadata

Pool-checkout events are emitted by the runtime pool-checkout wrapper. They
measure checkout time for each per-node pool attempt.

- `:start` measurements: `:system_time`, `:monotonic_time`
- `:stop` and `:exception` measurements: `:duration`
- metadata on all suffixes: `:node_name`, `:pool_pid`,
  `:telemetry_span_context`
- `:exception` metadata: `:kind`, `:reason`, `:stacktrace`

Command transport events are emitted around framed socket sends and receives.
Retries produce separate command spans for each attempt.

- send `:start` measurements: `:system_time`
- send `:stop` and `:exception` measurements: `:duration`
- recv `:start` measurements: `:system_time`
- recv `:stop` measurements: `:duration`
- recv `:exception` measurements: `:duration`
- metadata on all suffixes: `:node_name`, `:attempt`, `:deadline_ms`
- recv `:stop` metadata: `:bytes`
- `:exception` metadata: `:kind`, `:reason`, `:stacktrace`

Info RPC events are emitted around ordinary info requests and login/authenticate
handshakes. Authentication uses `commands: [:login]`.

- `:start` measurements: `:system_time`
- `:stop` and `:exception` measurements: `:duration`
- metadata on all suffixes: `:node_name`, `:commands`
- `:exception` metadata: `:kind`, `:reason`, `:stacktrace`

Tender events are emitted around cluster maintenance work. The partition-map
refresh span is nested inside the tend-cycle span.

- `:start` measurements: `:system_time`
- `:stop` and `:exception` measurements: `:duration`
- `:exception` metadata: `:kind`, `:reason`, `:stacktrace`

Node-transition events fire when tender changes a node lifecycle state.

- measurements: `:system_time`
- metadata: `:node_name`, `:from`, `:to`, `:reason`
- current reasons: `:bootstrap`, `:peer_discovery`, `:recovery`,
  `:failure_threshold`, `:dropped`

Retry-attempt events fire when a retryable unary or batch result resolves into
a real next dispatch. The first attempt does not emit a retry event.

- measurements: `:remaining_budget_ms`
- metadata: `:classification`, `:attempt`, `:node_name`
- current classifications: `:rebalance`, `:transport`, `:circuit_open`

Batch retries emit only after reroute resolution succeeds. If reroute
resolution fails, there is no next dispatch and no retry event.

Event emitters are part of the runtime, transport, cluster tending, and retry
paths. Use `Aerospike.Telemetry.handler_events/0` as the stable subscription
source instead of depending on emitter module names.

## Runtime Metrics

Runtime metrics are separate from `:telemetry` handlers. They are disabled by
default and live on the started cluster runtime.

```elixir
false = Aerospike.metrics_enabled?(:aerospike)
:ok = Aerospike.enable_metrics(:aerospike, reset: true)

{:ok, _record} = Aerospike.get(:aerospike, key)
stats = Aerospike.stats(:aerospike)

:ok = Aerospike.disable_metrics(:aerospike)
```

`stats/1` reports cluster configuration, command totals, command status counts,
error-code counts, retry counters, tend counters, and pool checkout counters.
Use telemetry handlers for external metrics pipelines and tracing. Use runtime
metrics when the running cluster should retain local counters for operator
inspection.
