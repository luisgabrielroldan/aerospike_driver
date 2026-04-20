defmodule Aerospike.Telemetry do
  @moduledoc """
  Event-name constants for the telemetry taxonomy emitted by this driver.

  All events live under the `[:aerospike, ...]` namespace. Callers attach
  handlers via `:telemetry.attach/4` (or `:telemetry.attach_many/4`) using
  the event names returned by the functions in this module. Emitters call
  `:telemetry.span/3` or `:telemetry.execute/3` with those same names.

  Using the constants from here is the contract. Handler code relies on
  these exact event names and metadata keys; emitters must not invent
  new names or forget to route through this module.

  ## Span versus instant events

  Functions whose name ends with `_span` return the *prefix* expected by
  `:telemetry.span/3`. `:telemetry` appends `:start`, `:stop`, or
  `:exception` itself, producing three event names per span:

      prefix = Aerospike.Telemetry.pool_checkout_span()
      :telemetry.span(prefix, start_metadata, fn ->
        {result, stop_metadata}
      end)

  Functions without the `_span` suffix return the full event name for an
  instant event dispatched via `:telemetry.execute/3`:

      :telemetry.execute(
        Aerospike.Telemetry.node_transition(),
        measurements,
        metadata
      )

  ## Taxonomy

  Span events (prefix + `:start | :stop | :exception`):

    * `[:aerospike, :pool, :checkout]` — wraps every
      `Aerospike.NodePool.checkout!/3`. Metadata: `:node_name`,
      `:pool_pid`. Measurements: `:system_time` on start;
      `:duration` on stop (native units per `:telemetry.span/3`).

    * `[:aerospike, :command, :send]` — wraps the send side of a
      per-attempt command on the transport. Metadata: `:node_name`,
      `:attempt` (0-indexed, matching the retry driver), `:deadline_ms`.
      Measurements: `:system_time` on start; `:duration` on stop.

    * `[:aerospike, :command, :recv]` — wraps the receive side of a
      per-attempt command. Metadata: `:node_name`, `:attempt`,
      `:deadline_ms`. Measurements: `:duration` and `:bytes` on stop.

    * `[:aerospike, :info, :rpc]` — wraps every info RPC (Tender cycles
      and pool-worker login). Metadata: `:node_name`, `:commands` (the
      list of info keys requested). Measurements: `:duration` on stop.

    * `[:aerospike, :tender, :tend_cycle]` — wraps one full
      `Aerospike.Tender` cycle. Metadata: `:node_name`. Measurements:
      `:duration` on stop.

    * `[:aerospike, :tender, :partition_map_refresh]` — wraps the
      partition-map refresh stage of the tend cycle (the most expensive
      sub-step). Metadata: `:node_name`. Measurements: `:duration`
      on stop.

  Instant events (dispatched via `:telemetry.execute/3`):

    * `[:aerospike, :node, :transition]` — lifecycle flip for a node.
      Metadata: `:node_name`, `:from` (`:active | :inactive | :unknown`),
      `:to` (same enum), `:reason` (`:bootstrap | :peer_discovery |
      :recovery | :failure_threshold | :dropped`). Measurements:
      `:system_time`.

    * `[:aerospike, :retry, :attempt]` — one event per retry attempt
      beyond the first. Metadata: `:classification` (`:rebalance |
      :transport | :circuit_open`), `:attempt`, `:node_name`.
      Measurements: `:remaining_budget_ms`.
  """

  @pool_checkout_span [:aerospike, :pool, :checkout]
  @command_send_span [:aerospike, :command, :send]
  @command_recv_span [:aerospike, :command, :recv]
  @info_rpc_span [:aerospike, :info, :rpc]
  @tend_cycle_span [:aerospike, :tender, :tend_cycle]
  @partition_map_refresh_span [:aerospike, :tender, :partition_map_refresh]

  @node_transition [:aerospike, :node, :transition]
  @retry_attempt [:aerospike, :retry, :attempt]

  @doc """
  Event prefix for the pool-checkout span.

  Pass to `:telemetry.span/3`. Emits `[..., :start]`, `[..., :stop]`,
  and `[..., :exception]` under `[:aerospike, :pool, :checkout]`.
  """
  @spec pool_checkout_span() :: [:aerospike | :pool | :checkout, ...]
  def pool_checkout_span, do: @pool_checkout_span

  @doc """
  Event prefix for the command-send span.

  Wraps the send side of a single attempt against one node. Retry
  iterations emit a new span per attempt; `:attempt` in metadata
  distinguishes them.
  """
  @spec command_send_span() :: [:aerospike | :command | :send, ...]
  def command_send_span, do: @command_send_span

  @doc """
  Event prefix for the command-recv span.

  Paired with the command-send span. Measurements include `:bytes`
  on `:stop` so handlers can track payload size alongside latency.
  """
  @spec command_recv_span() :: [:aerospike | :command | :recv, ...]
  def command_recv_span, do: @command_recv_span

  @doc """
  Event prefix for the info-RPC span.

  Covers Tender info calls and per-worker login RPCs. Metadata
  `:commands` carries the list of info keys.
  """
  @spec info_rpc_span() :: [:aerospike | :info | :rpc, ...]
  def info_rpc_span, do: @info_rpc_span

  @doc """
  Event prefix for the tend-cycle span.

  Wraps one invocation of `Aerospike.Tender.run_tend/1`. The partition
  map refresh is a nested span — both fire during a normal cycle.
  """
  @spec tend_cycle_span() :: [:aerospike | :tender | :tend_cycle, ...]
  def tend_cycle_span, do: @tend_cycle_span

  @doc """
  Event prefix for the partition-map-refresh span.

  Wraps the `maybe_refresh_partition_maps/1` stage specifically so
  operators can separate fetching partition-generation from decoding
  the full replicas payload.
  """
  @spec partition_map_refresh_span() ::
          [:aerospike | :tender | :partition_map_refresh, ...]
  def partition_map_refresh_span, do: @partition_map_refresh_span

  @doc """
  Event name for a node lifecycle transition.

  Instant event dispatched via `:telemetry.execute/3`. Metadata carries
  `:from`, `:to`, `:reason`, and `:node_name`.
  """
  @spec node_transition() :: [:aerospike | :node | :transition, ...]
  def node_transition, do: @node_transition

  @doc """
  Event name for a retry attempt beyond the first.

  Instant event dispatched via `:telemetry.execute/3`. Metadata carries
  the retry classification, attempt index, and node name.
  """
  @spec retry_attempt() :: [:aerospike | :retry | :attempt, ...]
  def retry_attempt, do: @retry_attempt
end
