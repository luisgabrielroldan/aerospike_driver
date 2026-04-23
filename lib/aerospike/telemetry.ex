defmodule Aerospike.Telemetry do
  @moduledoc """
  Event-name constants for the telemetry taxonomy emitted by this driver.

  All events live under the `[:aerospike, ...]` namespace. Callers attach
  handlers via `:telemetry.attach/4` (or `:telemetry.attach_many/4`) using
  the event names returned by the functions in this module. Emitters call
  `:telemetry.span/3` or emit the concrete event names directly via
  `:telemetry.execute/3` with those same names.

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

  Operators that want one handler for the whole driver can attach to
  `handler_events/0`, which expands the current taxonomy into concrete
  event names suitable for `:telemetry.attach_many/4`.

  For the operator-facing reference, including emitter locations and the
  supported metadata/measurement contract for each family, see
  `spike-docs/telemetry.md` in the workspace root.

  ## Taxonomy

  Span events (prefix + `:start | :stop | :exception`):

    * `[:aerospike, :pool, :checkout]` — wraps every
      `Aerospike.Cluster.NodePool.checkout!/3`. Metadata: `:node_name`,
      `:pool_pid`, `:telemetry_span_context`. Measurements:
      `:system_time` and `:monotonic_time` on start; `:duration` on
      stop.

    * `[:aerospike, :command, :send]` — wraps the send side of a
      per-attempt command on the transport. Metadata: `:node_name`,
      `:attempt` (0-indexed, matching the retry driver), `:deadline_ms`.
      Measurements: `:system_time` on start; `:duration` on stop.

    * `[:aerospike, :command, :recv]` — wraps the receive side of a
      per-attempt command. Metadata: `:node_name`, `:attempt`,
      `:deadline_ms`. Measurements: `:duration` and `:bytes` on stop.

    * `[:aerospike, :info, :rpc]` — wraps every info RPC (Tender cycles
      and transport login/authenticate handshakes). Metadata:
      `:node_name`, `:commands` (the list of info keys requested, or
      `[:login]` for auth). Measurements: `:duration` on stop.

    * `[:aerospike, :tender, :tend_cycle]` — wraps one full
      `Aerospike.Cluster.Tender` cycle. Cluster-wide (one pair per cycle).
      Metadata: none. Measurements: `:duration` on stop.

    * `[:aerospike, :tender, :partition_map_refresh]` — wraps the
      partition-map refresh stage of the tend cycle (the most expensive
      sub-step). Cluster-wide, nested inside a `:tend_cycle` span.
      Metadata: none. Measurements: `:duration` on stop.

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
  @span_suffixes [:start, :stop, :exception]

  @doc """
  Event prefix for the pool-checkout span.

  This prefix names the checkout telemetry family. The current emitter
  dispatches the concrete `:start`, `:stop`, and `:exception` names
  directly so it can keep the same metadata on every suffix.
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

  Covers Tender info calls and transport login/authenticate RPCs.
  Metadata `:commands` carries the list of info keys, or `[:login]`
  for auth handshakes.
  """
  @spec info_rpc_span() :: [:aerospike | :info | :rpc, ...]
  def info_rpc_span, do: @info_rpc_span

  @doc """
  Event prefix for the tend-cycle span.

  Wraps one invocation of `Aerospike.Cluster.Tender.run_tend/1`. The partition
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

  @doc """
  Emits one retry-attempt event beyond the first command attempt.

  Unary and batch executors share this helper so the retry metadata stays
  aligned across execution paths.
  """
  @spec emit_retry_attempt(String.t() | nil, non_neg_integer(), atom(), integer()) :: :ok
  def emit_retry_attempt(node_name, attempt, classification, remaining_budget_ms)
      when is_integer(attempt) and attempt >= 0 and is_integer(remaining_budget_ms) do
    :telemetry.execute(
      retry_attempt(),
      %{remaining_budget_ms: max(remaining_budget_ms, 0)},
      %{
        classification: classification,
        attempt: attempt,
        node_name: node_name
      }
    )
  end

  @doc """
  Returns every span prefix in the supported telemetry taxonomy.

  This is the stable list for code that needs to derive concrete
  `:start`/`:stop`/`:exception` event names without hard-coding them in
  multiple places.
  """
  @spec span_prefixes() :: [[atom()]]
  def span_prefixes do
    [
      pool_checkout_span(),
      command_send_span(),
      command_recv_span(),
      info_rpc_span(),
      tend_cycle_span(),
      partition_map_refresh_span()
    ]
  end

  @doc """
  Returns every instant-event name in the supported telemetry taxonomy.
  """
  @spec instant_event_names() :: [[atom()]]
  def instant_event_names do
    [
      node_transition(),
      retry_attempt()
    ]
  end

  @doc """
  Returns the concrete event names operators can attach to directly.

  Span prefixes are expanded to the standard `:start`, `:stop`, and
  `:exception` event names, then instant events are appended.
  """
  @spec handler_events() :: [[atom()]]
  def handler_events do
    Enum.flat_map(span_prefixes(), &span_event_names/1) ++ instant_event_names()
  end

  defp span_event_names(prefix) do
    Enum.map(@span_suffixes, &(prefix ++ [&1]))
  end
end
