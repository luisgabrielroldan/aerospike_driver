defmodule Aerospike.CircuitBreaker do
  @moduledoc """
  Stateless per-node circuit breaker.

  The breaker is a pure function over a node's `Aerospike.NodeCounters`
  reference plus a small options map. It answers one question: is this
  node usable for an immediate command attempt, or should the caller
  short-circuit and fall through to the retry layer (Task 7)?

  Two thresholds gate the answer:

    * `:circuit_open_threshold` — ceiling on the counters' `:failed`
      slot. Once transport-class failures pile up past this value the
      breaker refuses every attempt until the Tender zeroes the slot at
      the next successful tend cycle.

    * `:max_concurrent_ops_per_node` — ceiling on `in_flight + queued`.
      `:queued` currently has no writer and stays at zero, so this is
      effectively a cap on in-flight commands per node. It backstops
      `NimblePool`'s own checkout queue: the breaker rejects before the
      caller enters the queue, so a wedged pool cannot accumulate a
      thundering herd of stuck checkouts.

  ## Stateless by design

  No GenServer, no ETS, no timer. Every call re-reads the counters and
  recomputes the decision, which keeps the hot path a handful of
  `:counters.get/2` reads. Failure-counter decay is the Tender's job —
  it calls `Aerospike.NodeCounters.reset_failed/1` on each successful
  tend cycle so the breaker's "recent failure count" metric resets in
  lockstep with the cluster-state view of node health.

  The breaker is paired with the `:active | :inactive` node lifecycle
  (Task 1): a node the Tender has demoted to `:inactive` already
  returns `{:error, :unknown_node}` from `Aerospike.Tender.node_handle/2`,
  so commands never reach `allow?/2` against an inactive node. The
  breaker handles the narrower case of an `:active` node that is
  currently unusable without needing to promote it to `:inactive`.
  """

  alias Aerospike.Error
  alias Aerospike.NodeCounters

  @typedoc """
  Breaker options.

    * `:circuit_open_threshold` — non-negative integer; the `:failed`
      slot must stay strictly below this to allow an attempt.
    * `:max_concurrent_ops_per_node` — positive integer; the sum
      `in_flight + queued` must stay strictly below this to allow an
      attempt.
  """
  @type opts :: %{
          circuit_open_threshold: non_neg_integer(),
          max_concurrent_ops_per_node: pos_integer()
        }

  @doc """
  Returns `:ok` when a new command may be dispatched against the node
  backed by `counters`, or `{:error, %Aerospike.Error{code: :circuit_open}}`
  when either threshold is breached.

  The returned `Error` names the failing dimension in its `:message`
  so the retry layer's logs can distinguish "too many concurrent
  commands" from "too many recent failures" without reading counter
  slots itself.
  """
  @spec allow?(NodeCounters.t(), opts()) :: :ok | {:error, Error.t()}
  def allow?(counters, %{
        circuit_open_threshold: failure_cap,
        max_concurrent_ops_per_node: concurrency_cap
      })
      when is_integer(failure_cap) and failure_cap >= 0 and is_integer(concurrency_cap) and
             concurrency_cap > 0 do
    failed = NodeCounters.failed(counters)
    in_flight = NodeCounters.in_flight(counters)
    queued = NodeCounters.queued(counters)

    cond do
      failed >= failure_cap ->
        open_error("failure counter #{failed} reached circuit_open_threshold #{failure_cap}")

      in_flight + queued >= concurrency_cap ->
        open_error(
          "in_flight=#{in_flight} queued=#{queued} reached max_concurrent_ops_per_node " <>
            "#{concurrency_cap}"
        )

      true ->
        :ok
    end
  end

  defp open_error(message) do
    {:error, %Error{code: :circuit_open, message: message}}
  end
end
