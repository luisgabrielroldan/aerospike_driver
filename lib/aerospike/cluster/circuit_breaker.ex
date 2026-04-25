defmodule Aerospike.Cluster.CircuitBreaker do
  @moduledoc false

  alias Aerospike.Cluster.NodeCounters
  alias Aerospike.Error

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
