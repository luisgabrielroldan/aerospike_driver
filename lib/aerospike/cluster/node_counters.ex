defmodule Aerospike.Cluster.NodeCounters do
  @moduledoc false

  alias Aerospike.RetryPolicy

  @in_flight 1
  @queued 2
  @failed 3

  @num_slots 3

  @type t :: :counters.counters_ref()

  @doc """
  Allocates a fresh counters reference with every slot initialised to
  zero. The Tender calls this when it registers a node.
  """
  @spec new() :: t()
  def new, do: :counters.new(@num_slots, [:atomics])

  @doc "Reads the `:in_flight` slot."
  @spec in_flight(t()) :: non_neg_integer()
  def in_flight(ref), do: :counters.get(ref, @in_flight)

  @doc "Reads the `:queued` slot (always `0`; no writer currently maintains it)."
  @spec queued(t()) :: non_neg_integer()
  def queued(ref), do: :counters.get(ref, @queued)

  @doc "Reads the `:failed` slot."
  @spec failed(t()) :: non_neg_integer()
  def failed(ref), do: :counters.get(ref, @failed)

  @doc "Increments `:in_flight` by 1."
  @spec incr_in_flight(t()) :: :ok
  def incr_in_flight(ref), do: :counters.add(ref, @in_flight, 1)

  @doc "Decrements `:in_flight` by 1."
  @spec decr_in_flight(t()) :: :ok
  def decr_in_flight(ref), do: :counters.sub(ref, @in_flight, 1)

  @doc "Increments `:failed` by 1."
  @spec incr_failed(t()) :: :ok
  def incr_failed(ref), do: :counters.add(ref, @failed, 1)

  @doc """
  Zeroes the `:failed` slot. Called by the Tender on a successful tend
  cycle so the breaker's "recent failure count" metric does not grow
  monotonically over a node's lifetime.
  """
  @spec reset_failed(t()) :: :ok
  def reset_failed(ref) do
    :counters.put(ref, @failed, 0)
  end

  @doc """
  Returns `true` when `term` is a command result that the pool should
  count as a transport-class failure. Non-error results (`{:ok, _}`,
  bare `:ok`), server errors (`:key_not_found`), and rebalance-class
  errors (`:partition_unavailable`) return `false`.

  The pool calls this on the `fun.(conn)` return's first element; the
  second element (the checkin value) is not consulted.
  """
  @spec failure?(term()) :: boolean()
  def failure?(term), do: RetryPolicy.node_failure?(term)
end
