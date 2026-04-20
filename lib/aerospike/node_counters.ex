defmodule Aerospike.NodeCounters do
  @moduledoc """
  Per-node `:counters` reference holding lock-free cluster-health metrics.

  The Tender allocates one `:counters` reference when it registers a node
  and releases it (by dropping the reference) when the node is dropped or
  marked `:inactive`. Slots are:

    * `@in_flight` (1) — number of commands currently holding a pooled
      connection for this node. Incremented by `Aerospike.NodePool`
      inside `handle_checkout/4` before the caller's `fun.(conn)` runs;
      decremented in `handle_checkin/4` (both `:ok` and `:remove`
      branches) and in `terminate_worker/3`.

    * `@queued` (2) — reserved. No writer currently maintains this slot.
      NimblePool's queue length is available via `:sys.get_state/1`,
      which is a GenServer call. Until the circuit breaker proves it
      needs a lock-free read, the slot stays at zero.

    * `@failed` (3) — number of transport-class command failures
      observed against this node. Incremented by `Aerospike.NodePool`
      after `fun.(conn)` returns when the command result is an
      `%Aerospike.Error{}` whose `:code` is in `@failure_codes`.
      Rebalance-class errors (`Aerospike.Error.rebalance?/1`) are a
      routing cue, not a node-health signal, so they never bump this
      slot. Pool-level errors that occur before `fun` runs
      (`:pool_timeout`, `:invalid_node`) likewise do not bump this slot.
      The Tender zeroes the slot on a successful tend cycle for the
      node; the circuit breaker reads it.

  Writer discipline:

    * `@in_flight` — single writer = `Aerospike.NodePool` (callbacks run
      inside the pool's own process).
    * `@queued` — no writer.
    * `@failed` — two writers: `Aerospike.NodePool` (increment on
      transport-class failure) and `Aerospike.Tender` (zero on tend
      success, clear on node drop). `:counters` is atomic so this is
      safe, but only these two processes may write.

  Tests that construct counters directly must use `new/0` so the slot
  layout stays internal to this module.
  """

  @in_flight 1
  @queued 2
  @failed 3

  @num_slots 3

  # Transport-class error codes that the pool counts as node-health
  # signals. Keep this list narrow: only errors that plausibly indicate
  # "the node itself is misbehaving" belong here. Server-side logical
  # errors (`:key_not_found`, `:generation_error`, etc.) are not failures
  # of the node; rebalance-class errors are a routing cue handled by the
  # retry layer.
  @failure_codes [:network_error, :timeout, :connection_error]

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
  def failure?({:error, %Aerospike.Error{code: code}}) do
    code in @failure_codes
  end

  def failure?(_other), do: false
end
