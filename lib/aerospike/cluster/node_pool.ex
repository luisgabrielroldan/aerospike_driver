defmodule Aerospike.Cluster.NodePool do
  @moduledoc false

  @behaviour NimblePool

  alias Aerospike.Cluster.NodeCounters
  alias Aerospike.Error
  alias Aerospike.Runtime.PoolCheckout
  alias Aerospike.RuntimeMetrics

  require Logger

  @checkout_reason :checkout

  @typedoc "Result returned verbatim from the `fun` passed to `checkout!/3`."
  @type result :: term()

  @typedoc """
  Value the caller's `fun` returns as the second element of its tuple.

  `conn` keeps the worker for reuse. `:close` removes the worker with
  no failure accounting. `{:close, :failure}` removes the worker *and*
  increments the node's `:failed` counter.
  """
  @type checkin_value :: term() | :close | {:close, :failure}

  @doc """
  Checks out and checks in `count` workers serially to prove the pool can serve them.

  This does not create a new warm-up mode. It exercises the already-started
  pool so operator-facing code can verify that the configured worker slots are
  reachable through the normal checkout path.
  """
  @spec warm_up(pid(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, Error.t(), non_neg_integer()}
  def warm_up(pool, count, timeout)
      when is_pid(pool) and is_integer(count) and count >= 0 and is_integer(timeout) and
             timeout >= 0 do
    do_warm_up(pool, count, 0, timeout)
  end

  @doc """
  Checks out a worker and runs `fun.(conn)` with a pool-owned connection.

  Equivalent to `checkout!/4` with `node_name: nil`. Kept as a separate
  entry point so tests that build a pool without a cluster context can
  call the pool directly.
  """
  @spec checkout!(
          NimblePool.pool(),
          (conn :: term() -> {result(), checkin_value()}),
          timeout()
        ) :: result() | {:error, Error.t()}
  def checkout!(pool, fun, timeout), do: checkout!(nil, pool, fun, timeout)

  @doc """
  Checks out a worker on behalf of `node_name` and runs `fun.(conn)`
  with a pool-owned connection.

  `fun` must return a two-tuple `{result, checkin_value}` where:

    * `result` is returned verbatim from `checkout!/4`.
    * `checkin_value` controls worker lifecycle and node-health
      accounting. See `t:checkin_value/0`.

  Pool-level failures are translated to `{:error, %Aerospike.Error{}}`:

    * `:pool_timeout` — the checkout timed out waiting for a free worker.
    * `:invalid_node` — the pool process is gone.
    * `:network_error` — the checkout exited for any other reason.

  These pool-level failures never bump the `:failed` counter because
  `fun` did not run against a live connection — there is no node-health
  signal to record.

  `Aerospike.Runtime.PoolCheckout` emits
  `[:aerospike, :pool, :checkout, :start | :stop]` events around the
  checkout wrapper. Stop events fire for both the success path and the
  pool-level error translations above. Metadata carries `:node_name`
  and `:pool_pid`; measurements are `:system_time` on start and
  `:duration` on stop.
  """
  @spec checkout!(
          String.t() | nil,
          NimblePool.pool(),
          (conn :: term() -> {result(), checkin_value()}),
          timeout()
        ) :: result() | {:error, Error.t()}
  def checkout!(node_name, pool, fun, timeout)
      when is_function(fun, 1) and is_integer(timeout) do
    PoolCheckout.run(
      node_name,
      pool,
      fn ->
        NimblePool.checkout!(
          pool,
          @checkout_reason,
          fn _from, conn ->
            fun.(conn)
          end,
          timeout
        )
      end,
      timeout
    )
  end

  defp do_warm_up(_pool, 0, warmed, _timeout), do: {:ok, warmed}

  defp do_warm_up(pool, remaining, warmed, timeout) when remaining > 0 do
    case checkout!(pool, fn conn -> {:ok, conn} end, timeout) do
      :ok ->
        do_warm_up(pool, remaining - 1, warmed + 1, timeout)

      {:error, %Error{} = error} ->
        {:error, error, warmed}
    end
  end

  ## NimblePool callbacks

  @impl NimblePool
  def init_pool(opts) when is_list(opts) do
    _transport = Keyword.fetch!(opts, :transport)
    _host = Keyword.fetch!(opts, :host)
    _port = Keyword.fetch!(opts, :port)
    _connect_opts = Keyword.fetch!(opts, :connect_opts)
    _node_name = Keyword.fetch!(opts, :node_name)
    # `:counters` is optional: tests and cluster-state-only modes may
    # omit it, and the callbacks below degrade to no-ops when it is nil.
    _counters = Keyword.get(opts, :counters)
    _cluster_name = Keyword.get(opts, :cluster_name)
    # `:features` is optional and defaults to an empty set so tests and
    # cluster-state-only modes that never run the bootstrap probe still
    # produce a usable pool. Stash an explicit empty set on the
    # pool_state when omitted so consumers can pattern-match without a
    # `nil` branch.
    opts =
      case Keyword.has_key?(opts, :features) do
        true -> opts
        false -> Keyword.put(opts, :features, MapSet.new())
      end

    {:ok, opts}
  end

  @impl NimblePool
  def init_worker(pool_state) when is_list(pool_state) do
    transport = Keyword.fetch!(pool_state, :transport)
    host = Keyword.fetch!(pool_state, :host)
    port = Keyword.fetch!(pool_state, :port)
    connect_opts = Keyword.fetch!(pool_state, :connect_opts)
    node_name = Keyword.fetch!(pool_state, :node_name)

    # Inject `:node_name` so transport implementations can stash it on
    # their connection state and tag telemetry events emitted from
    # worker-owned sockets. The bootstrap/peer info sockets opened by
    # `Aerospike.Cluster.Tender` do not pass through this path, so their events
    # carry `node_name: nil` — acceptable because that traffic is
    # cluster-state-only.
    connect_opts = Keyword.put(connect_opts, :node_name, node_name)
    cluster_name = Keyword.get(pool_state, :cluster_name)

    RuntimeMetrics.record_connection_attempt(cluster_name, node_name)

    try do
      transport.connect(host, port, connect_opts)
    catch
      # A transport process (e.g. a test-owned `Transport.Fake`) can die
      # while its `GenServer.call/3` in `connect/3` is in flight — either
      # because the test that owned it is shutting down, or because the
      # whole node supervisor is terminating. Convert the exit into the
      # same `{:remove, {:connect_failed, _}}` path a real connect error
      # would take, so NimblePool's own callback-error log stays quiet
      # and the pool simply retries the worker slot.
      :exit, reason ->
        {:error,
         %Error{
           code: :network_error,
           message:
             "Aerospike.Cluster.NodePool: connect exited for #{node_name} at #{host}:#{port}: #{inspect(reason)}"
         }}
    end
    |> case do
      {:ok, conn} ->
        RuntimeMetrics.record_connection_success(cluster_name, node_name)

        Logger.debug(fn ->
          "Aerospike.Cluster.NodePool: connected worker for #{node_name} at #{host}:#{port}"
        end)

        {:ok, conn, pool_state}

      {:error, %Error{} = err} ->
        RuntimeMetrics.record_connection_failure(cluster_name, node_name)

        Logger.debug(fn ->
          "Aerospike.Cluster.NodePool: connect failed for #{node_name} at #{host}:#{port}: #{err.message}"
        end)

        {:remove, {:connect_failed, err}}
    end
  end

  @impl NimblePool
  def handle_checkout(@checkout_reason, _from, conn, pool_state) do
    incr_in_flight(pool_state)
    {:ok, conn, conn, pool_state}
  end

  @impl NimblePool
  def handle_checkin({:close, :failure}, _from, _worker, pool_state) do
    decr_in_flight(pool_state)
    incr_failed(pool_state)

    RuntimeMetrics.record_connection_drop(
      Keyword.get(pool_state, :cluster_name),
      Keyword.fetch!(pool_state, :node_name),
      :dead
    )

    {:remove, :closed, pool_state}
  end

  def handle_checkin(:close, _from, _worker, pool_state) do
    decr_in_flight(pool_state)
    {:remove, :closed, pool_state}
  end

  def handle_checkin(conn, _from, _prev, pool_state) do
    decr_in_flight(pool_state)
    {:ok, conn, pool_state}
  end

  # Caller process crashed or timed out while holding a checked-out
  # worker. NimblePool will subsequently call `terminate_worker/3` to
  # destroy the now-abandoned connection, but that callback cannot
  # tell "worker was checked out" from "worker was idle at shutdown".
  # Decrement `:in_flight` here where the distinction is explicit so
  # pool shutdown (idle workers) does not over-decrement.
  @impl NimblePool
  def handle_cancelled(:checked_out, pool_state) do
    decr_in_flight(pool_state)
    :ok
  end

  def handle_cancelled(:queued, _pool_state), do: :ok

  @impl NimblePool
  def handle_ping(_conn, pool_state) do
    node_name = Keyword.fetch!(pool_state, :node_name)
    cluster_name = Keyword.get(pool_state, :cluster_name)

    Logger.debug(fn ->
      "Aerospike.Cluster.NodePool: evicting idle worker for #{node_name}"
    end)

    RuntimeMetrics.record_connection_drop(cluster_name, node_name, :idle)

    {:remove, :idle}
  end

  @impl NimblePool
  def terminate_worker(_reason, conn, pool_state) do
    transport = Keyword.fetch!(pool_state, :transport)
    cluster_name = Keyword.get(pool_state, :cluster_name)
    node_name = Keyword.fetch!(pool_state, :node_name)

    # A worker can outlive its transport peer during a supervisor
    # shutdown: NimblePool calls `terminate_worker/3` on every worker
    # when the pool itself stops, and a linked test-owned fake transport
    # may already be gone by then. Swallow exits from `close/1` so pool
    # shutdown stays clean.
    try do
      _ = transport.close(conn)
    catch
      :exit, _ -> :ok
    end

    RuntimeMetrics.record_connection_closed(cluster_name, node_name)

    {:ok, pool_state}
  end

  ## Counter helpers

  # Keep the optional `nil` branch at the pool boundary so the callback
  # write paths stay explicit and `NodeCounters` only models concrete
  # slot operations.

  defp incr_in_flight(pool_state) do
    case Keyword.get(pool_state, :counters) do
      nil -> :ok
      ref -> NodeCounters.incr_in_flight(ref)
    end
  end

  defp decr_in_flight(pool_state) do
    case Keyword.get(pool_state, :counters) do
      nil -> :ok
      ref -> NodeCounters.decr_in_flight(ref)
    end
  end

  defp incr_failed(pool_state) do
    case Keyword.get(pool_state, :counters) do
      nil -> :ok
      ref -> NodeCounters.incr_failed(ref)
    end
  end
end
