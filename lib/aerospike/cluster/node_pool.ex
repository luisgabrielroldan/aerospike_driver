defmodule Aerospike.Cluster.NodePool do
  @moduledoc """
  `NimblePool` worker that owns one unauthenticated TCP connection to a
  single Aerospike node via an `Aerospike.Cluster.NodeTransport` implementation.

  Each pool hosts a fixed number of workers (default `10`). Every worker
  calls `transport.connect(host, port, connect_opts)` on init and
  `transport.close/1` on termination. The pool does not perform liveness
  checks on checkout — a broken connection surfaces on the next send, and
  the caller returns `:close` at check-in to have the worker torn down
  and replaced.

  The pool deliberately omits login/auth and TLS. Checkout observability
  and pool-exit translation live in `Aerospike.Runtime.PoolCheckout`, which wraps
  the public checkout entry points, while the callback module stays
  focused on worker lifecycle plus idle-deadline eviction via
  `handle_ping/2` (see "Idle eviction" below).

  ## Warm-up

  The pool is non-lazy: NimblePool calls `init_worker/1` for every
  configured worker during `Supervisor.init/1` before any checkout can
  succeed. Each successful connect is logged at `:debug` and each
  failure at `:warning`. A failed `init_worker/1` returns
  `{:remove, {:connect_failed, _}}`, which NimblePool re-schedules
  asynchronously; the pool stays usable on the workers that did connect
  and the failed slot retries later. The pool does not escalate "all
  workers failed" — that decision belongs to the per-node state machine
  in the Tender.

  ## Idle eviction

  `handle_ping/2` returns `{:remove, :idle}` for every worker that has
  sat unused past the pool's `:worker_idle_timeout`, so NimblePool tears
  the socket down via `terminate_worker/3` (which calls
  `transport.close/1`). The pool does not eagerly re-open evicted
  workers; NimblePool re-initialises them on the next checkout. The
  default idle deadline is chosen by `Aerospike.Cluster.NodeSupervisor` to stay
  below Aerospike's default `proto-fd-idle-ms` of 60_000 ms so the
  client closes the socket before the server would.

  ## Pool state

  The pool_state is the keyword list passed to `NimblePool.start_link/1`'s
  `:worker` init arg, shaped as:

      [
        transport: module(),
        host: String.t(),
        port: :inet.port_number(),
        connect_opts: keyword(),
        node_name: String.t(),
        counters: Aerospike.Cluster.NodeCounters.t() | nil,
        features: MapSet.t()
      ]

  `node_name` is recorded for log context; it is not used by the
  transport callbacks. `counters`, when present, is the per-node
  `:counters` reference the Tender allocated at node registration; the
  pool increments its `:in_flight` slot in `handle_checkout/4`,
  decrements it in `handle_checkin/4` and `handle_cancelled/2`, and
  bumps `:failed` when the caller signals a transport-class failure via
  the `{:close, :failure}` checkin value. See
  `Aerospike.Cluster.NodeCounters` for writer discipline per slot. When
  `counters` is `nil` the callbacks degrade to no-ops so the pool keeps
  working in tests and cluster-state-only modes that never allocate a
  counters reference. The nil-tolerant branch stays in `NodePool`
  because the pool callbacks own the write timing; `NodeCounters`
  remains the concrete-ref API rather than learning about pool_state.

  `features` is the set of capability tokens the Tender captured from
  the node's `features` info-key reply at registration. The pool itself
  does not branch on the set — capability-gated dispatch decisions live
  in the command path — but the set rides on the pool state so that
  consumer can read it without paying a `GenServer.call` into the
  Tender. An empty set means either a probe failure or a server that
  advertises no client-relevant capabilities.

  ## Checkin value protocol

  The second element of the tuple returned by the caller's `fun` is
  passed verbatim to `handle_checkin/4`:

    * `conn` — normal check-in. Worker is kept for reuse.
    * `:close` — worker is removed without counting a failure. Use this
      for benign reasons (e.g. tests intentionally forcing a reconnect).
    * `{:close, :failure}` — worker is removed *and* the counters'
      `:failed` slot is incremented. Use this when the command returned
      a transport-class error (`:network_error`, `:timeout`,
      `:connection_error`) so the Task 6 circuit breaker can read the
      rate.
  """

  @behaviour NimblePool

  alias Aerospike.Error
  alias Aerospike.Cluster.NodeCounters
  alias Aerospike.Runtime.PoolCheckout

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
        Logger.debug(fn ->
          "Aerospike.Cluster.NodePool: connected worker for #{node_name} at #{host}:#{port}"
        end)

        {:ok, conn, pool_state}

      {:error, %Error{} = err} ->
        Logger.warning(
          "Aerospike.Cluster.NodePool: connect failed for #{node_name} at #{host}:#{port}: #{err.message}"
        )

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

    Logger.debug(fn ->
      "Aerospike.Cluster.NodePool: evicting idle worker for #{node_name}"
    end)

    {:remove, :idle}
  end

  @impl NimblePool
  def terminate_worker(_reason, conn, pool_state) do
    transport = Keyword.fetch!(pool_state, :transport)

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
