defmodule Aerospike.NodePool do
  @moduledoc """
  `NimblePool` worker that owns one unauthenticated TCP connection to a
  single Aerospike node via an `Aerospike.NodeTransport` implementation.

  Each pool hosts a fixed number of workers (default `10`). Every worker
  calls `transport.connect(host, port, connect_opts)` on init and
  `transport.close/1` on termination. The pool does not perform liveness
  checks on checkout — a broken connection surfaces on the next send, and
  the caller returns `:close` at check-in to have the worker torn down
  and replaced.

  Tier 1 deliberately omits login/auth, TLS, idle-deadline refresh,
  warm-up, and telemetry: those are scheduled for Tier 2 and Tier 3.

  ## Pool state

  The pool_state is the keyword list passed to `NimblePool.start_link/1`'s
  `:worker` init arg, shaped as:

      [
        transport: module(),
        host: String.t(),
        port: :inet.port_number(),
        connect_opts: keyword(),
        node_name: String.t()
      ]

  `node_name` is recorded for log context; it is not used by the
  transport callbacks.
  """

  @behaviour NimblePool

  alias Aerospike.Error

  require Logger

  @checkout_reason :checkout

  @typedoc "Result returned verbatim from the `fun` passed to `checkout!/3`."
  @type result :: term()

  @doc """
  Checks out a worker and runs `fun.(conn)` with a pool-owned connection.

  `fun` must return a two-tuple `{result, checkin_value}` where:

    * `result` is returned verbatim from `checkout!/3`.
    * `checkin_value` is either the connection (normal return — the
      worker is kept and will be reused) or the atom `:close` (the
      worker is removed and a fresh one is initialised on the next
      checkout).

  Pool-level failures are translated to `{:error, %Aerospike.Error{}}`:

    * `:pool_timeout` — the checkout timed out waiting for a free worker.
    * `:invalid_node` — the pool process is gone.
    * `:network_error` — the checkout exited for any other reason.
  """
  @spec checkout!(
          NimblePool.pool(),
          (conn :: term() -> {result(), term() | :close}),
          timeout()
        ) :: result() | {:error, Error.t()}
  def checkout!(pool, fun, timeout) when is_function(fun, 1) and is_integer(timeout) do
    NimblePool.checkout!(
      pool,
      @checkout_reason,
      fn _from, conn ->
        fun.(conn)
      end,
      timeout
    )
  catch
    :exit, {:timeout, {NimblePool, :checkout, _}} ->
      {:error, Error.from_result_code(:pool_timeout)}

    :exit, {:noproc, {NimblePool, :checkout, _}} ->
      {:error,
       %Error{
         code: :invalid_node,
         message: "Aerospike.NodePool: pool not available"
       }}

    :exit, reason ->
      {:error,
       %Error{
         code: :network_error,
         message: "Aerospike.NodePool: checkout exited: #{inspect(reason)}"
       }}
  end

  ## NimblePool callbacks

  @impl NimblePool
  def init_pool(opts) when is_list(opts) do
    _transport = Keyword.fetch!(opts, :transport)
    _host = Keyword.fetch!(opts, :host)
    _port = Keyword.fetch!(opts, :port)
    _connect_opts = Keyword.fetch!(opts, :connect_opts)
    _node_name = Keyword.fetch!(opts, :node_name)

    {:ok, opts}
  end

  @impl NimblePool
  def init_worker(pool_state) when is_list(pool_state) do
    transport = Keyword.fetch!(pool_state, :transport)
    host = Keyword.fetch!(pool_state, :host)
    port = Keyword.fetch!(pool_state, :port)
    connect_opts = Keyword.fetch!(pool_state, :connect_opts)

    case transport.connect(host, port, connect_opts) do
      {:ok, conn} ->
        {:ok, conn, pool_state}

      {:error, %Error{} = err} ->
        node_name = Keyword.fetch!(pool_state, :node_name)

        Logger.warning(
          "Aerospike.NodePool: connect failed for #{node_name} at #{host}:#{port}: #{err.message}"
        )

        {:remove, {:connect_failed, err}}
    end
  end

  @impl NimblePool
  def handle_checkout(@checkout_reason, _from, conn, pool_state) do
    {:ok, conn, conn, pool_state}
  end

  @impl NimblePool
  def handle_checkin(:close, _from, _worker, pool_state) do
    {:remove, :closed, pool_state}
  end

  def handle_checkin(conn, _from, _prev, pool_state) do
    {:ok, conn, pool_state}
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
end
