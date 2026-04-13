defmodule Aerospike.NodePool do
  @moduledoc false
  # NimblePool worker for a single Aerospike node. Each pool maintains
  # a fixed number of authenticated connections (plain TCP or TLS) to one node.
  # Connections are health-checked on checkout (idle deadline + socket liveness)
  # and torn down on checkin when the caller signals `:close`.

  @behaviour NimblePool

  alias Aerospike.Connection
  alias Aerospike.Error
  alias Aerospike.RuntimeMetrics

  @doc false
  @spec warm_up(pid(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, Error.t(), non_neg_integer()}
  def warm_up(pool_pid, count, checkout_timeout)
      when is_pid(pool_pid) and is_integer(count) and count >= 0 and
             is_integer(checkout_timeout) and checkout_timeout >= 0 do
    do_warm_up(pool_pid, count, 0, checkout_timeout)
  end

  # Pool state is the keyword opts passed at pool start (connect_opts, auth_opts).
  @impl NimblePool
  def init_pool(opts) when is_list(opts) do
    {:ok, opts}
  end

  # Each worker is a single authenticated Connection (TCP or TLS).
  # Connects and logs in eagerly; cancel on failure so NimblePool can retry.
  @impl NimblePool
  def init_worker(pool_state) when is_list(pool_state) do
    connect_opts = Keyword.fetch!(pool_state, :connect_opts)
    auth_opts = Keyword.get(pool_state, :auth_opts, [])
    conn_name = Keyword.get(pool_state, :conn_name)
    node_name = Keyword.get(pool_state, :node_name)

    RuntimeMetrics.record_connection_attempt(conn_name, node_name)

    case Connection.connect(connect_opts) do
      {:ok, conn} ->
        case Connection.login(conn, auth_opts) do
          {:ok, conn2} ->
            RuntimeMetrics.record_connection_success(conn_name, node_name)
            {:ok, conn2, pool_state}

          {:error, reason} ->
            _ = Connection.close(conn)
            RuntimeMetrics.record_connection_failure(conn_name, node_name)
            {:cancel, reason, pool_state}
        end

      {:error, reason} ->
        RuntimeMetrics.record_connection_failure(conn_name, node_name)
        {:cancel, reason, pool_state}
    end
  end

  # Reject idle or dead connections so NimblePool replaces them with fresh workers.
  @impl NimblePool
  def handle_checkout(:checkout, _from, conn, pool_state) do
    if Connection.idle?(conn) do
      RuntimeMetrics.record_connection_drop(
        Keyword.get(pool_state, :conn_name),
        Keyword.get(pool_state, :node_name),
        :idle
      )

      {:remove, :idle, pool_state}
    else
      case Connection.transport_peername(conn) do
        {:ok, _} ->
          {:ok, conn, conn, pool_state}

        {:error, _} ->
          RuntimeMetrics.record_connection_drop(
            Keyword.get(pool_state, :conn_name),
            Keyword.get(pool_state, :node_name),
            :dead
          )

          {:remove, :dead, pool_state}
      end
    end
  end

  # Caller returns `:close` as the checkin value when the connection errored.
  @impl NimblePool
  def handle_checkin(:close, _from, _worker, pool_state) do
    {:remove, :closed, pool_state}
  end

  # Normal return: reset the idle deadline so the connection stays warm.
  @impl NimblePool
  def handle_checkin(%Connection{} = conn, _from, _prev, pool_state) do
    {:ok, Connection.refresh_idle(conn), pool_state}
  end

  @impl NimblePool
  def terminate_worker(_reason, conn, pool_state) do
    RuntimeMetrics.record_connection_closed(
      Keyword.get(pool_state, :conn_name),
      Keyword.get(pool_state, :node_name)
    )

    _ = Connection.close(conn)
    {:ok, pool_state}
  end

  defp do_warm_up(_pool_pid, 0, warmed, _checkout_timeout), do: {:ok, warmed}

  defp do_warm_up(pool_pid, remaining, warmed, checkout_timeout) when remaining > 0 do
    case checkout_once(pool_pid, checkout_timeout) do
      :ok ->
        do_warm_up(pool_pid, remaining - 1, warmed + 1, checkout_timeout)

      {:error, %Error{} = error} ->
        {:error, error, warmed}
    end
  end

  defp checkout_once(pool_pid, checkout_timeout) do
    NimblePool.checkout!(
      pool_pid,
      :checkout,
      fn _from, conn -> {:ok, conn} end,
      checkout_timeout
    )

    :ok
  catch
    :exit, {:timeout, {NimblePool, :checkout, _}} ->
      {:error, Error.from_result_code(:pool_timeout)}

    :exit, {:noproc, {NimblePool, :checkout, _}} ->
      {:error, Error.from_result_code(:invalid_node)}

    :exit, reason ->
      {:error, Error.from_result_code(:network_error, message: inspect(reason))}
  end
end
