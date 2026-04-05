defmodule Aerospike.NodePool do
  @moduledoc false
  # NimblePool worker for a single Aerospike node. Each pool maintains
  # a fixed number of authenticated connections (plain TCP or TLS) to one node.
  # Connections are health-checked on checkout (idle deadline + socket liveness)
  # and torn down on checkin when the caller signals `:close`.

  @behaviour NimblePool

  alias Aerospike.Connection

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

    case Connection.connect(connect_opts) do
      {:ok, conn} ->
        case Connection.login(conn, auth_opts) do
          {:ok, conn2} -> {:ok, conn2, pool_state}
          {:error, reason} -> {:cancel, reason, pool_state}
        end

      {:error, reason} ->
        {:cancel, reason, pool_state}
    end
  end

  # Reject idle or dead connections so NimblePool replaces them with fresh workers.
  @impl NimblePool
  def handle_checkout(:checkout, _from, conn, pool_state) do
    if Connection.idle?(conn) do
      {:remove, :idle, pool_state}
    else
      case Connection.transport_peername(conn) do
        {:ok, _} -> {:ok, conn, conn, pool_state}
        {:error, _} -> {:remove, :dead, pool_state}
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
    _ = Connection.close(conn)
    {:ok, pool_state}
  end
end
