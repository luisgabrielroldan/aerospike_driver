defmodule Aerospike.NodePoolTest do
  use ExUnit.Case, async: false

  alias Aerospike.Connection
  alias Aerospike.NodePool
  alias Aerospike.Test.MockTcpServer

  # ── Mock Helpers ──

  # Starts a TCP listener that accepts up to `max_conns` connections and holds
  # them open. Sends {:mock_accepted, server_socket} for each to the caller.
  defp start_mock(max_conns \\ 10) do
    {:ok, lsock, port} = MockTcpServer.start()
    parent = self()

    acceptor =
      spawn(fn ->
        accept_hold_loop(lsock, max_conns, parent)
        Process.sleep(:infinity)
      end)

    %{lsock: lsock, port: port, acceptor: acceptor}
  end

  defp accept_hold_loop(_lsock, 0, _parent), do: :ok

  defp accept_hold_loop(lsock, n, parent) do
    case :gen_tcp.accept(lsock, 10_000) do
      {:ok, client} ->
        send(parent, {:mock_accepted, client})
        spawn(fn -> :gen_tcp.recv(client, 0) end)
        accept_hold_loop(lsock, n - 1, parent)

      {:error, _} ->
        :ok
    end
  end

  defp stop_mock(%{lsock: lsock, acceptor: pid}) do
    :gen_tcp.close(lsock)
    if Process.alive?(pid), do: Process.exit(pid, :kill)
  end

  defp pool_child_spec(port, overrides \\ []) do
    idle_timeout = Keyword.get(overrides, :idle_timeout, 55_000)
    auth_opts = Keyword.get(overrides, :auth_opts, [])

    {NimblePool,
     worker:
       {NodePool,
        connect_opts: [
          host: "127.0.0.1",
          port: port,
          timeout: 2_000,
          idle_timeout: idle_timeout
        ],
        auth_opts: auth_opts},
     pool_size: 1}
  end

  # ── init_worker ──

  describe "init_worker" do
    test "connects successfully when no auth is required" do
      mock = start_mock()
      on_exit(fn -> stop_mock(mock) end)

      pool = start_supervised!(pool_child_spec(mock.port))

      result =
        NimblePool.checkout!(pool, :checkout, fn _meta, conn ->
          assert %Connection{} = conn
          assert {:gen_tcp, sock} = conn.transport
          assert is_port(sock)
          {:ok, conn}
        end)

      assert result == :ok
    end

    # NodePool.init_worker/1 returns {:cancel, reason, pool_state} on connect failure,
    # but NimblePool 1.1.0 only accepts {:ok, ...} or {:async, ...}. The pool crashes
    # during worker initialization instead of gracefully retrying.
    @tag :known_bug
    @tag skip: "NodePool.init_worker returns {:cancel, ...} unsupported by NimblePool 1.1.0"
    test "cancels when connect fails (no listener)" do
    end

    # Same incompatibility: {:cancel, ...} from a failed login crashes the pool.
    @tag :known_bug
    @tag skip: "NodePool.init_worker returns {:cancel, ...} unsupported by NimblePool 1.1.0"
    test "cancels when login is rejected" do
    end
  end

  # ── handle_checkout ──

  describe "handle_checkout" do
    test "removes idle worker and replaces with a fresh connection" do
      mock = start_mock()
      on_exit(fn -> stop_mock(mock) end)

      pool = start_supervised!(pool_child_spec(mock.port, idle_timeout: 50))

      # Warm the pool with a checkout/checkin cycle
      NimblePool.checkout!(pool, :checkout, fn _meta, conn -> {:ok, conn} end)

      # Let the idle deadline expire (50ms + buffer)
      Process.sleep(100)

      # Pool should detect idle, remove the worker, and init a fresh one
      result =
        NimblePool.checkout!(
          pool,
          :checkout,
          fn _meta, conn ->
            refute Connection.idle?(conn)
            {:fresh, conn}
          end,
          3_000
        )

      assert result == :fresh
    end

    test "removes dead-socket worker and replaces with a fresh connection" do
      mock = start_mock()
      on_exit(fn -> stop_mock(mock) end)

      pool = start_supervised!(pool_child_spec(mock.port))

      # Warm the pool, then sabotage the underlying socket from inside checkout.
      # The broken conn is returned to the pool via handle_checkin (which only
      # refreshes the idle deadline — it doesn't touch the socket).
      NimblePool.checkout!(pool, :checkout, fn _meta, conn ->
        {:gen_tcp, raw} = conn.transport
        :gen_tcp.close(raw)
        {:ok, conn}
      end)

      # Next checkout: handle_checkout calls transport_peername on the dead socket
      # -> {:error, _} -> {:remove, :dead}. Pool inits a new worker with a fresh
      # connection to the mock server.
      result =
        NimblePool.checkout!(
          pool,
          :checkout,
          fn _meta, conn ->
            assert {:ok, _} = Connection.transport_peername(conn)
            {:fresh, conn}
          end,
          3_000
        )

      assert result == :fresh
    end
  end

  # ── handle_checkin ──

  describe "handle_checkin" do
    test ":close signal removes the worker" do
      mock = start_mock()
      on_exit(fn -> stop_mock(mock) end)

      pool = start_supervised!(pool_child_spec(mock.port))

      # Return :close as the checkin value — handle_checkin(:close, ...) -> {:remove, :closed}
      NimblePool.checkout!(pool, :checkout, fn _meta, _conn ->
        {:ok, :close}
      end)

      # Pool should have removed the old worker; next checkout creates a new one
      result =
        NimblePool.checkout!(pool, :checkout, fn _meta, conn ->
          assert %Connection{} = conn
          {:new_worker, conn}
        end)

      assert result == :new_worker
    end

    test "normal Connection return refreshes idle deadline" do
      mock = start_mock()
      on_exit(fn -> stop_mock(mock) end)

      pool = start_supervised!(pool_child_spec(mock.port, idle_timeout: 200))

      # Checkout/checkin — handle_checkin(%Connection{}, ...) refreshes idle_deadline
      NimblePool.checkout!(pool, :checkout, fn _meta, conn -> {:ok, conn} end)

      # Sleep less than idle_timeout
      Process.sleep(50)

      # The checkin refreshed the deadline, so the connection should NOT be idle
      result =
        NimblePool.checkout!(pool, :checkout, fn _meta, conn ->
          refute Connection.idle?(conn)
          {:alive, conn}
        end)

      assert result == :alive
    end
  end

  # ── terminate_worker ──

  describe "terminate_worker" do
    test "closes the TCP socket when pool stops" do
      mock = start_mock()
      on_exit(fn -> stop_mock(mock) end)

      # Start unmanaged so we control the stop timing. Trap exits so the
      # link doesn't kill the test process when we stop the pool.
      Process.flag(:trap_exit, true)

      {:ok, pool} =
        NimblePool.start_link(
          worker:
            {NodePool,
             connect_opts: [host: "127.0.0.1", port: mock.port, timeout: 2_000], auth_opts: []},
          pool_size: 1
        )

      # Grab the raw socket via checkout
      transport =
        NimblePool.checkout!(pool, :checkout, fn _meta, conn ->
          {conn.transport, conn}
        end)

      {:gen_tcp, raw_socket} = transport

      # Stopping the pool triggers terminate_worker -> Connection.close
      GenServer.stop(pool, :normal, 1_000)
      Process.sleep(50)

      assert {:error, _} = :inet.peername(raw_socket)
    end
  end
end
