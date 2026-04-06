defmodule Aerospike.Integration.TxnTest do
  @moduledoc """
  Integration tests for Multi-Record Transactions (MRT).

  Requires Aerospike Enterprise Edition with server 8.0+ and strong-consistency
  namespaces enabled. Start with:

      docker compose --profile enterprise up -d

  Run with:

      mix test --include integration --include enterprise
  """
  use ExUnit.Case, async: false

  alias Aerospike.Error
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers
  alias Aerospike.Txn
  alias Aerospike.TxnOps

  @moduletag :enterprise
  @moduletag :integration

  setup do
    host = System.get_env("AEROSPIKE_EE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_EE_PORT", "3100") |> String.to_integer()

    unless ee_running?(host, port) do
      flunk("""
      Enterprise server not running on #{host}:#{port}. Start with:

          docker compose --profile enterprise up -d
      """)
    end

    name = :"txn_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      tend_interval: 60_000
    ]

    {:ok, _sup} = start_supervised({Aerospike, opts})
    await_cluster_ready(name)

    {:ok, conn: name, host: host, port: port}
  end

  # ---------------------------------------------------------------------------
  # Manual commit/abort lifecycle
  # ---------------------------------------------------------------------------

  test "commit persists all writes", %{conn: conn, host: host, port: port} do
    key1 = Helpers.unique_key("test", "txn_itest")
    key2 = Helpers.unique_key("test", "txn_itest")

    on_exit(fn ->
      Helpers.cleanup_key(key1, host: host, port: port)
      Helpers.cleanup_key(key2, host: host, port: port)
    end)

    txn = Txn.new()
    TxnOps.init_tracking(conn, txn)

    assert :ok = Aerospike.put(conn, key1, %{"x" => 1}, txn: txn)
    assert :ok = Aerospike.put(conn, key2, %{"y" => 2}, txn: txn)
    assert {:ok, :committed} = Aerospike.commit(conn, txn)

    assert {:ok, r1} = Aerospike.get(conn, key1)
    assert r1.bins["x"] == 1

    assert {:ok, r2} = Aerospike.get(conn, key2)
    assert r2.bins["y"] == 2
  end

  test "abort discards all writes", %{conn: conn} do
    key1 = Helpers.unique_key("test", "txn_itest")
    key2 = Helpers.unique_key("test", "txn_itest")

    txn = Txn.new()
    TxnOps.init_tracking(conn, txn)

    assert :ok = Aerospike.put(conn, key1, %{"x" => 1}, txn: txn)
    assert :ok = Aerospike.put(conn, key2, %{"y" => 2}, txn: txn)
    assert {:ok, :aborted} = Aerospike.abort(conn, txn)

    assert {:error, %Error{code: :key_not_found}} = Aerospike.get(conn, key1)
    assert {:error, %Error{code: :key_not_found}} = Aerospike.get(conn, key2)
  end

  test "empty transaction commits successfully", %{conn: conn} do
    txn = Txn.new()
    TxnOps.init_tracking(conn, txn)

    assert {:ok, :committed} = Aerospike.commit(conn, txn)
  end

  test "empty transaction aborts successfully", %{conn: conn} do
    txn = Txn.new()
    TxnOps.init_tracking(conn, txn)

    assert {:ok, :aborted} = Aerospike.abort(conn, txn)
  end

  # ---------------------------------------------------------------------------
  # Verify failure (external modification)
  # ---------------------------------------------------------------------------

  test "commit fails when a read key was externally modified before commit", %{
    conn: conn,
    host: host,
    port: port
  } do
    key = Helpers.unique_key("test", "txn_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    # Create the record outside any transaction
    assert :ok = Aerospike.put(conn, key, %{"v" => 1})

    txn = Txn.new()
    TxnOps.init_tracking(conn, txn)

    # Read inside the transaction — tracks the record version
    assert {:ok, record} = Aerospike.get(conn, key, txn: txn)
    assert record.bins["v"] == 1

    # External write (outside transaction) — bumps the server-side record version
    assert :ok = Aerospike.put(conn, key, %{"v" => 2})

    # Commit should fail because the verify phase detects a version mismatch
    assert {:error, %Error{}} = Aerospike.commit(conn, txn)

    # The external write's value must persist (transaction did not overwrite it)
    assert {:ok, after_record} = Aerospike.get(conn, key)
    assert after_record.bins["v"] == 2
  end

  # ---------------------------------------------------------------------------
  # txn_status/2 observable state
  # ---------------------------------------------------------------------------

  test "txn_status returns :open inside transaction callback", %{conn: conn} do
    captured = :ets.new(:txn_status_capture, [:set, :public])

    Aerospike.transaction(conn, fn txn ->
      status = Aerospike.txn_status(conn, txn)
      :ets.insert(captured, {:status, status})
    end)

    assert [{:status, {:ok, :open}}] = :ets.lookup(captured, :status)
  end

  # ---------------------------------------------------------------------------
  # transaction/2 and transaction/3 wrapper
  # ---------------------------------------------------------------------------

  test "transaction/2 commits on success and records persist", %{
    conn: conn,
    host: host,
    port: port
  } do
    key1 = Helpers.unique_key("test", "txn_itest")
    key2 = Helpers.unique_key("test", "txn_itest")

    on_exit(fn ->
      Helpers.cleanup_key(key1, host: host, port: port)
      Helpers.cleanup_key(key2, host: host, port: port)
    end)

    assert {:ok, :done} =
             Aerospike.transaction(conn, fn txn ->
               :ok = Aerospike.put(conn, key1, %{"a" => 10}, txn: txn)
               :ok = Aerospike.put(conn, key2, %{"b" => 20}, txn: txn)
               :done
             end)

    assert {:ok, r1} = Aerospike.get(conn, key1)
    assert r1.bins["a"] == 10

    assert {:ok, r2} = Aerospike.get(conn, key2)
    assert r2.bins["b"] == 20
  end

  test "transaction/2 aborts on Aerospike.Error and records are not persisted", %{conn: conn} do
    key = Helpers.unique_key("test", "txn_itest")

    result =
      Aerospike.transaction(conn, fn txn ->
        :ok = Aerospike.put(conn, key, %{"x" => 99}, txn: txn)
        raise %Error{code: :server_error, message: "intentional test error"}
      end)

    assert {:error, %Error{code: :server_error}} = result
    assert {:error, %Error{code: :key_not_found}} = Aerospike.get(conn, key)
  end

  test "transaction/3 with verify failure aborts and returns error", %{
    conn: conn,
    host: host,
    port: port
  } do
    key = Helpers.unique_key("test", "txn_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    # Create the record before starting the transaction
    assert :ok = Aerospike.put(conn, key, %{"v" => 1})

    # Inside the transaction callback: read the key (tracks version), then write
    # it externally without the txn option to cause a version mismatch before
    # the auto-commit's verify phase runs.
    result =
      Aerospike.transaction(conn, fn txn ->
        assert {:ok, _} = Aerospike.get(conn, key, txn: txn)
        # External write — bumps server-side version before callback returns
        assert :ok = Aerospike.put(conn, key, %{"v" => 2})
        :should_not_commit
      end)

    # Auto-commit fails because the read version no longer matches
    assert {:error, %Error{}} = result

    # The external write persists; the transaction did not roll it back
    assert {:ok, after_record} = Aerospike.get(conn, key)
    assert after_record.bins["v"] == 2
  end

  # ---------------------------------------------------------------------------
  # Read-then-write in the same transaction
  # ---------------------------------------------------------------------------

  test "read then write different keys in the same transaction commits successfully", %{
    conn: conn,
    host: host,
    port: port
  } do
    read_key = Helpers.unique_key("test", "txn_itest")
    write_key = Helpers.unique_key("test", "txn_itest")

    on_exit(fn ->
      Helpers.cleanup_key(read_key, host: host, port: port)
      Helpers.cleanup_key(write_key, host: host, port: port)
    end)

    # Pre-create the record that will be read inside the transaction
    assert :ok = Aerospike.put(conn, read_key, %{"val" => 42})

    txn = Txn.new()
    TxnOps.init_tracking(conn, txn)

    assert {:ok, record} = Aerospike.get(conn, read_key, txn: txn)
    assert record.bins["val"] == 42

    assert :ok = Aerospike.put(conn, write_key, %{"new" => true}, txn: txn)

    assert {:ok, :committed} = Aerospike.commit(conn, txn)

    assert {:ok, wr} = Aerospike.get(conn, write_key)
    assert wr.bins["new"] == true
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp ee_running?(host, port) do
    case :gen_tcp.connect(~c"#{host}", port, [], 2_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        true

      _ ->
        false
    end
  end

  defp await_cluster_ready(name, timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_loop(name, deadline, timeout)
  end

  defp await_loop(name, deadline, timeout) do
    cond do
      match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key())) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("enterprise cluster not ready within #{timeout}ms")

      true ->
        Process.sleep(100)
        await_loop(name, deadline, timeout)
    end
  end
end
