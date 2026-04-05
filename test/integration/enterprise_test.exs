defmodule Aerospike.Integration.EnterpriseTest do
  @moduledoc """
  Integration tests for Aerospike Enterprise-only features.

  Requires the enterprise container:

      docker compose --profile enterprise up -d

  Run with:

      mix test --include enterprise
  """
  use ExUnit.Case, async: false

  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  @moduletag :enterprise

  setup do
    host = System.get_env("AEROSPIKE_EE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_EE_PORT", "3100") |> String.to_integer()

    unless ee_running?(host, port) do
      flunk("""
      Enterprise server not running on #{host}:#{port}. Start with:

          docker compose --profile enterprise up -d
      """)
    end

    name = :"ee_itest_#{System.unique_integer([:positive])}"

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

  test "durable_delete leaves tombstone", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "ee_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"x" => 1})
    assert {:ok, true} = Aerospike.delete(conn, key, durable_delete: true)
    assert {:ok, false} = Aerospike.exists(conn, key)
  end

  test "put with durable_delete write policy", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "ee_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"v" => 1}, durable_delete: true)
    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["v"] == 1
  end

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
