defmodule Aerospike.Integration.BatchTest do
  use ExUnit.Case, async: false

  alias Aerospike.Batch
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  defmodule TelemetryForwarder do
    @moduledoc false

    def relay(event, _measurements, metadata, parent) do
      case event do
        [:aerospike, :command, :start] ->
          send(parent, {:telemetry_cmd, :start, metadata})

        [:aerospike, :command, :stop] ->
          send(parent, {:telemetry_cmd, :stop, metadata})

        _ ->
          :ok
      end
    end
  end

  @moduletag :integration

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"batch_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      tend_interval: 60_000,
      defaults: [
        read: [timeout: 3_000],
        write: [timeout: 3_000],
        batch: [timeout: 5_000]
      ]
    ]

    {:ok, _sup} = start_supervised({Aerospike, opts})
    await_cluster_ready(name)

    {:ok, conn: name, host: host, port: port}
  end

  test "batch_get empty list", %{conn: conn} do
    assert {:ok, []} = Aerospike.batch_get(conn, [])
  end

  test "batch_get!/batch_exists!/batch_operate! unwrap results", %{
    conn: conn,
    host: host,
    port: port
  } do
    k = Helpers.unique_key("test", "batch_itest")
    on_exit(fn -> Helpers.cleanup_key(k, host: host, port: port) end)

    :ok = Aerospike.put!(conn, k, %{"bang" => 1})

    assert [_] = Aerospike.batch_get!(conn, [k])
    assert [true] = Aerospike.batch_exists!(conn, [k])

    {:ok, want} =
      Aerospike.batch_operate(conn, [Batch.read(k, bins: ["bang"])])

    assert want == Aerospike.batch_operate!(conn, [Batch.read(k, bins: ["bang"])])
  end

  test "batch_get all existing keys", %{conn: conn, host: host, port: port} do
    k1 = Helpers.unique_key("test", "batch_itest")
    k2 = Helpers.unique_key("test", "batch_itest")
    on_exit(fn -> Helpers.cleanup_key(k1, host: host, port: port) end)
    on_exit(fn -> Helpers.cleanup_key(k2, host: host, port: port) end)

    :ok = Aerospike.put!(conn, k1, %{"n" => 1})
    :ok = Aerospike.put!(conn, k2, %{"n" => 2})

    assert {:ok, [r1, r2]} = Aerospike.batch_get(conn, [k1, k2])
    assert r1.bins["n"] == 1
    assert r2.bins["n"] == 2
  end

  test "batch_get mixed existing and missing", %{conn: conn, host: host, port: port} do
    k1 = Helpers.unique_key("test", "batch_itest")
    k2 = Helpers.unique_key("test", "batch_itest")
    on_exit(fn -> Helpers.cleanup_key(k1, host: host, port: port) end)
    on_exit(fn -> Helpers.cleanup_key(k2, host: host, port: port) end)

    :ok = Aerospike.put!(conn, k1, %{"x" => "a"})

    assert {:ok, [rec, nil]} = Aerospike.batch_get(conn, [k1, k2])
    assert rec.bins["x"] == "a"
  end

  test "batch_get bins projection", %{conn: conn, host: host, port: port} do
    k = Helpers.unique_key("test", "batch_itest")
    on_exit(fn -> Helpers.cleanup_key(k, host: host, port: port) end)

    :ok = Aerospike.put!(conn, k, %{"a" => 1, "b" => 2, "c" => 3})

    assert {:ok, [r]} = Aerospike.batch_get(conn, [k], bins: ["b"])
    assert r.bins == %{"b" => 2}
  end

  test "batch_get header_only returns generation and ttl without bins", %{
    conn: conn,
    host: host,
    port: port
  } do
    k = Helpers.unique_key("test", "batch_itest")
    on_exit(fn -> Helpers.cleanup_key(k, host: host, port: port) end)

    :ok = Aerospike.put!(conn, k, %{"a" => 1, "b" => 2}, ttl: 600)

    assert {:ok, [r]} = Aerospike.batch_get(conn, [k], header_only: true)
    assert r.bins == %{}
    assert r.generation >= 1
    assert r.ttl > 0
  end

  test "batch_exists", %{conn: conn, host: host, port: port} do
    k1 = Helpers.unique_key("test", "batch_itest")
    k2 = Helpers.unique_key("test", "batch_itest")
    on_exit(fn -> Helpers.cleanup_key(k1, host: host, port: port) end)
    on_exit(fn -> Helpers.cleanup_key(k2, host: host, port: port) end)

    :ok = Aerospike.put!(conn, k1, %{"v" => 1})

    assert {:ok, [true, false]} = Aerospike.batch_exists(conn, [k1, k2])
  end

  test "batch_operate read put delete", %{conn: conn, host: host, port: port} do
    k1 = Helpers.unique_key("test", "batch_itest")
    k2 = Helpers.unique_key("test", "batch_itest")
    k3 = Helpers.unique_key("test", "batch_itest")
    on_exit(fn -> Helpers.cleanup_key(k1, host: host, port: port) end)
    on_exit(fn -> Helpers.cleanup_key(k2, host: host, port: port) end)
    on_exit(fn -> Helpers.cleanup_key(k3, host: host, port: port) end)

    :ok = Aerospike.put!(conn, k3, %{"old" => true})

    import Aerospike.Op

    assert {:ok, results} =
             Aerospike.batch_operate(conn, [
               Batch.read(k1, bins: ["x"]),
               Batch.put(k2, %{"y" => 2}),
               Batch.delete(k3)
             ])

    assert [%{status: :ok, record: r1}, %{status: :ok, record: nil}, %{status: :ok, record: nil}] =
             Enum.map(results, &Map.take(&1, [:status, :record]))

    assert r1 == nil or r1.bins == %{}

    assert {:ok, false} = Aerospike.exists(conn, k3)
  end

  test "batch_operate with Op.add", %{conn: conn, host: host, port: port} do
    k = Helpers.unique_key("test", "batch_itest")
    on_exit(fn -> Helpers.cleanup_key(k, host: host, port: port) end)

    :ok = Aerospike.put!(conn, k, %{"c" => 10})

    import Aerospike.Op

    assert {:ok, [res]} =
             Aerospike.batch_operate(conn, [Batch.operate(k, [add("c", 5), get("c")])])

    assert res.status == :ok
    assert res.record.bins["c"] == 15
  end

  test "batch telemetry emits command events", %{conn: conn, host: host, port: port} do
    handler_id = :"batch_telem_#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach_many(
        handler_id,
        [[:aerospike, :command, :start], [:aerospike, :command, :stop]],
        &TelemetryForwarder.relay/4,
        parent
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    k = Helpers.unique_key("test", "batch_itest")
    on_exit(fn -> Helpers.cleanup_key(k, host: host, port: port) end)

    assert {:ok, [_]} = Aerospike.batch_get(conn, [k])

    assert_receive {:telemetry_cmd, :start, %{command: :batch_get}}
    assert_receive {:telemetry_cmd, :stop, stop_meta}
    assert stop_meta.result == :ok
    assert stop_meta.batch_size == 1
  end

  defp await_cluster_ready(name, attempts \\ 50)

  defp await_cluster_ready(_name, 0), do: flunk("cluster not ready")

  defp await_cluster_ready(name, n) do
    case :ets.lookup(Tables.meta(name), Tables.ready_key()) do
      [{_, true}] -> :ok
      _ -> Process.sleep(20) && await_cluster_ready(name, n - 1)
    end
  end
end
