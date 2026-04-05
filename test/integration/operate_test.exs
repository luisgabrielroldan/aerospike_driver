defmodule Aerospike.Integration.OperateTest do
  use ExUnit.Case, async: false

  alias Aerospike.Op
  alias Aerospike.Op.Bit, as: BitOp
  alias Aerospike.Op.HLL, as: HLLOp
  alias Aerospike.Op.List, as: ListOp
  alias Aerospike.Op.Map, as: MapOp
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  @moduletag :integration

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"operate_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      tend_interval: 60_000,
      defaults: [
        read: [timeout: 5_000],
        write: [timeout: 5_000],
        operate: [timeout: 5_000]
      ]
    ]

    {:ok, _sup} = start_supervised({Aerospike, opts})
    await_cluster_ready(name)

    {:ok, conn: name, host: host, port: port}
  end

  defp await_cluster_ready(name, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_cluster_ready_loop(name, deadline)
  end

  defp await_cluster_ready_loop(name, deadline) do
    cond do
      cluster_ready?(name) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("cluster not ready")

      true ->
        Process.sleep(50)
        await_cluster_ready_loop(name, deadline)
    end
  end

  defp cluster_ready?(name) do
    match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key()))
  end

  test "operate mixed read and write returns bins", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"n" => 1, "s" => "a"})

    import Op

    assert {:ok, rec} =
             Aerospike.operate(conn, key, [
               add("n", 2),
               get("n"),
               get("s")
             ])

    assert rec.bins["n"] == 3
    assert rec.bins["s"] == "a"
  end

  test "list append remove size", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"tags" => []})

    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               ListOp.append("tags", "trial"),
               ListOp.append("tags", "vip")
             ])

    assert {:ok, rec} =
             Aerospike.operate(conn, key, [
               ListOp.remove_by_value("tags", "trial", return_type: ListOp.return_none()),
               ListOp.size("tags")
             ])

    assert is_integer(rec.bins["tags"])
    assert rec.bins["tags"] == 1
  end

  test "map put get_by_key remove_by_key", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"_seed" => 0})

    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               MapOp.put_items("prefs", %{"theme" => "dark", "lang" => "en"})
             ])

    assert {:ok, rec} = Aerospike.operate(conn, key, [MapOp.get_by_key("prefs", "theme")])
    assert rec.bins["prefs"] == "dark"

    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               MapOp.remove_by_key("prefs", "lang", return_type: MapOp.return_none())
             ])
  end

  test "bit set and get", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"flags" => {:bytes, <<0x00>>}})

    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               BitOp.resize("flags", 2, 0, flags: 0)
             ])

    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               BitOp.set("flags", 0, 8, <<0xFF>>)
             ])

    assert {:ok, rec} = Aerospike.operate(conn, key, [BitOp.get("flags", 0, 8)])
    # Bit read returns a BLOB particle; Value decodes as `{:blob, binary}`.
    assert {:blob, <<0xFF>>} = rec.bins["flags"]
  end

  test "HLL add and get_count", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"_seed" => 0})

    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               HLLOp.init("visitors", 14, 0)
             ])

    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               HLLOp.add("visitors", ["user:42", "user:99"], 14, 0)
             ])

    assert {:ok, rec} = Aerospike.operate(conn, key, [HLLOp.get_count("visitors")])
    assert is_integer(rec.bins["visitors"])
    assert rec.bins["visitors"] >= 1
  end

  test "nested map ctx put and read", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"profile" => %{"geo" => %{"lat" => 0.0}}})

    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               MapOp.put("profile", "lat", 45.52, ctx: [Aerospike.Ctx.map_key("geo")])
             ])

    assert {:ok, rec} =
             Aerospike.operate(conn, key, [
               MapOp.get_by_key("profile", "lat", ctx: [Aerospike.Ctx.map_key("geo")])
             ])

    assert rec.bins["profile"] == 45.52
  end
end
