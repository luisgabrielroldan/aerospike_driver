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

  defp contains_ext_tuple?({:ext, _, _}), do: true

  defp contains_ext_tuple?(list) when is_list(list) do
    Enum.any?(list, &contains_ext_tuple?/1)
  end

  defp contains_ext_tuple?(map) when is_map(map) do
    Enum.any?(map, fn {k, v} ->
      contains_ext_tuple?(k) or contains_ext_tuple?(v)
    end)
  end

  defp contains_ext_tuple?(_), do: false

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

  test "operate supports add + put + get in one round-trip", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"n" => 1, "s" => "hello"})

    import Op

    assert {:ok, rec} =
             Aerospike.operate(conn, key, [
               add("n", 4),
               put("tag", "ok"),
               get("n"),
               get("tag")
             ])

    assert rec.bins["n"] == 5
    assert rec.bins["tag"] == "ok"
  end

  test "operate returns per-op values for repeated same-bin ops", %{
    conn: conn,
    host: host,
    port: port
  } do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"counter" => 1})

    import Op

    assert {:ok, rec} =
             Aerospike.operate(
               conn,
               key,
               [
                 add("counter", 1),
                 add("counter", 2),
                 get("counter")
               ],
               respond_per_each_op: true
             )

    assert rec.bins["counter"] == [nil, nil, 4]
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

  test "list operations cover insert, pop, sort and rank reads", %{
    conn: conn,
    host: host,
    port: port
  } do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"nums" => [3, 1, 2]})

    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               ListOp.insert("nums", 1, 5),
               ListOp.sort("nums")
             ])

    assert {:ok, rec1} =
             Aerospike.operate(conn, key, [
               ListOp.get_by_rank("nums", 0),
               ListOp.pop("nums", 0)
             ])

    assert rec1.bins["nums"] == [1, 1]

    assert {:ok, rec2} = Aerospike.get(conn, key)
    assert rec2.bins["nums"] == [2, 3, 5]
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

  test "map increment and get_by_rank_range_from", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"_seed" => 0})

    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               MapOp.put_items("metrics", %{"a" => 1, "b" => 2, "c" => 3})
             ])

    assert {:ok, _} = Aerospike.operate(conn, key, [MapOp.increment("metrics", "b", 5)])

    assert {:ok, rec} =
             Aerospike.operate(conn, key, [
               MapOp.get_by_rank_range_from("metrics", 0, return_type: MapOp.return_key_value())
             ])

    entries = rec.bins["metrics"]

    case entries do
      m when is_map(m) ->
        assert m["b"] == 7 or m[:b] == 7

      list when is_list(list) ->
        assert Enum.any?(list, fn
                 {"b", 7} -> true
                 {:b, 7} -> true
                 _ -> false
               end)

      other ->
        flunk("unexpected map rank-range payload: #{inspect(other)}")
    end
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

  test "list/map round-trip supports nested and mixed values", %{
    conn: conn,
    host: host,
    port: port
  } do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    payload = %{
      "list_bin" => [1, "two", true, %{"k" => "v"}],
      "map_bin" => %{"n" => 1, "s" => "x", "l" => [1, 2], "m" => %{"deep" => true}}
    }

    assert :ok = Aerospike.put(conn, key, payload)
    assert {:ok, rec} = Aerospike.get(conn, key)

    assert rec.bins["list_bin"] == payload["list_bin"]
    assert rec.bins["map_bin"] == payload["map_bin"]
  end

  test "ordered map/list reads do not leak MessagePack sentinel entries", %{
    conn: conn,
    host: host,
    port: port
  } do
    key = Helpers.unique_key("test", "operate_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"_seed" => 0})

    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               MapOp.put_items("ordered_map", %{"b" => 2, "a" => 1}, policy: %{attr: 3, flags: 0}),
               ListOp.append_items("ordered_list", [3, 1, 2], policy: %{order: 1, flags: 0})
             ])

    assert {:ok, rec} = Aerospike.get(conn, key)

    ordered_map = rec.bins["ordered_map"]
    ordered_list = rec.bins["ordered_list"]

    assert ordered_map == %{"a" => 1, "b" => 2}
    assert ordered_list == [1, 2, 3]
    refute contains_ext_tuple?(ordered_map)
    refute contains_ext_tuple?(ordered_list)
  end
end
