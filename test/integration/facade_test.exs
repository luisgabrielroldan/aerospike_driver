defmodule Aerospike.Integration.FacadeTest do
  use ExUnit.Case, async: false

  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  defmodule TelemetryForwarder do
    @moduledoc false

    def relay(event, _measurements, metadata, parent) do
      case event do
        [:aerospike, :command, :start] ->
          send(parent, {:telemetry_cmd, event, metadata})

        [:aerospike, :command, :stop] ->
          send(parent, {:telemetry_cmd, event, metadata})

        _ ->
          :ok
      end
    end
  end

  @moduletag :integration

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"facade_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      tend_interval: 60_000,
      defaults: [
        read: [timeout: 1_500],
        write: [timeout: 1_500]
      ]
    ]

    {:ok, _sup} = start_supervised({Aerospike, opts})
    await_cluster_ready(name)

    {:ok, conn: name, host: host, port: port}
  end

  test "put and get round-trip string bins", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"a" => 1, "b" => "x"})
    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["a"] == 1
    assert record.bins["b"] == "x"
  end

  test "put normalizes atom bin keys", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{name: "atom-key"})
    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["name"] == "atom-key"
  end

  test "put with exists create_only", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"v" => 1}, exists: :create_only)

    assert {:error, %{code: :key_exists}} =
             Aerospike.put(conn, key, %{"v" => 2}, exists: :create_only)
  end

  test "put with send_key true", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"v" => 1}, send_key: true)
    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["v"] == 1
  end

  test "get with selective bins", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"keep" => 1, "drop" => 2})
    assert {:ok, record} = Aerospike.get(conn, key, bins: ["keep"])
    assert Map.has_key?(record.bins, "keep")
    refute Map.has_key?(record.bins, "drop")
  end

  test "get with header_only", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"x" => 1})
    assert {:ok, record} = Aerospike.get(conn, key, header_only: true)
    assert record.bins == %{}
    assert record.generation >= 0
  end

  test "delete and exists", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert {:ok, false} = Aerospike.exists(conn, key)
    assert :ok = Aerospike.put(conn, key, %{"z" => 1})
    assert {:ok, true} = Aerospike.exists(conn, key)
    assert {:ok, true} = Aerospike.delete(conn, key)
    assert {:ok, false} = Aerospike.exists(conn, key)
    assert {:ok, false} = Aerospike.delete(conn, key)
  end

  test "touch refreshes record", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"n" => 1})
    assert :ok = Aerospike.touch(conn, key, [])
  end

  test "bang unwraps get!", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"q" => 1})
    record = Aerospike.get!(conn, key)
    assert record.bins["q"] == 1
  end

  test "put! raises on generation mismatch", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"g" => 1})
    assert {:ok, rec} = Aerospike.get(conn, key)
    gen = rec.generation

    assert_raise Aerospike.Error, fn ->
      Aerospike.put!(conn, key, %{"g" => 2}, generation: gen + 99, gen_policy: :expect_gen_equal)
    end
  end

  test "key_digest round-trip read", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"d" => 1})
    digest_key = Aerospike.key_digest(key.namespace, key.set, key.digest)
    assert {:ok, record} = Aerospike.get(conn, digest_key)
    assert record.bins["d"] == 1
  end

  test "invalid option returns parameter_error", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert {:error, %{code: :parameter_error}} =
             Aerospike.put(conn, key, %{x: 1}, not_a_valid_key: 1)
  end

  test "policy defaults merged: read timeout from defaults", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"m" => 1})
    assert {:ok, _} = Aerospike.get(conn, key, [])
  end

  test "telemetry start and stop for command", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    handler_id = "facade-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [[:aerospike, :command, :start], [:aerospike, :command, :stop]],
      &TelemetryForwarder.relay/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok = Aerospike.put!(conn, key, %{"t" => 1})

    assert_receive {:telemetry_cmd, [:aerospike, :command, :start], start_meta}
    assert start_meta.namespace == key.namespace
    assert start_meta.command == :put

    assert_receive {:telemetry_cmd, [:aerospike, :command, :stop], stop_meta}
    assert stop_meta.result == :ok
    assert is_binary(stop_meta.node)
  end

  test "telemetry stop metadata includes error code on failed get", %{
    conn: conn,
    host: host,
    port: port
  } do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    handler_id = "facade-test-err-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [[:aerospike, :command, :stop]],
      &TelemetryForwarder.relay/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:error, %{code: :key_not_found}} = Aerospike.get(conn, key)

    assert_receive {:telemetry_cmd, [:aerospike, :command, :stop], stop_meta}
    assert stop_meta.result == {:error, :key_not_found}
    assert is_binary(stop_meta.node)
  end

  test "put with exists update_only rejects missing key", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert {:error, %{code: :key_not_found}} =
             Aerospike.put(conn, key, %{"v" => 1}, exists: :update_only)

    :ok = Aerospike.put!(conn, key, %{"v" => 1})

    assert :ok = Aerospike.put(conn, key, %{"v" => 2}, exists: :update_only)
    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["v"] == 2
  end

  test "put with exists replace_only wipes prior bins", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"a" => 1, "b" => 2})
    :ok = Aerospike.put!(conn, key, %{"a" => 99}, exists: :replace_only)

    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["a"] == 99
    refute Map.has_key?(record.bins, "b")
  end

  test "put with exists create_or_replace upserts and wipes old bins", %{
    conn: conn,
    host: host,
    port: port
  } do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"x" => 1}, exists: :create_or_replace)
    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["x"] == 1

    assert :ok = Aerospike.put(conn, key, %{"y" => 2}, exists: :create_or_replace)
    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["y"] == 2
    refute Map.has_key?(record.bins, "x")
  end

  test "put with gen_policy expect_gen_gt", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"v" => 1})
    assert {:ok, rec} = Aerospike.get(conn, key)
    gen = rec.generation

    assert :ok =
             Aerospike.put(conn, key, %{"v" => 2},
               generation: gen + 1,
               gen_policy: :expect_gen_gt
             )

    assert {:error, %{code: :generation_error}} =
             Aerospike.put(conn, key, %{"v" => 3},
               generation: gen,
               gen_policy: :expect_gen_gt
             )
  end

  test "CAS round-trip with expect_gen_equal", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"counter" => 0})
    assert {:ok, rec} = Aerospike.get(conn, key)
    gen = rec.generation

    assert :ok =
             Aerospike.put(conn, key, %{"counter" => 1},
               generation: gen,
               gen_policy: :expect_gen_equal
             )

    assert {:ok, rec2} = Aerospike.get(conn, key)
    assert rec2.generation == gen + 1
    assert rec2.bins["counter"] == 1

    assert {:error, %{code: :generation_error}} =
             Aerospike.put(conn, key, %{"counter" => 2},
               generation: gen,
               gen_policy: :expect_gen_equal
             )
  end

  test "integer key put/get round-trip", %{conn: conn, host: host, port: port} do
    int_id = System.unique_integer([:positive])
    key = Aerospike.key("test", "facade_itest", int_id)
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"n" => "int-key-test"})
    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["n"] == "int-key-test"
  end

  test "put with ttl and readback ttl value", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"x" => 1}, ttl: 300)
    assert {:ok, record} = Aerospike.get(conn, key)
    # record.ttl is currently the raw server void_time (Aerospike epoch-relative);
    # conversion to seconds-remaining is future work
    assert record.ttl > 0
  end

  test "touch with explicit ttl extends expiration", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"x" => 1}, ttl: 60)
    assert {:ok, before} = Aerospike.get(conn, key)

    assert :ok = Aerospike.touch(conn, key, ttl: 600)
    assert {:ok, after_touch} = Aerospike.get(conn, key)

    assert after_touch.ttl > before.ttl
  end

  test "delete! returns boolean on success", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"z" => 1})
    assert Aerospike.delete!(conn, key) == true
    assert Aerospike.delete!(conn, key) == false
  end

  test "exists! returns boolean", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    refute Aerospike.exists!(conn, key)
    :ok = Aerospike.put!(conn, key, %{"z" => 1})
    assert Aerospike.exists!(conn, key)
  end

  test "touch! returns :ok", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"z" => 1})
    assert :ok = Aerospike.touch!(conn, key)
  end

  test "put with replica option succeeds", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "facade_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"r" => 1}, replica: 0)
    assert {:ok, record} = Aerospike.get(conn, key, replica: 0)
    assert record.bins["r"] == 1
  end

  test "send_key with integer key", %{conn: conn, host: host, port: port} do
    int_id = System.unique_integer([:positive])
    key = Aerospike.key("test", "facade_itest", int_id)
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"v" => 1}, send_key: true)
    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["v"] == 1
  end

  test "close stops supervisor with default timeout", %{conn: conn} do
    sup = Aerospike.Supervisor.sup_name(conn)
    assert Process.whereis(sup) != nil
    :ok = Aerospike.close(conn)
    refute Process.whereis(sup)
  end

  test "start_link/1 starts the supervision tree directly" do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"facade_sl_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 1,
      connect_timeout: 5_000,
      tend_interval: 60_000
    ]

    assert {:ok, pid} = Aerospike.start_link(opts)
    assert is_pid(pid)
    :ok = Aerospike.close(name)
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
end
