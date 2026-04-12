defmodule Aerospike.Integration.UDFTest do
  use ExUnit.Case, async: false

  alias Aerospike.Error
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  @moduletag :integration

  @udf_fixture Path.join([__DIR__, "..", "support", "fixtures", "test_udf.lua"])
  @package_name "test_udf"
  @server_name "test_udf.lua"

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"udf_itest_#{System.unique_integer([:positive])}"

    {:ok, _sup} =
      start_supervised(
        {Aerospike,
         name: name,
         hosts: ["#{host}:#{port}"],
         pool_size: 2,
         connect_timeout: 5_000,
         tend_interval: 60_000}
      )

    await_cluster_ready(name)

    # Remove leftover package from a previous run; ignore error if not present.
    Aerospike.remove_udf(name, @server_name)

    {:ok, task} = Aerospike.register_udf(name, @udf_fixture, @server_name)
    :ok = Aerospike.RegisterTask.wait(task, timeout: 10_000)

    {:ok, conn: name, host: host, port: port}
  end

  describe "apply_udf/5 — echo function" do
    test "returns the argument passed to the UDF (string)", %{conn: conn} do
      key = unique_key()
      :ok = Aerospike.put(conn, key, %{"v" => 1})

      assert {:ok, "hello"} = Aerospike.apply_udf(conn, key, @package_name, "echo", ["hello"])
    end

    test "returns integer argument", %{conn: conn} do
      key = unique_key()
      :ok = Aerospike.put(conn, key, %{"v" => 1})

      assert {:ok, 42} = Aerospike.apply_udf(conn, key, @package_name, "echo", [42])
    end

    test "returns nil when UDF returns nil (empty arg list passed as nil)", %{conn: conn} do
      key = unique_key()
      :ok = Aerospike.put(conn, key, %{"v" => 1})

      result = Aerospike.apply_udf(conn, key, @package_name, "echo", [nil])
      assert match?({:ok, _}, result)
    end
  end

  describe "apply_udf/6 — with options" do
    test "accepts timeout option", %{conn: conn} do
      key = unique_key()
      :ok = Aerospike.put(conn, key, %{"v" => 1})

      assert {:ok, "world"} =
               Aerospike.apply_udf(conn, key, @package_name, "echo", ["world"], timeout: 5_000)
    end

    test "returns error for invalid option", %{conn: conn} do
      key = unique_key()

      assert {:error, %Error{code: :parameter_error}} =
               Aerospike.apply_udf(conn, key, @package_name, "echo", [], invalid_opt: true)
    end
  end

  describe "apply_udf/5 — error handling" do
    test "returns error for unknown UDF package", %{conn: conn} do
      key = unique_key()
      :ok = Aerospike.put(conn, key, %{"v" => 1})

      assert {:error, %Error{}} =
               Aerospike.apply_udf(conn, key, "no_such_package", "echo", ["x"])
    end

    test "returns error for unknown function in package", %{conn: conn} do
      key = unique_key()
      :ok = Aerospike.put(conn, key, %{"v" => 1})

      assert {:error, %Error{}} =
               Aerospike.apply_udf(conn, key, @package_name, "no_such_function", [])
    end
  end

  describe "udf inventory lifecycle" do
    test "list_udfs/1 reflects registration and removal", %{conn: conn} do
      assert {:ok, udfs} = Aerospike.list_udfs(conn)

      assert %Aerospike.UDF{filename: @server_name, language: "LUA", hash: hash} =
               Enum.find(udfs, &(&1.filename == @server_name))

      assert is_binary(hash)
      assert hash != ""

      assert :ok = Aerospike.remove_udf(conn, @server_name)

      assert_eventually(
        fn ->
          case Aerospike.list_udfs(conn) do
            {:ok, udfs2} ->
              removed? = Enum.all?(udfs2, &(&1.filename != @server_name))
              {removed?, "udf #{inspect(@server_name)} still present: #{inspect(udfs2)}"}

            {:error, err} ->
              {false, "list_udfs failed after remove: #{inspect(err)}"}
          end
        end,
        timeout: 10_000,
        interval: 200
      )
    end
  end

  describe "demo parity — lifecycle and batch UDF" do
    test "register inline package, mutate record, remove package", %{
      conn: conn,
      host: host,
      port: port
    } do
      package = "demo_parity_udf_#{System.unique_integer([:positive])}"
      server_name = "#{package}.lua"
      key = unique_key()

      source = """
      function put_value(rec, bin_name, value)
        rec[bin_name] = value
        aerospike:update(rec)
        return rec[bin_name]
      end
      """

      on_exit(fn ->
        Helpers.cleanup_key(key, host: host, port: port)
        maybe_remove_udf(conn, server_name)
      end)

      assert {:ok, task} = Aerospike.register_udf(conn, source, server_name)
      assert :ok = Aerospike.RegisterTask.wait(task, timeout: 10_000)
      Process.sleep(300)

      assert :ok = Aerospike.put(conn, key, %{"v" => 1})
      assert {:ok, 99} = Aerospike.apply_udf(conn, key, package, "put_value", ["z", 99])
      assert {:ok, rec} = Aerospike.get(conn, key)
      assert rec.bins["z"] == 99

      assert :ok = Aerospike.remove_udf(conn, server_name)
      assert {:error, %Error{}} = Aerospike.apply_udf(conn, key, package, "put_value", ["z", 100])
    end
  end

  defp unique_key do
    suffix = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    Aerospike.key("test", "udf_itest", suffix)
  end

  defp maybe_remove_udf(conn, server_name) do
    if :ets.whereis(Tables.meta(conn)) != :undefined do
      _ = Aerospike.remove_udf(conn, server_name)
    end

    :ok
  end

  defp assert_eventually(fun, opts) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.fetch!(opts, :timeout)
    interval = Keyword.get(opts, :interval, 200)
    deadline = System.monotonic_time(:millisecond) + timeout
    assert_eventually_loop(fun, deadline, interval, nil)
  end

  defp assert_eventually_loop(fun, deadline, interval, last_message) do
    case fun.() do
      true ->
        :ok

      {true, _message} ->
        :ok

      false ->
        await_or_flunk(fun, deadline, interval, last_message || "condition returned false")

      {false, message} when is_binary(message) ->
        await_or_flunk(fun, deadline, interval, message)
    end
  end

  defp await_or_flunk(fun, deadline, interval, message) do
    if System.monotonic_time(:millisecond) > deadline do
      flunk("condition did not become true within timeout: #{message}")
    else
      Process.sleep(interval)
      assert_eventually_loop(fun, deadline, interval, message)
    end
  end

  defp await_cluster_ready(name, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_cluster_ready_loop(name, deadline)
  end

  defp await_cluster_ready_loop(name, deadline) do
    cond do
      match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key())) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("cluster not ready within timeout")

      true ->
        Process.sleep(50)
        await_cluster_ready_loop(name, deadline)
    end
  end
end
