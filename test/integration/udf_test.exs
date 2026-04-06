defmodule Aerospike.Integration.UDFTest do
  use ExUnit.Case, async: false

  alias Aerospike.Error
  alias Aerospike.Tables

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

    {:ok, conn: name}
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

  defp unique_key do
    suffix = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    Aerospike.key("test", "udf_itest", suffix)
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
