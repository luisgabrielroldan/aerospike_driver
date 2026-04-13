defmodule Aerospike.Integration.QueryExecuteTest do
  use ExUnit.Case, async: false

  import Aerospike.Op

  alias Aerospike.ExecuteTask
  alias Aerospike.Filter
  alias Aerospike.Query
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  @moduletag :integration

  @namespace "test"

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    conn = :"query_execute_itest_#{System.unique_integer([:positive, :monotonic])}"
    set = "query_execute_#{System.unique_integer([:positive, :monotonic])}"
    index_name = "age_idx_#{System.unique_integer([:positive, :monotonic])}"
    package = "query_execute_udf_#{System.unique_integer([:positive, :monotonic])}"
    server_name = "#{package}.lua"

    {:ok, _sup} =
      start_supervised(
        {Aerospike,
         name: conn,
         hosts: ["#{host}:#{port}"],
         pool_size: 2,
         connect_timeout: 5_000,
         recv_timeout: 60_000,
         tend_interval: 60_000,
         defaults: [query: [timeout: 60_000], write: [timeout: 5_000], read: [timeout: 5_000]]}
      )

    await_cluster_ready(conn)

    udf_source = """
    function put_value(rec, bin_name, value)
      rec[bin_name] = value
      aerospike:update(rec)
      return value
    end

    local function sum_reducer(left, right)
      return left + right
    end

    function sum_bin(stream, bin_name)
      local function mapper(rec)
        return rec[bin_name] or 0
      end

      return stream : map(mapper) : reduce(sum_reducer)
    end
    """

    {:ok, register_task} = Aerospike.register_udf(conn, udf_source, server_name)
    assert :ok = Aerospike.RegisterTask.wait(register_task, timeout: 10_000, poll_interval: 200)

    {:ok, index_task} =
      Aerospike.create_index(conn, @namespace, set,
        bin: "age",
        name: index_name,
        type: :numeric
      )

    assert :ok = Aerospike.IndexTask.wait(index_task, timeout: 30_000, poll_interval: 200)

    keys =
      for age <- 20..29 do
        key = Aerospike.key(@namespace, set, "user_#{age}")
        :ok = Aerospike.put!(conn, key, %{"age" => age, "state" => "new"})
        key
      end

    Process.sleep(1_000)

    on_exit(fn ->
      Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port))
      Helpers.cleanup_index(@namespace, index_name, host: host, port: port)
      Helpers.cleanup_udf(server_name, host: host, port: port)
    end)

    {:ok, conn: conn, set: set, package: package, keys: keys}
  end

  test "query_execute/4 applies write operations and returns a waitable task", %{
    conn: conn,
    set: set,
    keys: keys
  } do
    query =
      Query.new(@namespace, set)
      |> Query.where(Filter.range("age", 20, 24))

    assert {:ok, %ExecuteTask{} = task} =
             Aerospike.query_execute(conn, query, [put("state", "executed")])

    assert task.kind == :query_execute
    assert is_integer(task.task_id)
    assert :ok = ExecuteTask.wait(task, timeout: 30_000, poll_interval: 200)
    assert {:ok, :complete} = ExecuteTask.status(task)

    keys
    |> Enum.take(5)
    |> Enum.each(fn key ->
      assert {:ok, record} = Aerospike.get(conn, key)
      assert record.bins["state"] == "executed"
    end)

    keys
    |> Enum.drop(5)
    |> Enum.each(fn key ->
      assert {:ok, record} = Aerospike.get(conn, key)
      assert record.bins["state"] == "new"
    end)
  end

  test "query_udf/6 applies record UDFs across matching records", %{
    conn: conn,
    set: set,
    package: package,
    keys: keys
  } do
    query =
      Query.new(@namespace, set)
      |> Query.where(Filter.range("age", 25, 29))

    assert {:ok, %ExecuteTask{} = task} =
             Aerospike.query_udf(conn, query, package, "put_value", ["state", "udf"])

    assert task.kind == :query_udf
    assert :ok = ExecuteTask.wait(task, timeout: 30_000, poll_interval: 200)
    assert {:ok, :complete} = ExecuteTask.status(task)

    keys
    |> Enum.take(5)
    |> Enum.each(fn key ->
      assert {:ok, record} = Aerospike.get(conn, key)
      assert record.bins["state"] == "new"
    end)

    keys
    |> Enum.drop(5)
    |> Enum.each(fn key ->
      assert {:ok, record} = Aerospike.get(conn, key)
      assert record.bins["state"] == "udf"
    end)
  end

  test "query_aggregate/6 streams aggregate values instead of records", %{
    conn: conn,
    set: set,
    package: package
  } do
    query =
      Query.new(@namespace, set)
      |> Query.where(Filter.range("age", 20, 24))

    assert {:ok, stream} = Aerospike.query_aggregate(conn, query, package, "sum_bin", ["age"])
    results = Enum.to_list(stream)

    assert results != []
    assert Enum.all?(results, &is_integer/1)
    assert Enum.sum(results) == 110
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
