defmodule Demo.Examples.QueryAggregateResult do
  @moduledoc """
  Demonstrates server-side Lua aggregation with local finalization from a file.

  Registers a Lua package from `priv/udf`, writes indexed records, and calls
  `query_aggregate_result/6` with `source_path:` so the client can finalize the
  server aggregate stream into one value.
  """

  require Logger

  alias Aerospike.Filter
  alias Aerospike.Query

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_qagg_result"
  @index_name "demo_qagg_result_age_idx"
  @package "demo_query_aggregate_result"
  @server_name "#{@package}.lua"
  @source_path Path.expand("../../../priv/udf/query_aggregate_result.lua", __DIR__)
  @people [
    {"alice", 20},
    {"bob", 21},
    {"carol", 22},
    {"dave", 23},
    {"eve", 24}
  ]

  def run do
    cleanup()

    try do
      register_udf()
      create_index()
      write_records()
      query_aggregate_result()
    after
      cleanup()
    end
  end

  defp register_udf do
    Logger.info("  Registering file-backed UDF package '#{@server_name}'...")

    {:ok, task} = @repo.register_udf(@source_path, @server_name)
    :ok = Aerospike.RegisterTask.wait(task, timeout: 10_000)
    Process.sleep(500)

    Logger.info("  UDF package registered from #{@source_path}.")
  end

  defp create_index do
    Logger.info("  Creating numeric index on 'age'...")

    {:ok, task} =
      @repo.create_index(@namespace, @set,
        bin: "age",
        name: @index_name,
        type: :numeric
      )

    :ok = Aerospike.IndexTask.wait(task, timeout: 15_000)
    Process.sleep(500)

    Logger.info("  Index ready.")
  end

  defp write_records do
    Logger.info("  Writing records for aggregate finalization...")

    for {name, age} <- @people do
      :ok = @repo.put!(key(name), %{"name" => name, "age" => age})
    end
  end

  defp query_aggregate_result do
    Logger.info("  Running query_aggregate_result with source_path...")

    query =
      Query.new(@namespace, @set)
      |> Query.where(Filter.range("age", 20, 24))
      |> Query.max_records(length(@people))

    expected = @people |> Enum.map(fn {_name, age} -> age end) |> Enum.sum()

    assert_aggregate_sum(query, expected)

    Logger.info("    Finalized age sum: #{expected}.")
  end

  defp assert_aggregate_sum(query, expected), do: assert_aggregate_sum(query, expected, 20, nil)

  defp assert_aggregate_sum(_query, expected, 0, last_result) do
    raise "Expected finalized sum #{expected}, got #{inspect(last_result)}"
  end

  defp assert_aggregate_sum(query, expected, attempts, _last_result) when attempts > 0 do
    result =
      @repo.query_aggregate_result(query, @package, "sum_bin", ["age"],
        source_path: @source_path,
        timeout: 10_000
      )

    case result do
      {:ok, ^expected} ->
        :ok

      _other ->
        Process.sleep(250)
        assert_aggregate_sum(query, expected, attempts - 1, result)
    end
  end

  defp cleanup do
    _ = @repo.drop_index(@namespace, @index_name)
    _ = @repo.remove_udf(@server_name)

    for {name, _age} <- @people do
      _ = @repo.delete(key(name))
    end
  end

  defp key(name), do: Aerospike.key(@namespace, @set, name)
end
