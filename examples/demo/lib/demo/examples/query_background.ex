defmodule Demo.Examples.QueryBackground do
  @moduledoc """
  Demonstrates background query write jobs and background query UDF jobs.

  Creates a secondary index, starts background jobs with `query_execute/3` and
  `query_udf/5`, waits for each `Aerospike.ExecuteTask`, and verifies the
  mutated records with normal reads.
  """

  require Logger

  alias Aerospike.ExecuteTask
  alias Aerospike.Filter
  alias Aerospike.Query

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_query_background"
  @index_name "demo_query_background_score_idx"
  @package "demo_query_background"
  @server_name "#{@package}.lua"
  @records [
    {"a", 10},
    {"b", 20},
    {"c", 30}
  ]

  @lua_source """
  function mark_background(rec, bin_name, value)
    rec[bin_name] = value
    aerospike:update(rec)
  end
  """

  def run do
    cleanup()

    try do
      register_udf()
      create_index()
      write_records()
      query_execute()
      query_udf()
    after
      cleanup()
    end
  end

  defp register_udf do
    Logger.info("  Registering background query UDF package '#{@server_name}'...")

    {:ok, task} = @repo.register_udf(@lua_source, @server_name)
    :ok = Aerospike.RegisterTask.wait(task, timeout: 10_000)
    Process.sleep(500)

    Logger.info("  UDF package registered.")
  end

  defp create_index do
    Logger.info("  Creating numeric index on 'score'...")

    {:ok, task} =
      @repo.create_index(@namespace, @set,
        bin: "score",
        name: @index_name,
        type: :numeric
      )

    :ok = Aerospike.IndexTask.wait(task, timeout: 15_000)
    Process.sleep(500)

    Logger.info("  Index ready.")
  end

  defp write_records do
    Logger.info("  Writing records for background queries...")

    for {id, score} <- @records do
      :ok = @repo.put!(key(id), %{"score" => score, "execute_marker" => "pending"})
    end
  end

  defp query_execute do
    Logger.info("  Starting query_execute background write...")

    {:ok, %ExecuteTask{} = task} =
      @repo.query_execute(query(), [{:write, :execute_marker, "complete"}],
        timeout: 5_000,
        task_timeout: 10_000
      )

    :ok = ExecuteTask.wait(task, timeout: 15_000, poll_interval: 200)
    assert_bin_eventually("execute_marker", "complete")

    Logger.info("    query_execute completed and marked all records.")
  end

  defp query_udf do
    Logger.info("  Starting query_udf background record UDF...")

    {:ok, %ExecuteTask{} = task} =
      @repo.query_udf(query(), @package, "mark_background", ["udf_marker", "complete"],
        timeout: 5_000,
        task_timeout: 10_000
      )

    :ok = ExecuteTask.wait(task, timeout: 15_000, poll_interval: 200)
    assert_bin_eventually("udf_marker", "complete")

    Logger.info("    query_udf completed and marked all records.")
  end

  defp assert_bin_eventually(bin_name, expected),
    do: assert_bin_eventually(bin_name, expected, 20)

  defp assert_bin_eventually(bin_name, expected, 0) do
    values = current_values(bin_name)
    raise "Expected #{bin_name}=#{inspect(expected)} on all records, got #{inspect(values)}"
  end

  defp assert_bin_eventually(bin_name, expected, attempts) when attempts > 0 do
    values = current_values(bin_name)

    if Enum.all?(values, &(&1 == expected)) do
      :ok
    else
      Process.sleep(250)
      assert_bin_eventually(bin_name, expected, attempts - 1)
    end
  end

  defp current_values(bin_name) do
    Enum.map(@records, fn {id, _score} ->
      {:ok, record} = @repo.get(key(id))
      record.bins[bin_name]
    end)
  end

  defp query do
    Query.new(@namespace, @set)
    |> Query.where(Filter.range("score", 10, 30))
    |> Query.max_records(length(@records))
  end

  defp cleanup do
    _ = @repo.drop_index(@namespace, @index_name)
    _ = @repo.remove_udf(@server_name)

    for {id, _score} <- @records do
      _ = @repo.delete(key(id))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
