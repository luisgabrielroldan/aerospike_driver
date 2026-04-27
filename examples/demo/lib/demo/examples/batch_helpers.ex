defmodule Demo.Examples.BatchHelpers do
  @moduledoc """
  Demonstrates direct batch helpers for read operations, deletes, and UDF calls.
  """

  require Logger

  alias Aerospike.Error

  import Aerospike.Op, only: [get: 1]

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_batch_helpers"
  @package "demo_batch_helpers"

  @lua_source """
  function add_score(rec, bin_name, amount)
    rec[bin_name] = (rec[bin_name] or 0) + amount
    aerospike:update(rec)
    return rec[bin_name]
  end
  """

  def run do
    cleanup()
    register_udf()

    try do
      write_records()
      batch_get_operate()
      batch_udf()
      batch_delete()
    after
      remove_udf()
      cleanup()
    end
  end

  defp register_udf do
    Logger.info("  Registering UDF package '#{@package}.lua'...")
    {:ok, task} = @repo.register_udf(@lua_source, "#{@package}.lua")
    :ok = Aerospike.RegisterTask.wait(task, timeout: 10_000)
    Process.sleep(500)
    Logger.info("  UDF package registered.")
  end

  defp write_records do
    Logger.info("  Writing records for direct batch helpers...")

    :ok = @repo.put!(key("a"), %{"name" => "alpha", "score" => 10})
    :ok = @repo.put!(key("b"), %{"name" => "bravo", "score" => 20})
    :ok = @repo.put!(key("delete_me"), %{"temp" => true})
  end

  defp batch_get_operate do
    Logger.info("  batch_get_operate with read-only operations...")

    {:ok, [first, second, third]} =
      @repo.batch_get_operate(
        [key("a"), key("missing_read"), key("b")],
        [get("name"), get("score")],
        timeout: 10_000
      )

    assert_read_result(first, "alpha", 10)
    assert_key_not_found(second)
    assert_read_result(third, "bravo", 20)

    Logger.info("    Read helper preserved caller order and missing-key result.")
  end

  defp batch_udf do
    Logger.info("  batch_udf applying one UDF to multiple records...")

    {:ok, [first, second]} =
      @repo.batch_udf([key("a"), key("b")], @package, "add_score", ["score", 7], timeout: 10_000)

    assert_udf_result(first, 17)
    assert_udf_result(second, 27)

    Logger.info("    UDF helper returned per-record SUCCESS values.")

    assert_score("a", 17)
    assert_score("b", 27)

    Logger.info("    Persisted UDF writes verified.")
  end

  defp batch_delete do
    Logger.info("  batch_delete with an existing key and a missing key...")

    {:ok, [first, second]} =
      @repo.batch_delete([key("delete_me"), key("missing_delete")], timeout: 10_000)

    assert_delete_result(first)
    assert_delete_missing(second)

    {:ok, false} = @repo.exists(key("delete_me"))

    Logger.info("    Delete helper removed the existing record and kept the missing-key error.")
  end

  defp assert_read_result({:ok, record}, name, score) do
    unless record.bins["name"] == name and record.bins["score"] == score do
      raise "Unexpected batch_get_operate record: #{inspect(record)}"
    end
  end

  defp assert_read_result(result, _name, _score) do
    raise "Expected batch_get_operate success, got #{inspect(result)}"
  end

  defp assert_key_not_found({:error, %Error{code: :key_not_found}}), do: :ok

  defp assert_key_not_found(result) do
    raise "Expected key_not_found, got #{inspect(result)}"
  end

  defp assert_udf_result(%{status: :ok, record: %{bins: %{"SUCCESS" => expected}}}, expected),
    do: :ok

  defp assert_udf_result(result, expected) do
    raise "Expected batch_udf SUCCESS=#{expected}, got #{inspect(result)}"
  end

  defp assert_score(id, expected) do
    {:ok, record} = @repo.get(key(id))

    unless record.bins["score"] == expected do
      raise "Expected score=#{expected} for #{id}, got #{inspect(record.bins["score"])}"
    end
  end

  defp assert_delete_result(%{status: :ok, record: nil, error: nil}), do: :ok

  defp assert_delete_result(result) do
    raise "Expected batch_delete success, got #{inspect(result)}"
  end

  defp assert_delete_missing(%{status: :error, error: %Error{code: :key_not_found}}), do: :ok

  defp assert_delete_missing(result) do
    raise "Expected batch_delete key_not_found, got #{inspect(result)}"
  end

  defp remove_udf do
    Logger.info("  Removing UDF package '#{@package}.lua'...")
    :ok = @repo.remove_udf("#{@package}.lua")
    Logger.info("  UDF package removed.")
  end

  defp cleanup do
    for id <- ["a", "b", "delete_me", "missing_read", "missing_delete"] do
      @repo.delete(key(id))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
