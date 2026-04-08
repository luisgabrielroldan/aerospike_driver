defmodule Demo.Examples.BatchOperate do
  @moduledoc """
  Demonstrates `Aerospike.batch_operate/2` with heterogeneous operations:
  batch reads, writes, deletes, and atomic operate in a single round-trip.
  """

  require Logger

  alias Aerospike.Batch

  import Aerospike.Op, only: [add: 2, get: 1]

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_bop"

  def run do
    setup_records()
    batch_mixed_operations()
    batch_atomic_operate()
    cleanup()
  end

  defp setup_records do
    Logger.info("  Setting up records for batch_operate...")

    :ok = @repo.put!(key("read_me"), %{"greeting" => "hello", "count" => 1})
    :ok = @repo.put!(key("delete_me"), %{"temp" => true})
    :ok = @repo.put!(key("operate_me"), %{"counter" => 100})
  end

  defp batch_mixed_operations do
    Logger.info("  batch_operate with mixed ops: read + write + delete...")

    {:ok, results} =
      @repo.batch_operate([
        Batch.read(key("read_me"), bins: ["greeting"]),
        Batch.put(key("new_record"), %{"created_by" => "batch"}),
        Batch.delete(key("delete_me"))
      ])

    [read_result, write_result, delete_result] = results

    unless read_result.status == :ok do
      raise "Batch read failed: #{inspect(read_result)}"
    end

    Logger.info("    Read: greeting=#{read_result.record.bins["greeting"]}")

    unless write_result.status == :ok do
      raise "Batch write failed: #{inspect(write_result)}"
    end

    Logger.info("    Write: new_record created")

    unless delete_result.status == :ok do
      raise "Batch delete failed: #{inspect(delete_result)}"
    end

    Logger.info("    Delete: delete_me removed")

    {:ok, record} = @repo.get(key("new_record"))

    unless record.bins["created_by"] == "batch" do
      raise "Expected new_record with created_by=batch"
    end

    Logger.info("  Verified: new_record exists with created_by=batch")

    case @repo.exists(key("delete_me")) do
      {:ok, false} ->
        Logger.info("  Verified: delete_me no longer exists")

      {:ok, true} ->
        raise "delete_me should have been removed by batch_operate"
    end
  end

  defp batch_atomic_operate do
    Logger.info("  batch_operate with atomic add+get on a single record...")

    {:ok, [result]} =
      @repo.batch_operate([
        Batch.operate(key("operate_me"), [add("counter", 50), get("counter")])
      ])

    unless result.status == :ok do
      raise "Batch operate failed: #{inspect(result)}"
    end

    value = result.record.bins["counter"]

    unless value == 150 do
      raise "Expected counter=150 (100+50), got #{value}"
    end

    Logger.info("    Operate: counter=#{value} (100 + 50 = 150)")
  end

  defp cleanup do
    for suffix <- ["read_me", "delete_me", "new_record", "operate_me"] do
      @repo.delete(key(suffix))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
