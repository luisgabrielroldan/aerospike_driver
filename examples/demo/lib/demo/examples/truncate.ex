defmodule Demo.Examples.Truncate do
  @moduledoc """
  Demonstrates `Aerospike.truncate/4`: removing all records in a set.
  """

  require Logger

  alias Aerospike.Scan

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_trunc"
  @count 10

  def run do
    write_records()
    verify_count()
    truncate_set()
    verify_empty()
  end

  defp write_records do
    Logger.info("  Writing #{@count} records...")

    for i <- 1..@count do
      key = Aerospike.key(@namespace, @set, "trunc_#{i}")
      :ok = @repo.put!(key, %{"idx" => i})
    end
  end

  defp verify_count do
    {:ok, n} = @repo.count(Scan.new(@namespace, @set))

    unless n >= @count do
      raise "Expected at least #{@count} records, got #{n}"
    end

    Logger.info("  Count before truncate: #{n}")
  end

  defp truncate_set do
    :ok = @repo.truncate(@namespace, @set)
    Logger.info("  Truncated set '#{@set}'")
    Process.sleep(1_000)
  end

  defp verify_empty do
    {:ok, n} = @repo.count(Scan.new(@namespace, @set))
    Logger.info("  Count after truncate: #{n}")

    unless n == 0 do
      raise "Expected 0 records after truncate, got #{n}"
    end
  end
end
