defmodule Demo.Examples.CountSetObjects do
  @moduledoc """
  Demonstrates counting records in a set using `Aerospike.count/2`.

  Writes a known number of records, then uses a scan-based count to verify
  the server reports the correct total. This is the idiomatic Elixir approach —
  the Go example used the info protocol directly.
  """

  require Logger

  alias Aerospike.Scan

  @conn :aero
  @namespace "test"
  @set "demo_count"
  @size 10

  def run do
    write_records()
    count_records()
    cleanup()
  end

  defp write_records do
    Logger.info("  Writing #{@size} records for count test...")

    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "cnt_#{i}")
      bins = %{"data" => "value_#{i}"}
      :ok = Aerospike.put!(@conn, key, bins)
    end
  end

  defp count_records do
    Logger.info("  Counting records in set '#{@set}'...")

    scan = Scan.new(@namespace, @set)
    {:ok, count} = Aerospike.count(@conn, scan)

    Logger.info("  Count: #{count} records in #{@namespace}/#{@set}")

    unless count >= @size do
      raise "Expected at least #{@size} records, got #{count}"
    end

    Logger.info("  Count verified: #{count} >= #{@size}")
  end

  defp cleanup do
    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "cnt_#{i}")
      Aerospike.delete(@conn, key)
    end
  end
end
