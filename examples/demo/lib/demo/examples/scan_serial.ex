defmodule Demo.Examples.ScanSerial do
  @moduledoc """
  Demonstrates eager scan of all records in a namespace/set using `Aerospike.all/2`.

  Writes a batch of records, then performs a bounded scan to retrieve them.
  Also shows bin projection with `Scan.select/2`.
  """

  require Logger

  alias Aerospike.Scan

  @conn :aero
  @namespace "test"
  @set "demo_scan"
  @size 10

  def run do
    write_records()
    scan_all()
    scan_with_select()
    scan_namespace_wide()
    cleanup()
  end

  defp write_records do
    Logger.info("  Writing #{@size} records for scan...")

    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "scan_#{i}")
      bins = %{"name" => "user_#{i}", "age" => 20 + i, "city" => "Portland"}
      :ok = Aerospike.put!(@conn, key, bins)
    end
  end

  defp scan_all do
    Logger.info("  Scan all records in set (max_records: #{@size + 5})...")

    scan = Scan.new(@namespace, @set) |> Scan.max_records(@size + 5)
    {:ok, records} = Aerospike.all(@conn, scan)

    Logger.info("  Retrieved #{length(records)} records:")

    for r <- Enum.take(records, 3) do
      Logger.info("    name=#{r.bins["name"]} age=#{r.bins["age"]} city=#{r.bins["city"]}")
    end

    if length(records) > 3, do: Logger.info("    ... and #{length(records) - 3} more")

    unless length(records) >= @size do
      raise "Expected at least #{@size} records, got #{length(records)}"
    end
  end

  defp scan_with_select do
    Logger.info("  Scan with bin projection (select only 'name')...")

    scan =
      Scan.new(@namespace, @set)
      |> Scan.select(["name"])
      |> Scan.max_records(@size + 5)

    {:ok, records} = Aerospike.all(@conn, scan)

    for r <- Enum.take(records, 3) do
      unless Map.has_key?(r.bins, "name") do
        raise "Expected 'name' bin in projected scan result"
      end

      if Map.has_key?(r.bins, "age") do
        raise "Did not expect 'age' bin in projected scan result"
      end

      Logger.info("    name=#{r.bins["name"]} (age and city excluded)")
    end

    Logger.info("  Bin projection verified on #{length(records)} records.")
  end

  defp scan_namespace_wide do
    Logger.info("  Scan namespace-wide (max_records: 5)...")

    scan = Scan.new(@namespace) |> Scan.max_records(5)
    {:ok, records} = Aerospike.all(@conn, scan)

    Logger.info("  Namespace scan returned #{length(records)} records (capped at 5).")
  end

  defp cleanup do
    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "scan_#{i}")
      Aerospike.delete(@conn, key)
    end
  end
end
