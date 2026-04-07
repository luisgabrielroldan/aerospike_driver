defmodule Demo.Examples.PartitionFilter do
  @moduledoc """
  Demonstrates partition-targeted scans using `Aerospike.PartitionFilter`.

  Shows `by_id/1` (single partition) and `by_range/2` (contiguous partition range)
  to restrict which partitions a scan visits, useful for parallel fan-out or
  incremental processing.
  """

  require Logger

  alias Aerospike.PartitionFilter, as: PF
  alias Aerospike.Scan

  @conn :aero
  @namespace "test"
  @set "demo_pf"
  @size 50

  def run do
    write_records()
    scan_single_partition()
    scan_partition_range()
    cleanup()
  end

  defp write_records do
    Logger.info("  Writing #{@size} records across partitions...")

    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "pf_#{i}")
      bins = %{"idx" => i}
      :ok = Aerospike.put!(@conn, key, bins)
    end
  end

  defp scan_single_partition do
    target_key = Aerospike.key(@namespace, @set, "pf_1")
    partition_id = Aerospike.Key.partition_id(target_key)
    Logger.info("  Scan single partition #{partition_id} (where pf_1 lives)...")

    scan =
      Scan.new(@namespace, @set)
      |> Scan.partition_filter(PF.by_id(partition_id))
      |> Scan.max_records(100)

    {:ok, records} = Aerospike.all(@conn, scan)
    Logger.info("    Found #{length(records)} records in partition #{partition_id}")

    if records == [] do
      raise "Expected at least 1 record in partition #{partition_id}"
    end
  end

  defp scan_partition_range do
    begin_part = 0
    count = 512

    Logger.info(
      "  Scan partition range #{begin_part}..#{begin_part + count - 1} (#{count} of 4096)..."
    )

    scan =
      Scan.new(@namespace, @set)
      |> Scan.partition_filter(PF.by_range(begin_part, count))
      |> Scan.max_records(200)

    {:ok, records} = Aerospike.all(@conn, scan)
    Logger.info("    Found #{length(records)} records in partition range")

    full_scan = Scan.new(@namespace, @set) |> Scan.max_records(200)
    {:ok, all_records} = Aerospike.all(@conn, full_scan)

    Logger.info("    Partition subset: #{length(records)} / #{length(all_records)} total records")
  end

  defp cleanup do
    for i <- 1..@size do
      Aerospike.delete(@conn, Aerospike.key(@namespace, @set, "pf_#{i}"))
    end
  end
end
