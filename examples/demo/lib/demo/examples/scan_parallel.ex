defmodule Demo.Examples.ScanParallel do
  @moduledoc """
  Demonstrates lazy streaming scan using `Aerospike.stream!/2`.

  In the Elixir driver, scans automatically fan out to all cluster nodes in
  parallel. `stream!/2` returns a `Stream` that yields records lazily — ideal
  for large datasets or pipeline processing.

  Also demonstrates early termination with `Enum.take/2`.
  """

  require Logger

  alias Aerospike.Scan

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_scan_par"
  @size 15

  def run do
    write_records()
    stream_all()
    stream_with_pipeline()
    stream_early_termination()
    cleanup()
  end

  defp write_records do
    Logger.info("  Writing #{@size} records for stream scan...")

    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "spar_#{i}")
      bins = %{"name" => "user_#{i}", "score" => i * 10}
      :ok = @repo.put!(key, bins)
    end
  end

  defp stream_all do
    Logger.info("  Streaming all records with stream!/2...")

    scan = Scan.new(@namespace, @set) |> Scan.max_records(@size + 5)

    records =
      @repo.stream!(scan)
      |> Enum.to_list()

    Logger.info("  Streamed #{length(records)} records.")

    unless length(records) >= @size do
      raise "Expected at least #{@size} records, got #{length(records)}"
    end
  end

  defp stream_with_pipeline do
    Logger.info("  Stream pipeline: filter scores > 50, extract names...")

    scan = Scan.new(@namespace, @set) |> Scan.max_records(@size + 5)

    names =
      @repo.stream!(scan)
      |> Stream.filter(fn r -> r.bins["score"] > 50 end)
      |> Stream.map(fn r -> r.bins["name"] end)
      |> Enum.to_list()

    Logger.info(
      "  Found #{length(names)} records with score > 50: #{Enum.take(names, 5) |> Enum.join(", ")}"
    )
  end

  defp stream_early_termination do
    Logger.info("  Stream with early termination (take 3)...")

    scan = Scan.new(@namespace, @set)

    records =
      @repo.stream!(scan)
      |> Enum.take(3)

    unless length(records) == 3 do
      raise "Expected exactly 3 records from Enum.take, got #{length(records)}"
    end

    Logger.info("  Early termination: got exactly #{length(records)} records.")
  end

  defp cleanup do
    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "spar_#{i}")
      @repo.delete(key)
    end
  end
end
