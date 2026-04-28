defmodule Demo.Examples.PolicyOptions do
  @moduledoc """
  Demonstrates keyword policy options across record, batch, and scan calls.
  """

  require Logger

  alias Aerospike.Scan

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_policy_options"
  @size 4

  def run do
    cleanup()

    try do
      write_with_policy_options()
      read_with_policy_options()
      batch_with_parent_options()
      scan_with_runtime_options()
    after
      cleanup()
    end
  end

  defp write_with_policy_options do
    Logger.info(
      "  Writing records with timeout, socket, generation, commit, and send-key opts..."
    )

    for i <- 1..@size do
      :ok =
        @repo.put!(key(i), bins(i),
          timeout: 5_000,
          socket_timeout: 1_000,
          generation: 0,
          generation_policy: :none,
          commit_level: :all,
          send_key: true
        )
    end

    Logger.info("    Wrote #{@size} records with write policy options.")
  end

  defp read_with_policy_options do
    Logger.info("  Reading a record with read policy options...")

    {:ok, record} =
      @repo.get(key(1),
        timeout: 5_000,
        socket_timeout: 1_000,
        replica_policy: :sequence,
        read_touch_ttl_percent: -1,
        send_key: true
      )

    assert_bin(record, "name", "policy-1")

    Logger.info("    Read policy options accepted and returned name=policy-1.")
  end

  defp batch_with_parent_options do
    Logger.info("  Batch reading records with parent dispatch options...")

    keys = for i <- 1..@size, do: key(i)

    {:ok, records} =
      @repo.batch_get(keys,
        timeout: 5_000,
        socket_timeout: 1_000,
        max_concurrent_nodes: 1,
        allow_partial_results: false,
        read_touch_ttl_percent: -1
      )

    names = Enum.map(records, & &1.bins["name"])

    unless names == for(i <- 1..@size, do: "policy-#{i}") do
      raise "Unexpected batch names: #{inspect(names)}"
    end

    Logger.info("    Batch parent options accepted and returned #{@size} records.")
  end

  defp scan_with_runtime_options do
    Logger.info("  Scanning the set with runtime options...")

    scan =
      @namespace
      |> Scan.new(@set)
      |> Scan.max_records(100)
      |> Scan.records_per_second(100)

    {:ok, records} =
      @repo.all(scan,
        timeout: 5_000,
        socket_timeout: 1_000,
        max_concurrent_nodes: 1,
        records_per_second: 100,
        include_bin_data: true,
        expected_duration: :long
      )

    scanned_names = records |> Enum.map(& &1.bins["name"]) |> Enum.sort()
    expected_names = for i <- 1..@size, do: "policy-#{i}"

    unless scanned_names == expected_names do
      raise "Unexpected scan names: #{inspect(scanned_names)}"
    end

    Logger.info("    Scan runtime options accepted and returned #{@size} records.")
  end

  defp cleanup do
    for i <- 1..@size do
      @repo.delete(key(i))
    end
  end

  defp bins(i), do: %{"name" => "policy-#{i}", "rank" => i}

  defp key(i), do: Aerospike.key(@namespace, @set, "policy_#{i}")

  defp assert_bin(nil, bin, expected) do
    raise "Expected #{bin}=#{inspect(expected)}, got no record"
  end

  defp assert_bin(record, bin, expected) do
    unless record.bins[bin] == expected do
      raise "Expected #{bin}=#{inspect(expected)}, got #{inspect(record.bins[bin])}"
    end
  end
end
