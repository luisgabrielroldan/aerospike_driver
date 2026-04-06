defmodule Demo.Examples.Batch do
  @moduledoc """
  Demonstrates batch operations: write records, batch exists, batch reads, batch headers.

  """

  require Logger

  @conn :aero
  @namespace "test"
  @set "demo"

  @key_prefix "batchkey"
  @value_prefix "batchvalue"
  @bin_name "batchbin"
  @size 8

  def run do
    write_records()
    batch_exists()
    batch_reads()
    batch_read_headers()
    cleanup()
  end

  defp write_records do
    Logger.info("  Writing #{@size} records individually...")

    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "#{@key_prefix}#{i}")
      bins = %{@bin_name => "#{@value_prefix}#{i}"}

      Logger.info("    Put: key=#{@key_prefix}#{i} bin=#{@bin_name} value=#{@value_prefix}#{i}")
      :ok = Aerospike.put!(@conn, key, bins)
    end
  end

  defp batch_exists do
    Logger.info("  Batch exists check...")

    keys = for i <- 1..@size, do: Aerospike.key(@namespace, @set, "#{@key_prefix}#{i}")

    {:ok, exists_array} = Aerospike.batch_exists(@conn, keys)

    for i <- 0..(@size - 1) do
      exists = Enum.at(exists_array, i)

      Logger.info(
        "    Record: key=#{@key_prefix}#{i + 1} exists=#{exists}"
      )
    end

    unless length(exists_array) == @size do
      raise "Exists array size mismatch: expected #{@size}, got #{length(exists_array)}"
    end

    unless Enum.all?(exists_array) do
      raise "Some records should exist but reported as missing"
    end

    Logger.info("  Batch exists: all #{@size} records found.")
  end

  defp batch_reads do
    Logger.info("  Batch reads...")

    keys = for i <- 1..@size, do: Aerospike.key(@namespace, @set, "#{@key_prefix}#{i}")

    {:ok, records} = Aerospike.batch_get(@conn, keys, bins: [@bin_name])

    for i <- 0..(@size - 1) do
      record = Enum.at(records, i)

      if record do
        value = record.bins[@bin_name]

        Logger.info(
          "    Record: key=#{@key_prefix}#{i + 1} bin=#{@bin_name} value=#{value}"
        )
      else
        Logger.error("    Record: key=#{@key_prefix}#{i + 1} NOT FOUND")
      end
    end

    unless length(records) == @size do
      raise "Record size mismatch: expected #{@size}, got #{length(records)}"
    end

    Logger.info("  Batch reads: #{@size} records retrieved.")
  end

  defp batch_read_headers do
    Logger.info("  Batch read headers...")

    keys = for i <- 1..@size, do: Aerospike.key(@namespace, @set, "#{@key_prefix}#{i}")

    {:ok, records} = Aerospike.batch_get(@conn, keys, header_only: true)

    for i <- 0..(@size - 1) do
      record = Enum.at(records, i)

      if record do
        Logger.info(
          "    Record: key=#{@key_prefix}#{i + 1} generation=#{record.generation} ttl=#{record.ttl}"
        )
      else
        Logger.error("    Record: key=#{@key_prefix}#{i + 1} NOT FOUND")
      end
    end

    unless length(records) == @size do
      raise "Record size mismatch: expected #{@size}, got #{length(records)}"
    end

    Logger.info("  Batch headers: #{@size} headers retrieved.")
  end

  defp cleanup do
    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "#{@key_prefix}#{i}")
      Aerospike.delete(@conn, key)
    end
  end
end
