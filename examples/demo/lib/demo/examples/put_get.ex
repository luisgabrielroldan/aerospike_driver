defmodule Demo.Examples.PutGet do
  @moduledoc """
  Demonstrates multi-bin put/get, single-bin put/get, and header-only reads.

  """

  require Logger

  @conn :aero
  @namespace "test"
  @set "demo"

  def run do
    run_multi_bin_example()
    run_get_header_example()
  end

  defp run_multi_bin_example do
    key = Aerospike.key(@namespace, @set, "putgetkey")

    bins = %{"bin1" => "value1", "bin2" => "value2"}

    Logger.info(
      "  MultiBin Put: ns=#{@namespace} set=#{@set} key=putgetkey bins=#{inspect(bins)}"
    )

    :ok = Aerospike.put!(@conn, key, bins)

    Logger.info("  MultiBin Get: ns=#{@namespace} set=#{@set} key=putgetkey")
    {:ok, record} = Aerospike.get(@conn, key)

    unless record do
      raise "Failed to get: ns=#{@namespace} set=#{@set} key=putgetkey"
    end

    validate_bin("bin1", "value1", record)
    validate_bin("bin2", "value2", record)

    Logger.info("  MultiBin: validated bin1=value1 bin2=value2")

    # Cleanup
    Aerospike.delete(@conn, key)
  end

  defp run_get_header_example do
    key = Aerospike.key(@namespace, @set, "putgetkey_header")
    :ok = Aerospike.put!(@conn, key, %{"data" => "header_test"})

    Logger.info("  GetHeader: ns=#{@namespace} set=#{@set} key=putgetkey_header")
    {:ok, record} = Aerospike.get(@conn, key, header_only: true)

    unless record do
      raise "Failed to get header: ns=#{@namespace} set=#{@set} key=putgetkey_header"
    end

    unless record.generation > 0 do
      raise "Invalid record header: generation=#{record.generation} ttl=#{record.ttl}"
    end

    Logger.info("  Header: generation=#{record.generation} ttl=#{record.ttl}")

    # Cleanup
    Aerospike.delete(@conn, key)
  end

  defp validate_bin(name, expected, record) do
    received = record.bins[name]

    unless received == expected do
      raise "Bin mismatch: bin=#{name} expected=#{inspect(expected)} received=#{inspect(received)}"
    end
  end
end
