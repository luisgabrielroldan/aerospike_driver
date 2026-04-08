defmodule Demo.Examples.Add do
  @moduledoc """
  Demonstrates atomic integer add operations and add+get via operate.

  """

  require Logger

  import Aerospike.Op, only: [add: 2, get: 1]

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo"

  def run do
    key = Aerospike.key(@namespace, @set, "addkey")
    bin_name = "addbin"

    # Delete record if it already exists
    @repo.delete(key)

    # Initial add creates the record with value 10
    Logger.info("  Initial add will create record. Initial value is 10.")
    :ok = @repo.add!(key, %{bin_name => 10})

    # Add 5 to existing record
    Logger.info("  Add 5 to existing record.")
    :ok = @repo.add!(key, %{bin_name => 5})

    {:ok, record} = @repo.get(key)

    unless record do
      raise "Failed to get: ns=#{@namespace} set=#{@set} key=addkey"
    end

    received = record.bins[bin_name]
    expected = 15

    if received == expected do
      Logger.info("  Add successful: bin=#{bin_name} value=#{received}")
    else
      raise "Add mismatch: expected #{expected}, received #{received}"
    end

    # Demonstrate add and get combined via operate
    Logger.info("  Add 30 to existing record via operate.")

    {:ok, record2} =
      @repo.operate(key, [
        add(bin_name, 30),
        get(bin_name)
      ])

    received2 = record2.bins[bin_name]
    expected2 = 45

    if received2 == expected2 do
      Logger.info("  Operate add+get successful: bin=#{bin_name} value=#{received2}")
    else
      raise "Add mismatch: expected #{expected2}, received #{received2}"
    end

    # Cleanup
    @repo.delete(key)
  end
end
