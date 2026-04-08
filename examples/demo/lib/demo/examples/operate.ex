defmodule Demo.Examples.Operate do
  @moduledoc """
  Demonstrates multi-operation (operate) commands: add + put + get in one round-trip.

  """

  require Logger

  import Aerospike.Op, only: [add: 2, put: 2, get: 1]

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo"

  def run do
    key = Aerospike.key(@namespace, @set, "opkey")

    # Write initial record
    bins = %{"optintbin" => 7, "optstringbin" => "string value"}

    Logger.info(
      "  Put: ns=#{@namespace} set=#{@set} key=opkey bin1=optintbin value1=7 bin2=optstringbin value2='string value'"
    )

    :ok = @repo.put!(key, bins)

    # Add integer, write new string, and read record — all in one round-trip
    Logger.info("  Operate: add(optintbin, 4) + put(optstringbin, 'new string') + get()")

    {:ok, record} =
      @repo.operate(key, [
        add("optintbin", 4),
        put("optstringbin", "new string"),
        get("optintbin"),
        get("optstringbin")
      ])

    unless record do
      raise "Failed to operate: ns=#{@namespace} set=#{@set} key=opkey"
    end

    # Validate integer add: 7 + 4 = 11
    int_val = record.bins["optintbin"]

    unless int_val == 11 do
      raise "Operate add mismatch: expected 11, got #{int_val}"
    end

    Logger.info("  Validated: optintbin=#{int_val} (7+4=11)")

    # Validate string put
    str_val = record.bins["optstringbin"]

    unless str_val == "new string" do
      raise "Operate put mismatch: expected 'new string', got '#{str_val}'"
    end

    Logger.info("  Validated: optstringbin='#{str_val}'")

    # Cleanup
    @repo.delete(key)
  end
end
