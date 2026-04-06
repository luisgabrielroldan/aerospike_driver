defmodule Demo.Examples.Append do
  @moduledoc """
  Demonstrates atomic string append operations.

  """

  require Logger

  @conn :aero
  @namespace "test"
  @set "demo"

  def run do
    key = Aerospike.key(@namespace, @set, "appendkey")
    bin_name = "appendbin"

    # Delete record if it already exists
    Aerospike.delete(@conn, key)

    # Initial append creates the record
    Logger.info("  Initial append will create record. Initial value is 'Hello'.")
    :ok = Aerospike.append!(@conn, key, %{bin_name => "Hello"})

    # Append " World" to existing record
    Logger.info("  Append ' World' to existing record.")
    :ok = Aerospike.append!(@conn, key, %{bin_name => " World"})

    {:ok, record} = Aerospike.get(@conn, key)

    unless record do
      raise "Failed to get: ns=#{@namespace} set=#{@set} key=appendkey"
    end

    received = record.bins[bin_name]
    expected = "Hello World"

    if received == expected do
      Logger.info("  Append successful: bin=#{bin_name} value=#{received}")
    else
      raise "Append mismatch: expected '#{expected}', received '#{received}'"
    end

    # Cleanup
    Aerospike.delete(@conn, key)
  end
end
