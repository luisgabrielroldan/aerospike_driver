defmodule Demo.Examples.Prepend do
  @moduledoc """
  Demonstrates atomic string prepend operations.

  """

  require Logger

  @conn :aero
  @namespace "test"
  @set "demo"

  def run do
    key = Aerospike.key(@namespace, @set, "prependkey")
    bin_name = "prependbin"

    # Delete record if it already exists
    Aerospike.delete(@conn, key)

    # Initial prepend creates the record
    Logger.info("  Initial prepend will create record. Initial value is 'World'.")
    :ok = Aerospike.prepend!(@conn, key, %{bin_name => "World"})

    # Prepend "Hello " to existing record
    Logger.info("  Prepend 'Hello ' to existing record.")
    :ok = Aerospike.prepend!(@conn, key, %{bin_name => "Hello "})

    {:ok, record} = Aerospike.get(@conn, key)

    unless record do
      raise "Failed to get: ns=#{@namespace} set=#{@set} key=prependkey"
    end

    received = record.bins[bin_name]
    expected = "Hello World"

    if received == expected do
      Logger.info("  Prepend successful: bin=#{bin_name} value=#{received}")
    else
      raise "Prepend mismatch: expected '#{expected}', received '#{received}'"
    end

    # Cleanup
    Aerospike.delete(@conn, key)
  end
end
