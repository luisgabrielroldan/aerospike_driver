defmodule Demo.Examples.Get do
  @moduledoc """
  Demonstrates reading records by string and integer keys.

  """

  require Logger

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo"

  def run do
    # Setup: write a record first
    skey = "getkey_string"
    key = Aerospike.key(@namespace, @set, skey)
    bins = %{"name" => "test_record", "value" => 999}
    :ok = @repo.put!(key, bins)

    # Get by string key
    Logger.info("  Get: ns=#{@namespace} set=#{@set} key=#{skey}")
    {:ok, record} = @repo.get(key)

    unless record do
      raise "Record not found: ns=#{@namespace} set=#{@set} key=#{skey}"
    end

    Logger.info("  Got: #{inspect(record.bins)}")

    unless record.bins["name"] == "test_record" do
      raise "Get mismatch: expected 'test_record', got '#{record.bins["name"]}'"
    end

    unless record.bins["value"] == 999 do
      raise "Get mismatch: expected 999, got #{record.bins["value"]}"
    end

    # Get by integer key
    ikey = 67_890
    int_key = Aerospike.key(@namespace, @set, ikey)
    :ok = @repo.put!(int_key, %{"data" => "int_key_data"})

    Logger.info("  Get: ns=#{@namespace} set=#{@set} key=#{ikey}")
    {:ok, int_record} = @repo.get(int_key)

    unless int_record do
      raise "Record not found: ns=#{@namespace} set=#{@set} key=#{ikey}"
    end

    Logger.info("  Got: #{inspect(int_record.bins)}")

    # Cleanup
    @repo.delete(key)
    @repo.delete(int_key)
  end
end
