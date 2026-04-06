defmodule Demo.Examples.Touch do
  @moduledoc """
  Demonstrates touch operations to refresh TTL without modifying data.

  """

  require Logger

  @conn :aero
  @namespace "test"
  @set "demo"

  def run do
    key = Aerospike.key(@namespace, @set, "touchkey")
    bin_name = "touchbin"

    # Create record with 2-second expiration
    Logger.info("  Create record with 2 second expiration.")
    :ok = Aerospike.put!(@conn, key, %{bin_name => "touchvalue"}, ttl: 2)

    # Touch same record with 5-second expiration
    Logger.info("  Touch same record with 5 second expiration.")
    :ok = Aerospike.touch!(@conn, key, ttl: 5)

    # Verify the record has a non-zero TTL
    {:ok, record} = Aerospike.get(@conn, key, header_only: true)

    unless record do
      raise "Failed to get record after touch"
    end

    Logger.info("  After touch: generation=#{record.generation} ttl=#{record.ttl}")

    # Sleep 3 seconds — the original 2-second TTL would have expired, but touch extended it
    Logger.info("  Sleep 3 seconds.")
    Process.sleep(3_000)

    {:ok, record2} = Aerospike.get(@conn, key)

    unless record2 do
      raise "Record should still exist after 3 seconds (was touched to 5s TTL)"
    end

    Logger.info("  Success: record still exists after 3 seconds.")

    # Sleep 4 more seconds — now the 5-second TTL should have expired
    Logger.info("  Sleep 4 seconds.")
    Process.sleep(4_000)

    case Aerospike.get(@conn, key) do
      {:error, %Aerospike.Error{code: :key_not_found}} ->
        Logger.info("  Success: record expired as expected.")

      {:ok, nil} ->
        Logger.info("  Success: record expired as expected.")

      {:ok, _record} ->
        raise "Found record when it should have expired."
    end
  end
end
