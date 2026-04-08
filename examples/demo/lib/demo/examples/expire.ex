defmodule Demo.Examples.Expire do
  @moduledoc """
  Demonstrates record expiration (TTL) and no-expire behavior.


  NOTE: The noExpireExample from Go requires a namespace TTL configured to a small
  value (e.g., 5 seconds). This Elixir port only runs the expire portion, as the
  no-expire test would require special server configuration and a 10-second sleep.
  """

  require Logger

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo"

  def run do
    expire_example()
  end

  defp expire_example do
    key = Aerospike.key(@namespace, @set, "expirekey")
    bin_name = "expirebin"
    bin_value = "expirevalue"

    # Write record with 2-second expiration
    Logger.info("  Put: key=expirekey bin=#{bin_name} value=#{bin_value} ttl=2")
    :ok = @repo.put!(key, %{bin_name => bin_value}, ttl: 2)

    # Read the record before it expires
    Logger.info("  Get: key=expirekey (before expiry)")
    {:ok, record} = @repo.get(key)

    unless record do
      raise "Failed to get record before expiry"
    end

    received = record.bins[bin_name]

    if received == bin_value do
      Logger.info("  Get successful: bin=#{bin_name} value=#{received}")
    else
      raise "Expire record mismatch: expected '#{bin_value}', received '#{received}'"
    end

    # Wait for the record to expire
    Logger.info("  Sleeping for 3 seconds...")
    Process.sleep(3_000)

    # Read the record after it expires — should be gone
    case @repo.get(key) do
      {:error, %Aerospike.Error{code: :key_not_found}} ->
        Logger.info("  Expiry successful: record not found.")

      {:ok, nil} ->
        Logger.info("  Expiry successful: record not found.")

      {:ok, _record} ->
        raise "Found record when it should have expired."
    end
  end
end
