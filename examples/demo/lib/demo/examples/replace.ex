defmodule Demo.Examples.Replace do
  @moduledoc """
  Demonstrates replace mode (REPLACE removes old bins) and replace-only (fails if missing).

  """

  require Logger

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo"

  def run do
    run_replace_example()
    run_replace_only_example()
  end

  defp run_replace_example do
    key = Aerospike.key(@namespace, @set, "replacekey")

    # Write two bins
    bins1 = %{"bin1" => "value1", "bin2" => "value2"}

    Logger.info("  Put: ns=#{@namespace} set=#{@set} key=replacekey bin1=value1 bin2=value2")

    :ok = @repo.put!(key, bins1)

    # Replace with only bin3 — bin1 and bin2 should be deleted.
    # Matches Aerospike REPLACE / INFO3_CREATE_OR_REPLACE (not :replace_only).
    Logger.info("  Replace with: bin3=value3")
    :ok = @repo.put!(key, %{"bin3" => "value3"}, exists: :create_or_replace)

    {:ok, record} = @repo.get(key)

    unless record do
      raise "Failed to get: ns=#{@namespace} set=#{@set} key=replacekey"
    end

    if record.bins["bin1"] == nil do
      Logger.info("  bin1 was deleted as expected.")
    else
      raise "bin1 found when it should have been deleted."
    end

    if record.bins["bin2"] == nil do
      Logger.info("  bin2 was deleted as expected.")
    else
      raise "bin2 found when it should have been deleted."
    end

    unless record.bins["bin3"] == "value3" do
      raise "bin3 mismatch: expected 'value3', got '#{record.bins["bin3"]}'"
    end

    Logger.info("  bin3=value3 validated.")

    # Cleanup
    @repo.delete(key)
  end

  defp run_replace_only_example do
    key = Aerospike.key(@namespace, @set, "replaceonlykey")

    # Delete record if it already exists
    @repo.delete(key)

    Logger.info("  Replace record requiring that it exists (should fail)...")

    case @repo.put(key, %{"bin" => "value"}, exists: :replace_only) do
      {:error, %Aerospike.Error{code: :key_not_found}} ->
        Logger.info("  Success: 'Not found' error returned as expected.")

      :ok ->
        raise "Should have received key_not_found error, but write succeeded."

      {:error, other} ->
        raise "Unexpected error: #{inspect(other)}"
    end
  end
end
