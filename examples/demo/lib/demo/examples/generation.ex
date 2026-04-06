defmodule Demo.Examples.Generation do
  @moduledoc """
  Demonstrates generation-based optimistic concurrency (compare-and-swap).

  """

  require Logger

  @conn :aero
  @namespace "test"
  @set "demo"

  def run do
    key = Aerospike.key(@namespace, @set, "genkey")
    bin_name = "genbin"

    # Delete record if it already exists
    Aerospike.delete(@conn, key)

    # Set some values for the same record (creates generation history)
    Logger.info("  Put: key=genkey bin=genbin value=genvalue1")
    :ok = Aerospike.put!(@conn, key, %{bin_name => "genvalue1"})

    Logger.info("  Put: key=genkey bin=genbin value=genvalue2")
    :ok = Aerospike.put!(@conn, key, %{bin_name => "genvalue2"})

    # Retrieve record and its generation count
    {:ok, record} = Aerospike.get(@conn, key)

    unless record do
      raise "Failed to get: ns=#{@namespace} set=#{@set} key=genkey"
    end

    received = record.bins[bin_name]

    unless received == "genvalue2" do
      raise "Get mismatch: expected 'genvalue2', received '#{received}'"
    end

    Logger.info(
      "  Get successful: bin=#{bin_name} value=#{received} generation=#{record.generation}"
    )

    # Set record and fail if it's not the expected generation
    Logger.info("  Put with expected generation=#{record.generation}: bin=genbin value=genvalue3")

    :ok =
      Aerospike.put!(@conn, key, %{bin_name => "genvalue3"},
        generation: record.generation,
        gen_policy: :expect_gen_equal
      )

    # Set record with INVALID generation — should fail
    Logger.info("  Put with invalid generation=9999 (should fail)...")

    case Aerospike.put(@conn, key, %{bin_name => "genvalue4"},
           generation: 9999,
           gen_policy: :expect_gen_equal
         ) do
      {:error, %Aerospike.Error{code: :generation_error}} ->
        Logger.info("  Success: generation error returned as expected.")

      :ok ->
        raise "Should have received generation error instead of success"

      {:error, other} ->
        raise "Unexpected error: #{inspect(other)}"
    end

    # Verify the record still has genvalue3 (the invalid generation write was rejected)
    {:ok, verify} = Aerospike.get(@conn, key)

    unless verify do
      raise "Failed to get: ns=#{@namespace} set=#{@set} key=genkey"
    end

    received2 = verify.bins[bin_name]

    if received2 == "genvalue3" do
      Logger.info(
        "  Verified: bin=#{bin_name} value=#{received2} generation=#{verify.generation}"
      )
    else
      raise "Get mismatch: expected 'genvalue3', received '#{received2}'"
    end

    # Cleanup
    Aerospike.delete(@conn, key)
  end
end
