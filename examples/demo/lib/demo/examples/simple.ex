defmodule Demo.Examples.Simple do
  @moduledoc """
  Full lifecycle example: put, get, add, prepend, append, delete bin, exists, delete.

  """

  require Logger

  @conn :aero
  @namespace "test"
  @set "demo"

  def run do
    key = Aerospike.key(@namespace, @set, "simple_example_key")

    bins = %{
      "bin1" => 42,
      "bin2" => "An elephant is a mouse with an operating system",
      "bin3" => ["Go", 17_981]
    }

    # Write bins
    Logger.info("  Put: key=simple_example_key bins=#{inspect(bins)}")
    :ok = Aerospike.put!(@conn, key, bins)

    # Read it back
    {:ok, rec} = Aerospike.get(@conn, key)
    Logger.info("  Get: #{inspect(rec.bins)}")

    # Add to bin1
    :ok = Aerospike.add!(@conn, key, %{"bin1" => 1})
    {:ok, rec2} = Aerospike.get(@conn, key)
    Logger.info("  After add(1): bin1=#{rec2.bins["bin1"]}")

    unless rec2.bins["bin1"] == 43 do
      raise "Add mismatch: expected 43, got #{rec2.bins["bin1"]}"
    end

    # Prepend and append to bin2
    :ok = Aerospike.prepend!(@conn, key, %{"bin2" => "Frankly:  "})
    :ok = Aerospike.append!(@conn, key, %{"bin2" => "."})

    {:ok, rec3} = Aerospike.get(@conn, key)
    Logger.info("  After prepend+append: bin2=#{rec3.bins["bin2"]}")

    expected_bin2 = "Frankly:  An elephant is a mouse with an operating system."

    unless rec3.bins["bin2"] == expected_bin2 do
      raise "Prepend/Append mismatch: expected #{expected_bin2}, got #{rec3.bins["bin2"]}"
    end

    # Delete bin3 by setting to nil
    :ok = Aerospike.put!(@conn, key, %{"bin3" => nil})
    {:ok, rec4} = Aerospike.get(@conn, key)
    Logger.info("  After delete bin3: bins=#{inspect(rec4.bins)}")

    if Map.has_key?(rec4.bins, "bin3") && rec4.bins["bin3"] != nil do
      raise "bin3 should have been deleted"
    end

    # Check if key exists
    {:ok, true} = Aerospike.exists(@conn, key)
    Logger.info("  Key exists: true")

    # Delete the key
    {:ok, true} = Aerospike.delete(@conn, key)
    Logger.info("  Deleted: true")

    {:ok, false} = Aerospike.exists(@conn, key)
    Logger.info("  Key exists after delete: false")
  end
end
