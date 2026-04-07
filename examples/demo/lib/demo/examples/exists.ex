defmodule Demo.Examples.Exists do
  @moduledoc """
  Demonstrates `Aerospike.exists/3` and `Aerospike.batch_exists/3`:
  single-key and multi-key existence checks.
  """

  require Logger

  @conn :aero
  @namespace "test"
  @set "demo_exists"

  def run do
    exists_basic()
    batch_exists()
    cleanup()
  end

  defp exists_basic do
    key = key("present")
    :ok = Aerospike.put!(@conn, key, %{"v" => 1})

    {:ok, true} = Aerospike.exists(@conn, key)
    Logger.info("  Exists for present key: true")

    absent_key = key("absent")
    {:ok, false} = Aerospike.exists(@conn, absent_key)
    Logger.info("  Exists for absent key: false")
  end

  defp batch_exists do
    keys =
      for i <- 1..5 do
        k = key("batch_#{i}")
        :ok = Aerospike.put!(@conn, k, %{"idx" => i})
        k
      end

    missing = key("batch_missing")
    all_keys = keys ++ [missing]

    {:ok, results} = Aerospike.batch_exists(@conn, all_keys)

    present = Enum.count(results, & &1)
    absent = Enum.count(results, &(!&1))

    unless present == 5 and absent == 1 do
      raise "Expected 5 present + 1 absent, got #{present} present + #{absent} absent"
    end

    Logger.info(
      "  batch_exists: #{present} present, #{absent} absent (out of #{length(all_keys)})"
    )
  end

  defp cleanup do
    ids = ["present", "absent"] ++ Enum.map(1..5, &"batch_#{&1}") ++ ["batch_missing"]

    for id <- ids do
      Aerospike.delete(@conn, key(id))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
