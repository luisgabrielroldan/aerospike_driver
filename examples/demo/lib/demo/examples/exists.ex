defmodule Demo.Examples.Exists do
  @moduledoc """
  Demonstrates `Aerospike.exists/3` and `Aerospike.batch_exists/3`:
  single-key and multi-key existence checks.
  """

  require Logger

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_exists"

  def run do
    exists_basic()
    batch_exists()
    cleanup()
  end

  defp exists_basic do
    key = key("present")
    :ok = @repo.put!(key, %{"v" => 1})

    {:ok, true} = @repo.exists(key)
    Logger.info("  Exists for present key: true")

    absent_key = key("absent")
    {:ok, false} = @repo.exists(absent_key)
    Logger.info("  Exists for absent key: false")
  end

  defp batch_exists do
    keys =
      for i <- 1..5 do
        k = key("batch_#{i}")
        :ok = @repo.put!(k, %{"idx" => i})
        k
      end

    missing = key("batch_missing")
    all_keys = keys ++ [missing]

    {:ok, results} = @repo.batch_exists(all_keys)

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
      @repo.delete(key(id))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
