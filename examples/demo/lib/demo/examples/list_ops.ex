defmodule Demo.Examples.ListOps do
  @moduledoc """
  Demonstrates server-side list CDT operations via `Aerospike.Op.List`:
  append, insert, pop, sort, trim, get_by_rank_range, increment, and size.
  """

  require Logger

  alias Aerospike.Op

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_listops"

  def run do
    append_and_insert()
    pop_and_remove()
    sort_and_trim()
    rank_queries()
    increment_element()
    cleanup()
  end

  defp append_and_insert do
    key = key("a_i")
    @repo.delete(key)
    :ok = @repo.put!(key, %{"scores" => [10]})

    rec =
      @repo.operate!(key, [
        Op.List.append("scores", 30),
        Op.List.append("scores", 20),
        Op.List.size("scores")
      ])

    size = rec.bins["scores"]
    Logger.info("  Append 2 items to existing list: size=#{size}")
    unless size == 3, do: raise("Expected size=3, got #{size}")

    rec2 =
      @repo.operate!(key, [
        Op.List.insert("scores", 1, 15),
        Op.List.size("scores")
      ])

    Logger.info("  After insert at index 1: size=#{rec2.bins["scores"]}")
  end

  defp pop_and_remove do
    key = key("p_r")
    @repo.delete(key)
    :ok = @repo.put!(key, %{"q" => ["a", "b", "c", "d", "e"]})

    rec =
      @repo.operate!(key, [
        Op.List.pop("q", 0)
      ])

    popped = rec.bins["q"]
    Logger.info("  Pop index 0: #{inspect(popped)}")
    unless popped == "a", do: raise("Expected 'a', got #{inspect(popped)}")

    rec2 =
      @repo.operate!(key, [
        Op.List.remove("q", -1),
        Op.List.size("q")
      ])

    remaining = rec2.bins["q"]
    Logger.info("  After remove last: #{remaining} items remaining")
    unless remaining == 3, do: raise("Expected 3 items after pop+remove, got #{remaining}")
  end

  defp sort_and_trim do
    key = key("s_t")
    @repo.delete(key)
    :ok = @repo.put!(key, %{"nums" => [50, 10, 40, 20, 30]})

    @repo.operate!(key, [
      Op.List.sort("nums", 0),
      Op.List.trim("nums", -3, 3)
    ])

    {:ok, r} = @repo.get(key)
    Logger.info("  Sort then trim (keep last 3): #{inspect(r.bins["nums"])}")

    unless length(r.bins["nums"]) == 3 do
      raise "Expected 3 items after trim, got #{length(r.bins["nums"])}"
    end
  end

  defp rank_queries do
    key = key("rank")
    @repo.delete(key)
    :ok = @repo.put!(key, %{"vals" => [80, 20, 60, 40, 100]})

    rec =
      @repo.operate!(key, [
        Op.List.get_by_rank_range("vals", -3, 3, return_type: Op.List.return_value())
      ])

    top3 = rec.bins["vals"]
    Logger.info("  Top 3 by rank: #{inspect(top3)}")
    unless length(top3) == 3, do: raise("Expected 3 items, got #{length(top3)}")
  end

  defp increment_element do
    key = key("incr")
    @repo.delete(key)
    :ok = @repo.put!(key, %{"counters" => [10, 20, 30]})

    rec =
      @repo.operate!(key, [
        Op.List.increment("counters", 1, 5)
      ])

    val = rec.bins["counters"]
    Logger.info("  Increment counters[1] by 5: result=#{val}")
    unless val == 25, do: raise("Expected 25, got #{val}")
  end

  defp cleanup do
    for id <- ["a_i", "p_r", "s_t", "rank", "incr"] do
      @repo.delete(key(id))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
