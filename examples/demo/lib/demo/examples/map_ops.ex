defmodule Demo.Examples.MapOps do
  @moduledoc """
  Demonstrates server-side map CDT operations via `Aerospike.Op.Map`:
  put, get_by_key, increment, remove_by_key, get_by_rank_range, and size.
  """

  require Logger

  alias Aerospike.Op

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_mapops"

  def run do
    put_and_get()
    increment_values()
    remove_and_size()
    rank_queries()
    cleanup()
  end

  defp put_and_get do
    key = key("pg")
    @repo.delete(key)

    @repo.operate!(key, [
      Op.Map.put("prefs", "theme", "dark"),
      Op.Map.put("prefs", "lang", "elixir"),
      Op.Map.put("prefs", "tz", "UTC")
    ])

    rec = @repo.operate!(key, [Op.Map.size("prefs")])

    size = rec.bins["prefs"]
    Logger.info("  Put 3 entries: size=#{size}")
    unless size == 3, do: raise("Expected size=3, got #{size}")

    rec2 =
      @repo.operate!(key, [
        Op.Map.get_by_key("prefs", "theme", return_type: Op.Map.return_value())
      ])

    val = rec2.bins["prefs"]
    Logger.info("  get_by_key('theme'): #{inspect(val)}")
    unless val == "dark", do: raise("Expected 'dark', got #{inspect(val)}")
  end

  defp increment_values do
    key = key("inc")
    @repo.delete(key)

    @repo.operate!(key, [
      Op.Map.put("stats", "views", 100),
      Op.Map.put("stats", "clicks", 20)
    ])

    rec = @repo.operate!(key, [Op.Map.increment("stats", "views", 15)])
    @repo.operate!(key, [Op.Map.increment("stats", "clicks", 3)])

    views = rec.bins["stats"]
    Logger.info("  Increment views (+15): #{views}")

    {:ok, r} = @repo.get(key)
    Logger.info("  Stats: views=#{r.bins["stats"]["views"]} clicks=#{r.bins["stats"]["clicks"]}")

    unless r.bins["stats"]["views"] == 115, do: raise("Expected views=115")
    unless r.bins["stats"]["clicks"] == 23, do: raise("Expected clicks=23")
  end

  defp remove_and_size do
    key = key("rm")
    @repo.delete(key)

    :ok = @repo.put!(key, %{"m" => %{"a" => 1, "b" => 2, "c" => 3}})

    rec =
      @repo.operate!(key, [
        Op.Map.remove_by_key("m", "b", return_type: Op.Map.return_value())
      ])

    Logger.info("  Removed key 'b': value=#{inspect(rec.bins["m"])}")

    {:ok, r} = @repo.get(key)

    unless map_size(r.bins["m"]) == 2, do: raise("Expected 2 entries after remove")
    unless r.bins["m"]["b"] == nil, do: raise("Key 'b' should be gone")

    Logger.info("  Remaining map: #{inspect(r.bins["m"])}")
  end

  defp rank_queries do
    key = key("rnk")
    @repo.delete(key)

    :ok =
      @repo.put!(key, %{
        "scores" => %{"alice" => 85, "bob" => 92, "carol" => 78, "dave" => 96, "eve" => 88}
      })

    rec =
      @repo.operate!(key, [
        Op.Map.get_by_rank_range("scores", -3, 3, return_type: Op.Map.return_key_value())
      ])

    top3 = rec.bins["scores"]
    Logger.info("  Top 3 scores by rank: #{inspect(top3)}")
  end

  defp cleanup do
    for id <- ["pg", "inc", "rm", "rnk"] do
      @repo.delete(key(id))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
