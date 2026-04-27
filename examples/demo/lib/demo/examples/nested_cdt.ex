defmodule Demo.Examples.NestedCdt do
  @moduledoc """
  Demonstrates nested CDT operations using `Aerospike.Ctx` context paths.

  Shows how to read and write inside nested maps and lists using context
  navigation: `Ctx.map_key/1`, `Ctx.list_index/1`, and combinations.
  """

  require Logger

  alias Aerospike.Ctx
  alias Aerospike.Op.List, as: ListOp
  alias Aerospike.Op.Map, as: MapOp

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_ncdt"

  def run do
    nested_map_update()
    nested_list_append()
    map_list_combo()
    cleanup()
  end

  defp nested_map_update do
    Logger.info("  Nested map: update a value inside a sub-map...")

    key = key("nmap")

    :ok =
      @repo.put!(key, %{
        "profile" => %{"name" => "alice", "settings" => %{"theme" => "light", "font" => 14}}
      })

    {:ok, _} =
      @repo.operate(key, [
        MapOp.put("profile", "theme", "dark", ctx: [Ctx.map_key("settings")])
      ])

    {:ok, rec} = @repo.get(key)
    settings = rec.bins["profile"]["settings"]
    Logger.info("    settings after ctx update: #{inspect(settings)}")

    unless settings["theme"] == "dark" do
      raise "Expected theme=dark, got #{inspect(settings["theme"])}"
    end

    unless settings["font"] == 14 do
      raise "Expected font=14, got #{inspect(settings["font"])}"
    end
  end

  defp nested_list_append do
    Logger.info("  Nested list: append to a list inside a list...")

    key = key("nlist")

    :ok =
      @repo.put!(key, %{
        "data" => ["header", ["a", "b"]]
      })

    {:ok, _} =
      @repo.operate(key, [
        ListOp.append("data", "c", ctx: [Ctx.list_index(1)])
      ])

    {:ok, rec} = @repo.operate(key, [ListOp.size("data", ctx: [Ctx.list_index(1)])])

    size = rec.bins["data"]
    Logger.info("    Inner list size after append: #{size}")

    unless size == 3 do
      raise "Expected inner list size 3, got #{size}"
    end

    {:ok, full} = @repo.get(key)
    inner = Enum.at(full.bins["data"], 1)
    Logger.info("    Inner list contents: #{inspect(inner)}")

    unless inner == ["a", "b", "c"] do
      raise "Expected [a, b, c], got #{inspect(inner)}"
    end
  end

  defp map_list_combo do
    Logger.info("  Map+list combo: append to a list that is a map value...")

    key = key("combo")

    :ok =
      @repo.put!(key, %{
        "config" => %{"tags" => ["elixir", "aerospike"], "version" => 1}
      })

    {:ok, _} =
      @repo.operate(key, [
        ListOp.append("config", "otp", ctx: [Ctx.map_key("tags")])
      ])

    {:ok, rec} = @repo.operate(key, [ListOp.size("config", ctx: [Ctx.map_key("tags")])])

    size = rec.bins["config"]
    Logger.info("    Tags list size after append: #{size}")

    unless size == 3 do
      raise "Expected tags size 3, got #{size}"
    end

    {:ok, full} = @repo.get(key)
    tags = full.bins["config"]["tags"]
    Logger.info("    Tags: #{inspect(tags)}")
  end

  defp cleanup do
    for id <- ["nmap", "nlist", "combo"] do
      @repo.delete(key(id))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
