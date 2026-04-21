defmodule Aerospike.FilterTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Filter

  describe "range/3" do
    test "builds integer range filter" do
      filter = Filter.range("score", 1, 100)

      assert filter.bin_name == "score"
      assert filter.index_type == :default
      assert filter.particle_type == :integer
      assert filter.begin == 1
      assert filter.end == 100
    end

    test "rejects begin > end" do
      assert_raise ArgumentError, fn -> Filter.range("score", 5, 2) end
    end
  end

  describe "equal/2" do
    test "integer equality" do
      filter = Filter.equal("id", 42)
      assert filter.particle_type == :integer
      assert filter.begin == 42
      assert filter.end == 42
    end

    test "string equality" do
      filter = Filter.equal("name", "ada")
      assert filter.particle_type == :string
      assert filter.begin == "ada"
      assert filter.end == "ada"
    end
  end

  describe "contains/3" do
    test "list index with integer" do
      filter = Filter.contains("tags", :list, 7)
      assert filter.index_type == :list
      assert filter.particle_type == :integer
      assert filter.begin == 7
    end

    test "mapkeys with string" do
      filter = Filter.contains("metadata", :mapkeys, "k")
      assert filter.index_type == :mapkeys
      assert filter.particle_type == :string
    end
  end

  describe "advanced helpers" do
    test "using_index/2 targets a named index" do
      filter = Filter.equal("age", 21) |> Filter.using_index("age_idx")
      assert filter.index_name == "age_idx"
    end

    test "with_ctx/2 attaches nested CDT context" do
      ctx = [Ctx.map_key("roles"), Ctx.list_index(0)]
      filter = Filter.contains("profile", :mapvalues, "admin") |> Filter.with_ctx(ctx)

      assert filter.ctx == ctx
    end
  end
end
