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

    test "rejects out-of-range integer bounds and empty bin names" do
      assert_raise ArgumentError, ~r/range begin must fit in signed int64/, fn ->
        Filter.range("score", 9_223_372_036_854_775_808, 10)
      end

      assert_raise ArgumentError, ~r/bin_name must be a non-empty string/, fn ->
        Filter.range("", 1, 2)
      end
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

    test "rejects unsupported values and empty bin names" do
      assert_raise ArgumentError, ~r/equal\/2 value must be integer or string/, fn ->
        Filter.equal("name", :ada)
      end

      assert_raise ArgumentError, ~r/bin_name must be a non-empty string/, fn ->
        Filter.equal("", "ada")
      end
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

    test "rejects invalid index types and unsupported values" do
      assert_raise ArgumentError,
                   ~r/contains\/3 index_type must be :list, :mapkeys, or :mapvalues/,
                   fn ->
                     Filter.contains("tags", :default, "k")
                   end

      assert_raise ArgumentError, ~r/contains\/3 value must be integer or string/, fn ->
        Filter.contains("tags", :list, :vip)
      end
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

    test "using_index/2 and with_ctx/2 validate their inputs" do
      filter = Filter.equal("age", 21)

      assert_raise ArgumentError, ~r/index_name must be a non-empty string/, fn ->
        Filter.using_index(filter, "")
      end

      assert_raise ArgumentError, ~r/ctx must be a non-empty list/, fn ->
        Filter.with_ctx(filter, [])
      end
    end
  end
end
