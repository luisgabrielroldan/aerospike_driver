defmodule Aerospike.FilterTest do
  use ExUnit.Case, async: true

  alias Aerospike.Filter

  describe "range/3" do
    test "builds integer range filter" do
      f = Filter.range("score", 1, 100)
      assert f.bin_name == "score"
      assert f.index_type == :default
      assert f.particle_type == :integer
      assert f.begin == 1
      assert f.end == 100
    end

    test "rejects begin > end" do
      assert_raise ArgumentError, fn -> Filter.range("b", 5, 2) end
    end

    test "rejects out-of-range int64" do
      big = 9_223_372_036_854_775_808
      assert_raise ArgumentError, fn -> Filter.range("b", 0, big) end
    end
  end

  describe "equal/2" do
    test "integer equality" do
      f = Filter.equal("id", 42)
      assert f.particle_type == :integer
      assert f.begin == 42
      assert f.end == 42
    end

    test "string equality" do
      f = Filter.equal("name", "ada")
      assert f.particle_type == :string
      assert f.begin == "ada"
      assert f.end == "ada"
    end

    test "rejects unsupported value type" do
      assert_raise ArgumentError, fn -> Filter.equal("b", :atom) end
    end
  end

  describe "contains/3" do
    test "list index with integer" do
      f = Filter.contains("tags", :list, 7)
      assert f.index_type == :list
      assert f.particle_type == :integer
      assert f.begin == 7
    end

    test "mapkeys with string" do
      f = Filter.contains("m", :mapkeys, "k")
      assert f.index_type == :mapkeys
      assert f.particle_type == :string
    end

    test "rejects invalid index_type" do
      assert_raise ArgumentError, fn -> Filter.contains("b", :default, 1) end
    end
  end

  describe "geo" do
    test "geo_within/2" do
      json = ~s({"type":"Polygon","coordinates":[[]]})
      f = Filter.geo_within("loc", json)
      assert f.index_type == :geo_within
      assert f.particle_type == :string
      assert f.begin == json
    end

    test "geo_contains/2" do
      json = ~s({"type":"Point","coordinates":[0,0]})
      f = Filter.geo_contains("loc", json)
      assert f.index_type == :geo_contains
      assert f.begin == json
    end

    test "rejects empty geo string" do
      assert_raise ArgumentError, fn -> Filter.geo_within("loc", "") end
      assert_raise ArgumentError, fn -> Filter.geo_contains("loc", "") end
    end
  end

  describe "bin name" do
    test "rejects empty bin name for range" do
      assert_raise ArgumentError, fn -> Filter.range("", 0, 1) end
    end
  end
end
