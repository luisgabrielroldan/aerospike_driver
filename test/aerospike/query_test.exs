defmodule Aerospike.QueryTest do
  use ExUnit.Case, async: true

  alias Aerospike.Exp
  alias Aerospike.Filter
  alias Aerospike.PartitionFilter
  alias Aerospike.Query

  describe "new/2" do
    test "builds query with namespace and set" do
      q = Query.new("ns", "users")
      assert q.namespace == "ns"
      assert q.set == "users"
      assert q.index_filter == nil
      assert q.bin_names == []
      assert q.filters == []
    end

    test "rejects empty namespace" do
      assert_raise ArgumentError, fn -> Query.new("", "s") end
    end

    test "rejects empty set" do
      assert_raise ArgumentError, fn -> Query.new("ns", "") end
    end
  end

  describe "where/2 replaces index filter" do
    test "only the last where is kept" do
      f1 = Filter.equal("x", 1)
      f2 = Filter.equal("y", 2)

      q =
        Query.new("ns", "s")
        |> Query.where(f1)
        |> Query.where(f2)

      assert q.index_filter == f2
    end
  end

  describe "select/2 and filter/2" do
    test "select sets bin projection" do
      q = Query.new("ns", "s") |> Query.select(["name"])
      assert q.bin_names == ["name"]
    end

    test "filter appends expression filters" do
      a = Exp.from_wire(<<1>>)
      b = Exp.from_wire(<<2>>)

      q =
        Query.new("ns", "s")
        |> Query.filter(a)
        |> Query.filter(b)

      assert q.filters == [a, b]
    end
  end

  describe "other builders" do
    test "max_records, records_per_second, partition_filter, no_bins" do
      pf = PartitionFilter.by_range(0, 10)

      q =
        Query.new("ns", "s")
        |> Query.max_records(50)
        |> Query.records_per_second(1_000)
        |> Query.partition_filter(pf)
        |> Query.no_bins(true)

      assert q.max_records == 50
      assert q.records_per_second == 1_000
      assert q.partition_filter == pf
      assert q.no_bins == true
    end
  end
end
