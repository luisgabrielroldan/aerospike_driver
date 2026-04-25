defmodule Aerospike.ScanQueryTypesTest do
  use ExUnit.Case, async: true

  alias Aerospike.Cursor
  alias Aerospike.Exp
  alias Aerospike.Filter
  alias Aerospike.Page
  alias Aerospike.PartitionFilter
  alias Aerospike.Query
  alias Aerospike.Record
  alias Aerospike.Scan

  describe "scan/0" do
    test "builds a namespace-wide scan and accepts updates without owning execution state" do
      scan =
        Scan.new("test")
        |> Scan.select(["name", "age"])
        |> Scan.filter(Exp.from_wire(<<1>>))
        |> Scan.max_records(25)
        |> Scan.records_per_second(100)
        |> Scan.partition_filter(PartitionFilter.by_id(42))
        |> Scan.no_bins(true)

      assert scan.namespace == "test"
      assert scan.set == nil
      assert scan.bin_names == ["name", "age"]
      assert %Exp{wire: <<1>>} = scan.filters |> hd()
      assert scan.max_records == 25
      assert scan.records_per_second == 100
      assert scan.partition_filter.begin == 42
      assert scan.no_bins == true
    end

    test "requires a non-empty namespace" do
      assert_raise ArgumentError, fn -> Scan.new("") end
    end
  end

  describe "query/0" do
    test "builds a query description and stores opaque predicate state" do
      query =
        Query.new("test", "users")
        |> Query.where(Filter.range("age", 10, 20))
        |> Query.select(["name"])
        |> Query.filter(Exp.from_wire(<<2>>))
        |> Query.max_records(10)
        |> Query.records_per_second(50)
        |> Query.partition_filter(PartitionFilter.by_range(10, 5))
        |> Query.no_bins(true)

      assert query.namespace == "test"
      assert query.set == "users"
      assert %Filter{} = query.index_filter
      assert query.index_filter.begin == 10
      assert query.index_filter.end == 20
      assert query.bin_names == ["name"]
      assert %Exp{wire: <<2>>} = query.filters |> hd()
      assert query.max_records == 10
      assert query.records_per_second == 50
      assert query.partition_filter.begin == 10
      assert query.partition_filter.count == 5
      assert query.no_bins == true
    end

    test "requires a non-empty set" do
      assert_raise ArgumentError, fn -> Query.new("test", "") end
    end
  end

  describe "cursor/page support" do
    test "cursor encode/decode preserves partition entries" do
      cursor = %Cursor{
        partitions: [%{id: 1, digest: <<1::160>>, bval: 12}, %{id: 2, digest: nil, bval: nil}]
      }

      assert encoded = Cursor.encode(cursor)
      assert {:ok, decoded} = Cursor.decode(encoded)
      assert decoded == cursor
    end

    test "page keeps records, cursor, and completion state together" do
      page = %Page{
        records: [%Record{key: :k, bins: %{}, generation: 0, ttl: 0}],
        cursor: nil,
        done?: true
      }

      assert page.records |> hd() |> Map.get(:key) == :k
      assert page.cursor == nil
      assert page.done? == true
    end
  end
end
