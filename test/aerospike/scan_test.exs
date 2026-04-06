defmodule Aerospike.ScanTest do
  use ExUnit.Case, async: true

  alias Aerospike.Exp
  alias Aerospike.PartitionFilter
  alias Aerospike.Scan

  describe "new/1 and new/2" do
    test "namespace-only scan" do
      s = Scan.new("ns")
      assert s.namespace == "ns"
      assert s.set == nil
      assert s.bin_names == []
      assert s.filters == []
      assert s.max_records == nil
      assert s.records_per_second == 0
      assert s.partition_filter == nil
      assert s.no_bins == false
    end

    test "namespace and set" do
      s = Scan.new("ns", "users")
      assert s.namespace == "ns"
      assert s.set == "users"
    end

    test "rejects empty namespace" do
      assert_raise ArgumentError, ~r/non-empty/, fn -> Scan.new("") end
    end
  end

  describe "builder chain" do
    test "select, filter, max_records, throttle, partition_filter, no_bins" do
      e1 = Exp.from_wire(<<1>>)
      e2 = Exp.from_wire(<<2>>)
      pf = PartitionFilter.by_id(10)

      s =
        Scan.new("ns", "s")
        |> Scan.select(["a", "b"])
        |> Scan.filter(e1)
        |> Scan.filter(e2)
        |> Scan.max_records(100)
        |> Scan.records_per_second(500)
        |> Scan.partition_filter(pf)
        |> Scan.no_bins(true)

      assert s.bin_names == ["a", "b"]
      assert s.filters == [e1, e2]
      assert s.max_records == 100
      assert s.records_per_second == 500
      assert s.partition_filter == pf
      assert s.no_bins == true
    end

    test "records_per_second allows zero" do
      s = Scan.new("ns") |> Scan.records_per_second(0)
      assert s.records_per_second == 0
    end
  end

  describe "validation" do
    test "select rejects empty bin name" do
      assert_raise ArgumentError, fn ->
        Scan.new("ns") |> Scan.select([""])
      end
    end

    test "max_records requires positive integer" do
      assert_raise FunctionClauseError, fn ->
        Scan.new("ns") |> Scan.max_records(0)
      end
    end

    test "records_per_second rejects negative" do
      assert_raise FunctionClauseError, fn ->
        Scan.new("ns") |> Scan.records_per_second(-1)
      end
    end

    test "no_bins requires boolean" do
      assert_raise FunctionClauseError, fn ->
        Scan.new("ns") |> Scan.no_bins("yes")
      end
    end
  end
end
