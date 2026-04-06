defmodule Aerospike.PartitionFilterTest do
  use ExUnit.Case, async: true

  alias Aerospike.PartitionFilter

  test "all/0" do
    pf = PartitionFilter.all()
    assert pf.begin == 0
    assert pf.count == 4096
    assert pf.digest == nil
    assert pf.partitions == []
  end

  test "by_id/1" do
    pf = PartitionFilter.by_id(0)
    assert pf.begin == 0
    assert pf.count == 1

    pf2 = PartitionFilter.by_id(4095)
    assert pf2.begin == 4095
    assert pf2.count == 1
  end

  test "by_id/1 rejects out of range" do
    assert_raise ArgumentError, fn -> PartitionFilter.by_id(-1) end
    assert_raise ArgumentError, fn -> PartitionFilter.by_id(4096) end
  end

  test "by_range/2" do
    pf = PartitionFilter.by_range(100, 50)
    assert pf.begin == 100
    assert pf.count == 50
  end

  test "by_range/2 rejects invalid span" do
    assert_raise ArgumentError, fn -> PartitionFilter.by_range(0, 0) end
    assert_raise ArgumentError, fn -> PartitionFilter.by_range(4090, 10) end
  end

  test "by_digest/1" do
    d = :crypto.strong_rand_bytes(20)
    pf = PartitionFilter.by_digest(d)
    assert pf.digest == d
    assert pf.begin == 0
    assert pf.count == 4096
  end

  test "by_digest/1 rejects wrong size" do
    assert_raise ArgumentError, fn -> PartitionFilter.by_digest(<<1, 2, 3>>) end
  end
end
