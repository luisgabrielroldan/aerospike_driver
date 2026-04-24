defmodule Aerospike.PartitionFilterTest do
  use ExUnit.Case, async: true

  alias Aerospike.PartitionFilter

  test "all/0 returns the full retryable partition range" do
    assert %PartitionFilter{
             begin: 0,
             count: 4096,
             digest: nil,
             partitions: [],
             done?: false,
             retry?: true
           } = PartitionFilter.all()

    assert PartitionFilter.partition_count() == 4096
  end

  test "by_id/1 validates the partition id bounds" do
    assert %PartitionFilter{begin: 17, count: 1} = PartitionFilter.by_id(17)

    assert_raise ArgumentError, ~r/partition_id must be in 0..4095, got: -1/, fn ->
      PartitionFilter.by_id(-1)
    end

    assert_raise ArgumentError, ~r/partition_id must be in 0..4095, got: 4096/, fn ->
      PartitionFilter.by_id(4096)
    end
  end

  test "by_range/2 validates the count and upper bound" do
    assert %PartitionFilter{begin: 100, count: 25} = PartitionFilter.by_range(100, 25)

    assert_raise ArgumentError, ~r/count must be a positive integer, got: 0/, fn ->
      PartitionFilter.by_range(10, 0)
    end

    assert_raise ArgumentError,
                 ~r/begin \+ count exceeds partition count \(4096\), got 4090 \+ 7/,
                 fn ->
                   PartitionFilter.by_range(4090, 7)
                 end
  end

  test "by_digest/1 requires a 20-byte digest" do
    digest = :binary.copy(<<7>>, 20)

    assert %PartitionFilter{begin: 0, count: 4096, digest: ^digest} =
             PartitionFilter.by_digest(digest)

    assert_raise ArgumentError, ~r/digest must be 20 bytes, got: 19/, fn ->
      PartitionFilter.by_digest(:binary.copy(<<1>>, 19))
    end
  end
end
