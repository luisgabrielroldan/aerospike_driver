defmodule Aerospike.Protocol.PartitionMapTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.PartitionMap

  @full_bitmap :binary.copy(<<0xFF>>, 512)

  test "parse_replicas_value: full bitmap assigns all partitions to node" do
    b64 = Base.encode64(@full_bitmap)
    body = "test:0,1,#{b64}"

    tuples = PartitionMap.parse_replicas_value(body, "BB9012345678")

    expected = MapSet.new(for p <- 0..4095, do: {"test", p, 0, "BB9012345678"})
    assert MapSet.new(tuples) == expected
  end

  test "parse_replicas_value: replicas_all line (regime/count/bitmap without three-part header)" do
    b64 = Base.encode64(@full_bitmap)
    body = "other_ns:1,#{b64}"

    tuples = PartitionMap.parse_replicas_value(body, "nodeA")

    expected = MapSet.new(for p <- 0..4095, do: {"other_ns", p, 0, "nodeA"})
    assert MapSet.new(tuples) == expected
  end

  test "parse_replicas_value: two namespaces in one body" do
    b64 = Base.encode64(@full_bitmap)
    body = "a:0,1,#{b64};b:0,1,#{b64}"

    tuples = PartitionMap.parse_replicas_value(body, "N")

    assert MapSet.new(for p <- 0..4095, do: {"a", p, 0, "N"}) ==
             MapSet.new(Enum.filter(tuples, fn {ns, _, _, _} -> ns == "a" end))

    assert MapSet.new(for p <- 0..4095, do: {"b", p, 0, "N"}) ==
             MapSet.new(Enum.filter(tuples, fn {ns, _, _, _} -> ns == "b" end))
  end

  test "parse_replicas_value: two replica bitmaps (second all zero)" do
    b64_a = Base.encode64(@full_bitmap)
    b64_b = Base.encode64(:binary.copy(<<0>>, 512))
    body = "dual:0,2,#{b64_a},#{b64_b}"

    tuples = PartitionMap.parse_replicas_value(body, "R")

    rep0 = MapSet.new(for p <- 0..4095, do: {"dual", p, 0, "R"})
    assert MapSet.new(Enum.filter(tuples, fn {_, _, r, _} -> r == 0 end)) == rep0
    assert Enum.all?(tuples, fn {_, _, r, _} -> r in [0, 1] end)
    refute Enum.any?(tuples, fn {_, _, r, _} -> r == 1 end)
  end

  test "parse_replicas_value: short decoded bitmap is padded to 512 bytes" do
    short = :binary.copy(<<1>>, 10)
    b64 = Base.encode64(short)
    body = "pad:0,1,#{b64}"

    tuples = PartitionMap.parse_replicas_value(body, "P")
    assert Enum.all?(tuples, fn {ns, _, _, node} -> ns == "pad" and node == "P" end)
    assert tuples != []
  end

  test "parse_replicas_value: invalid base64 yields empty partition set" do
    body = "x:0,1,not-base64!!!"
    assert PartitionMap.parse_replicas_value(body, "N") == []
  end

  test "parse_replicas_value: decoded bitmap larger than 512 bytes becomes zero bitmap" do
    big = :binary.copy(<<0xAB>>, 513)
    b64 = Base.encode64(big)
    body = "big:0,1,#{b64}"

    assert PartitionMap.parse_replicas_value(body, "Z") == []
  end

  test "parse_replicas_value: falls back when replica line does not match regime/count/bitmap" do
    b64 = Base.encode64(@full_bitmap)
    body = "ns:x,1,#{b64}"

    assert PartitionMap.parse_replicas_value(body, "N") == []
  end

  test "parse_replicas_value: ignores invalid segments" do
    b64 = Base.encode64(@full_bitmap)
    body = ";;#{String.duplicate("a", 32)}:0,1,#{b64};bad;good:0,1,#{b64}"

    tuples = PartitionMap.parse_replicas_value(body, "Z")
    assert {"good", 0, 0, "Z"} in tuples
    refute Enum.any?(tuples, fn {ns, _, _, _} -> String.length(ns) >= 32 end)
  end

  test "parse_replicas_value: single-element rest (no commas) yields empty list" do
    body = "ns:only_one_part"
    assert PartitionMap.parse_replicas_value(body, "N") == []
  end

  test "parse_partition_generation" do
    assert PartitionMap.parse_partition_generation("42\n") == {:ok, 42}
    assert PartitionMap.parse_partition_generation("0") == {:ok, 0}
    assert PartitionMap.parse_partition_generation("bad") == :error
    assert PartitionMap.parse_partition_generation("-1") == :error
  end
end
