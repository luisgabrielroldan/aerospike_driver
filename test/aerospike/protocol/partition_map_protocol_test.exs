defmodule Aerospike.Protocol.PartitionMapProtocolTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Aerospike.Protocol.PartitionMap

  test "parse_replicas_value/2 parses replicas and replicas-all segments and skips malformed ones" do
    body =
      Enum.join(
        [
          "test:7,2,#{bitmap64([0, 7, 1024])},#{bitmap64([1])},#{bitmap64([99])}",
          "legacy:1,#{bitmap64([3])}",
          "broken:bad,data",
          "#{String.duplicate("x", 32)}:1,#{bitmap64([2])}",
          ":1,#{bitmap64([4])}"
        ],
        ";"
      )

    assert Enum.sort(PartitionMap.parse_replicas_value(body, "A1")) == [
             {"legacy", 3, 0, "A1"},
             {"test", 0, 0, "A1"},
             {"test", 1, 1, "A1"},
             {"test", 7, 0, "A1"},
             {"test", 1024, 0, "A1"}
           ]
  end

  test "parse_replicas_value/2 pads short decoded bitmaps and drops invalid base64" do
    body = "test:1,1,#{Base.encode64(<<0x80>>)};other:1,1,not-base64!"

    assert PartitionMap.parse_replicas_value(body, "B1") == [{"test", 0, 0, "B1"}]
  end

  test "parse_replicas_with_regime/1 keeps regime-aware segments only" do
    body =
      Enum.join(
        [
          "test:9,2,#{bitmap64([0, 4095])},#{bitmap64([4])}",
          "legacy:1,#{bitmap64([8])}",
          "broken"
        ],
        ";"
      )

    assert PartitionMap.parse_replicas_with_regime(body) == [
             {"test", 9, [{0, 0}, {4095, 0}, {4, 1}]}
           ]
  end

  test "parse_partition_generation/1 accepts trimmed non-negative integers only" do
    assert PartitionMap.parse_partition_generation(" 42 \n") == {:ok, 42}
    assert PartitionMap.parse_partition_generation("-1") == :error
    assert PartitionMap.parse_partition_generation("4x") == :error
  end

  defp bitmap64(partitions) do
    bytes =
      Enum.reduce(partitions, :binary.copy(<<0>>, 512), fn partition, acc ->
        byte_index = div(partition, 8)
        bit_index = rem(partition, 8)
        byte = :binary.at(acc, byte_index) ||| 0x80 >>> bit_index

        binary_part(acc, 0, byte_index) <>
          <<byte>> <> binary_part(acc, byte_index + 1, 511 - byte_index)
      end)

    Base.encode64(bytes)
  end
end
