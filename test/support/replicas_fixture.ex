defmodule Aerospike.Test.ReplicasFixture do
  @moduledoc false
  # Helpers that produce valid `replicas` info values for tests so the
  # Tender exercises the real parser end-to-end without a running server.

  import Bitwise

  @partitions 4096
  @bitmap_bytes 512

  @doc """
  Builds a `replicas` info value for a single namespace at `regime`, where
  the replica index → partition-ids mapping says which partitions this
  node owns at each replica slot. Partition ids outside the lists default
  to "not owned."
  """
  @spec build(String.t(), non_neg_integer(), [[non_neg_integer()]]) :: String.t()
  def build(namespace, regime, partitions_per_replica) when is_list(partitions_per_replica) do
    replica_count = length(partitions_per_replica)

    bitmaps =
      partitions_per_replica
      |> Enum.map(&encode_bitmap/1)
      |> Enum.map(&Base.encode64/1)

    Enum.join([
      namespace,
      ":",
      Integer.to_string(regime),
      ",",
      Integer.to_string(replica_count),
      "," | bitmaps_with_commas(bitmaps)
    ])
  end

  defp bitmaps_with_commas([last]), do: [last]
  defp bitmaps_with_commas([head | rest]), do: [head, "," | bitmaps_with_commas(rest)]

  @doc """
  Convenience wrapper: build a replicas value where this node owns **every**
  partition at master (replica index 0) in `namespace`.
  """
  @spec all_master(String.t(), non_neg_integer()) :: String.t()
  def all_master(namespace, regime) do
    build(namespace, regime, [Enum.to_list(0..(@partitions - 1))])
  end

  defp encode_bitmap(partition_ids) do
    empty = :binary.copy(<<0>>, @bitmap_bytes)

    Enum.reduce(partition_ids, empty, fn pid, acc when pid in 0..(@partitions - 1)//1 ->
      set_bit(acc, pid)
    end)
  end

  defp set_bit(bin, partition_id) do
    byte_index = partition_id >>> 3
    bit_mask = 0x80 >>> band(partition_id, 7)
    <<prefix::binary-size(byte_index), byte, rest::binary>> = bin
    <<prefix::binary, bor(byte, bit_mask), rest::binary>>
  end
end
