defmodule Aerospike.Protocol.PartitionMap do
  @moduledoc false
  # Parses the `replicas` info response that maps partitions to nodes.
  #
  # Aerospike splits each namespace into 4096 partitions. Each node reports
  # which partitions it owns via a base64-encoded bitmap (512 bytes = 4096 bits).
  # The response format has two variants:
  #   - `replicas`:     `<ns>:<regime>,<replica_count>,<bitmap0>,<bitmap1>,...`
  #   - `replicas-all`: `<ns>:<replica_count>,<bitmap0>,<bitmap1>,...`
  # Both are semicolon-separated across namespaces.

  import Bitwise

  # Fixed partition count in Aerospike.
  @partitions 4096
  # 4096 bits = 512 bytes per bitmap.
  @bitmap_bytes 512

  @doc """
  Parses the `replicas` info response body (value only, after the `replicas\\t` key line).

  `node_name` is the Aerospike node id that produced this response; partition bits refer
  to ownership on that node for each replica index.

  Returns a list of `{namespace, partition_id, replica_index, node_name}` suitable for
  `:ets.insert/2` into the partitions table.
  """
  @spec parse_replicas_value(String.t(), String.t()) ::
          [{String.t(), non_neg_integer(), non_neg_integer(), String.t()}]
  def parse_replicas_value(body, node_name) when is_binary(body) and is_binary(node_name) do
    body
    |> String.trim()
    |> String.split(";", trim: true)
    |> Enum.flat_map(&parse_namespace_segment(&1, node_name))
  end

  # Splits "namespace:rest" — namespace names are max 31 chars in Aerospike.
  defp parse_namespace_segment(segment, node_name) do
    case String.split(segment, ":", parts: 2) do
      [namespace, rest] when namespace != "" and byte_size(namespace) < 32 ->
        parse_after_namespace(namespace, rest, node_name)

      _ ->
        []
    end
  end

  # Dispatches to the correct format variant (replicas vs replicas-all).
  defp parse_after_namespace(namespace, rest, node_name) do
    parts = String.split(rest, ",")

    case classify_line(parts) do
      {:replicas, _regime, replica_count, bitmap_parts} ->
        emit_tuples(namespace, replica_count, bitmap_parts, node_name)

      {:replicas_all, replica_count, bitmap_parts} ->
        emit_tuples(namespace, replica_count, bitmap_parts, node_name)

      :error ->
        []
    end
  end

  # Distinguishes `replicas` (regime, count, bitmaps...) from `replicas-all` (count, bitmaps...).
  # Tries the 3-field form first; falls back to the 2-field form.
  defp classify_line(parts) do
    case parts do
      [a, b, c | rest] ->
        with {:ok, regime} <- parse_int(a),
             {:ok, count} <- parse_int(b),
             true <- base64_looking?(c) do
          {:replicas, regime, count, [c | rest]}
        else
          _ -> classify_replicas_all(parts)
        end

      _ ->
        classify_replicas_all(parts)
    end
  end

  defp classify_replicas_all([a, b | rest]) do
    with {:ok, count} <- parse_int(a),
         true <- base64_looking?(b) do
      {:replicas_all, count, [b | rest]}
    else
      _ -> :error
    end
  end

  defp classify_replicas_all(_), do: :error

  defp parse_int(s) do
    case Integer.parse(s) do
      {n, ""} -> {:ok, n}
      _ -> :error
    end
  end

  # Quick heuristic: at least 4 chars, all base64 alphabet + padding.
  defp base64_looking?(s) when is_binary(s) do
    String.length(s) >= 4 and String.match?(s, ~r/^[A-Za-z0-9+\/]+=*$/)
  end

  # Decodes each replica bitmap and emits a tuple for every partition bit that is set.
  # One bitmap per replica index (0 = master, 1 = prole, ...).
  defp emit_tuples(namespace, replica_count, bitmap_parts, node_name) do
    bitmaps =
      bitmap_parts
      |> Enum.take(replica_count)
      |> Enum.map(&decode_bitmap/1)

    for {bitmap, replica_index} <- Enum.with_index(bitmaps),
        partition_id <- 0..(@partitions - 1),
        bit_set?(bitmap, partition_id),
        do: {namespace, partition_id, replica_index, node_name}
  end

  # Tests whether partition `partition` is owned by this node in the bitmap.
  # Bit layout: byte index = partition / 8, bit = MSB-first within the byte.
  defp bit_set?(bitmap, partition) when byte_size(bitmap) == @bitmap_bytes do
    byte = :binary.at(bitmap, partition >>> 3)
    bit_mask = 0x80 >>> band(partition, 7)
    band(byte, bit_mask) != 0
  end

  defp bit_set?(_, _), do: false

  # Decodes base64 to raw bytes; pads with zeros if shorter than expected.
  defp decode_bitmap(b64) do
    case Base.decode64(b64) do
      {:ok, bin} when byte_size(bin) == @bitmap_bytes -> bin
      {:ok, bin} when byte_size(bin) < @bitmap_bytes -> pad_bitmap(bin)
      _ -> :binary.copy(<<0>>, @bitmap_bytes)
    end
  end

  defp pad_bitmap(bin) do
    pad = @bitmap_bytes - byte_size(bin)
    bin <> :binary.copy(<<0>>, pad)
  end

  @doc """
  Parses the integer value of the `partition-generation` info field.
  """
  @spec parse_partition_generation(String.t()) :: {:ok, non_neg_integer()} | :error
  def parse_partition_generation(s) when is_binary(s) do
    case Integer.parse(String.trim(s)) do
      {n, ""} when n >= 0 -> {:ok, n}
      _ -> :error
    end
  end
end
