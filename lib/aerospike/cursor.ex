defmodule Aerospike.Cursor do
  @dialyzer :no_match

  @moduledoc """
  Opaque partition-resume cursor for paged queries.

  Serialize with `encode/1` for storage or URLs; restore with `decode/1`.
  The cursor captures partition progress, not a stable snapshot of the
  result set.
  """

  import Bitwise

  alias Aerospike.Error
  alias Aerospike.PartitionFilter

  @enforce_keys [:partitions]
  defstruct [:partitions]

  @version 1
  @digest_bytes 20
  @entry_bytes 2 + 1 + @digest_bytes + 8
  @header_bytes 3
  @max_partitions 4096

  @flag_digest 1
  @flag_bval 2

  @typedoc """
  Partition resume cursor used by paged scans and queries.

  Partition entries may carry the last digest and bval observed for each
  partition so a later page can resume partition-order traversal.
  """
  @type t :: %__MODULE__{partitions: [PartitionFilter.partition_entry()]}

  @min_int64 -9_223_372_036_854_775_808
  @max_int64 9_223_372_036_854_775_807

  @doc """
  Serializes a cursor to a URL-safe Base64 string.
  """
  @spec encode(t()) :: String.t()
  def encode(%__MODULE__{partitions: parts}) when is_list(parts) do
    count = length(parts)

    if count > @max_partitions do
      raise ArgumentError,
            "cursor cannot list more than #{@max_partitions} partitions, got: #{count}"
    end

    entries =
      parts
      |> Enum.map(&encode_partition_entry!/1)
      |> IO.iodata_to_binary()

    bin = <<@version::8, count::16-big, entries::binary>>
    Base.url_encode64(bin, padding: false)
  end

  @doc """
  Deserializes a cursor produced by `encode/1`.
  """
  @spec decode(String.t()) :: {:ok, t()} | {:error, Error.t()}
  def decode(encoded) when is_binary(encoded) do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, bin} -> decode_binary(bin)
      :error -> {:error, Error.from_result_code(:parse_error, message: "invalid cursor base64")}
    end
  end

  defp decode_binary(bin) when byte_size(bin) < @header_bytes do
    {:error, Error.from_result_code(:parse_error, message: "invalid cursor binary")}
  end

  defp decode_binary(bin) do
    <<version::8, count::16-big, rest::binary>> = bin

    cond do
      version != @version ->
        {:error, Error.from_result_code(:parse_error, message: "invalid cursor version")}

      count > @max_partitions ->
        {:error, Error.from_result_code(:parse_error, message: "invalid cursor count")}

      true ->
        expected = @header_bytes + count * @entry_bytes

        cond do
          byte_size(bin) < expected ->
            {:error, Error.from_result_code(:parse_error, message: "invalid cursor binary")}

          byte_size(bin) > expected ->
            {:error, Error.from_result_code(:parse_error, message: "invalid cursor binary")}

          true ->
            decode_entries(rest, count, [])
        end
    end
  rescue
    _ -> {:error, Error.from_result_code(:parse_error, message: "invalid cursor binary")}
  end

  defp decode_entries(_rest, 0, acc), do: {:ok, %__MODULE__{partitions: Enum.reverse(acc)}}

  defp decode_entries(rest, _n, _acc) when byte_size(rest) < @entry_bytes do
    {:error, Error.from_result_code(:parse_error, message: "invalid cursor binary")}
  end

  defp decode_entries(rest, n, acc) do
    <<id::16-big, flags::8, digest::binary-size(@digest_bytes), bval::64-big-signed,
      more::binary>> =
      rest

    with :ok <- validate_reserved_flags(flags),
         :ok <- validate_id(id),
         {:ok, entry} <- decode_entry_fields(id, flags, digest, bval) do
      decode_entries(more, n - 1, [entry | acc])
    else
      {:error, %Error{}} = err -> err
    end
  end

  defp validate_reserved_flags(flags) do
    if (flags &&& 0b1111_1100) == 0 do
      :ok
    else
      {:error, Error.from_result_code(:parse_error, message: "invalid cursor flags")}
    end
  end

  defp validate_id(id) do
    if id >= 0 and id < @max_partitions do
      :ok
    else
      {:error, Error.from_result_code(:parse_error, message: "invalid cursor partition id")}
    end
  end

  defp decode_entry_fields(id, flags, digest, bval) do
    has_digest? = (flags &&& @flag_digest) != 0
    has_bval? = (flags &&& @flag_bval) != 0
    zero_digest = <<0::size(@digest_bytes * 8)>>

    with :ok <- digest_consistent?(has_digest?, digest, zero_digest),
         :ok <- bval_consistent?(has_bval?, bval) do
      digest_out = if has_digest?, do: digest, else: nil
      bval_out = if has_bval?, do: bval, else: nil
      {:ok, %{id: id, digest: digest_out, bval: bval_out}}
    end
  end

  defp digest_consistent?(has_digest?, digest, zero_digest) do
    if has_digest? or digest == zero_digest,
      do: :ok,
      else: {:error, inconsistent_cursor()}
  end

  defp bval_consistent?(has_bval?, bval) do
    if has_bval? or bval == 0, do: :ok, else: {:error, inconsistent_cursor()}
  end

  defp inconsistent_cursor do
    Error.from_result_code(:parse_error, message: "invalid cursor binary")
  end

  defp encode_partition_entry!(map) when is_map(map) do
    assert_partition_entry_keys!(map)
    id = Map.fetch!(map, :id)
    digest = Map.get(map, :digest, nil)
    bval = Map.get(map, :bval, nil)
    assert_partition_id!(id)
    assert_digest_field!(digest)
    assert_bval_field!(bval)
    entry_binary(id, digest, bval)
  end

  defp encode_partition_entry!(_), do: raise(ArgumentError, "invalid cursor partition entry")

  defp assert_partition_entry_keys!(map) do
    allowed = [:id, :digest, :bval]

    unless Enum.all?(Map.keys(map), &(&1 in allowed)) do
      raise ArgumentError, "invalid cursor partition entry keys"
    end
  end

  defp assert_partition_id!(id) when is_integer(id) and id >= 0 and id < @max_partitions, do: :ok
  defp assert_partition_id!(_), do: raise(ArgumentError, "invalid cursor partition id")

  defp assert_digest_field!(nil), do: :ok
  defp assert_digest_field!(d) when is_binary(d) and byte_size(d) == @digest_bytes, do: :ok
  defp assert_digest_field!(_), do: raise(ArgumentError, "invalid cursor digest")

  defp assert_bval_field!(nil), do: :ok

  defp assert_bval_field!(bv) when is_integer(bv) and bv >= @min_int64 and bv <= @max_int64,
    do: :ok

  defp assert_bval_field!(_), do: raise(ArgumentError, "invalid cursor bval")

  defp entry_binary(id, digest, bval) do
    has_digest? = digest != nil
    has_bval? = bval != nil

    flags =
      0
      |> bor(if(has_digest?, do: @flag_digest, else: 0))
      |> bor(if(has_bval?, do: @flag_bval, else: 0))

    digest_bin =
      if has_digest? do
        digest
      else
        <<0::size(@digest_bytes * 8)>>
      end

    bval_i64 = if has_bval?, do: bval, else: 0

    <<id::16-big, flags::8, digest_bin::binary-size(@digest_bytes), bval_i64::64-big-signed>>
  end
end
