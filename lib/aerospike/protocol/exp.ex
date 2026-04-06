defmodule Aerospike.Protocol.Exp do
  @moduledoc false

  # Encodes Aerospike expression nodes to MessagePack wire bytes.
  #
  # Nodes are plain maps with fields:
  #   bytes: binary            — pre-encoded; written directly (used for sub-expressions)
  #   val:   term              — literal value (integer, float, bool, nil, {:string, s}, {:blob, b})
  #   cmd:   atom              — operation code (see @op_* constants)
  #   val:   term              — scalar argument for :bin and :digest_modulo commands
  #   type:  atom              — exp type for :bin reads (:int, :string, :float, etc.)
  #   exps:  [node]            — sub-expression nodes for compound operations
  #
  # String vs blob literals: both are Elixir binaries, so they are tagged:
  #   {:string, s} → msgpack str (raw UTF-8)
  #   {:blob, b}   → msgpack bin (0xC4/C5/C6 format)
  #
  # All string values in expression context (bin names, string literals) use plain
  # msgpack str — no particle-type prefix. This differs from CDT payloads.

  alias Aerospike.Protocol.MessagePack

  # Operation codes (from expression.go)
  @op_eq 1
  @op_ne 2
  @op_gt 3
  @op_gte 4
  @op_lt 5
  @op_lte 6
  @op_and 16
  @op_or 17
  @op_not 18
  @op_digest_modulo 64
  @op_device_size 65
  @op_last_update 66
  @op_void_time 68
  @op_ttl 69
  @op_set_name 70
  @op_key_exists 71
  @op_is_tombstone 72
  @op_record_size 74
  @op_bin 81

  # Expression type codes for bin reads (from expression.go)
  @exp_type_nil 0
  @exp_type_bool 1
  @exp_type_int 2
  @exp_type_string 3
  @exp_type_list 4
  @exp_type_map 5
  @exp_type_blob 6
  @exp_type_float 7
  @exp_type_geo 8
  @exp_type_hll 9

  @spec encode(map()) :: binary()

  # Pre-encoded sub-expression — write bytes directly.
  def encode(%{bytes: bytes}) when is_binary(bytes), do: bytes

  # Bin read: [81, exp_type_int, raw_str_name]
  # Bin name is packed as plain msgpack str (packRawString in Go).
  def encode(%{cmd: :bin, val: name, type: type}) when is_binary(name) do
    array_header(3) <>
      MessagePack.pack!(@op_bin) <>
      MessagePack.pack!(exp_type_int(type)) <>
      MessagePack.pack!(name)
  end

  # Digest modulo: [64, value]
  def encode(%{cmd: :digest_modulo, val: n}) when is_integer(n) do
    array_header(2) <> MessagePack.pack!(@op_digest_modulo) <> MessagePack.pack!(n)
  end

  # Compound expression: [op_code, sub_expr_1, ..., sub_expr_N]
  # Covers comparisons (:eq, :ne, :gt, :gte, :lt, :lte), boolean (:and_, :or_, :not_).
  def encode(%{cmd: cmd, exps: exps}) when is_list(exps) do
    encoded_exps = IO.iodata_to_binary(Enum.map(exps, &encode/1))
    array_header(1 + length(exps)) <> MessagePack.pack!(op_code(cmd)) <> encoded_exps
  end

  # Metadata (no value): [op_code]
  # Must come before the generic val-based literal clauses to avoid pattern overlap.
  def encode(%{cmd: cmd})
      when cmd in [
             :ttl,
             :void_time,
             :last_update,
             :key_exists,
             :set_name,
             :is_tombstone,
             :device_size,
             :record_size
           ] do
    array_header(1) <> MessagePack.pack!(op_code(cmd))
  end

  # Literal nil
  def encode(%{val: nil}), do: MessagePack.pack!(nil)

  # Literal bool
  def encode(%{val: false}), do: MessagePack.pack!(false)
  def encode(%{val: true}), do: MessagePack.pack!(true)

  # Literal integer
  def encode(%{val: n}) when is_integer(n), do: MessagePack.pack!(n)

  # Literal float
  def encode(%{val: f}) when is_float(f), do: MessagePack.pack!(f)

  # Literal string — plain msgpack str (raw UTF-8, no particle prefix)
  def encode(%{val: {:string, s}}) when is_binary(s), do: MessagePack.pack!(s)

  # Literal blob — msgpack bin format (0xC4/C5/C6)
  def encode(%{val: {:blob, b}}) when is_binary(b), do: msgpack_bin(b)

  # Op code lookup
  defp op_code(:eq), do: @op_eq
  defp op_code(:ne), do: @op_ne
  defp op_code(:gt), do: @op_gt
  defp op_code(:gte), do: @op_gte
  defp op_code(:lt), do: @op_lt
  defp op_code(:lte), do: @op_lte
  defp op_code(:and_), do: @op_and
  defp op_code(:or_), do: @op_or
  defp op_code(:not_), do: @op_not
  defp op_code(:ttl), do: @op_ttl
  defp op_code(:void_time), do: @op_void_time
  defp op_code(:last_update), do: @op_last_update
  defp op_code(:key_exists), do: @op_key_exists
  defp op_code(:set_name), do: @op_set_name
  defp op_code(:is_tombstone), do: @op_is_tombstone
  defp op_code(:device_size), do: @op_device_size
  defp op_code(:record_size), do: @op_record_size

  # Exp type integer lookup
  defp exp_type_int(nil), do: @exp_type_nil
  defp exp_type_int(:bool), do: @exp_type_bool
  defp exp_type_int(:int), do: @exp_type_int
  defp exp_type_int(:string), do: @exp_type_string
  defp exp_type_int(:list), do: @exp_type_list
  defp exp_type_int(:map), do: @exp_type_map
  defp exp_type_int(:blob), do: @exp_type_blob
  defp exp_type_int(:float), do: @exp_type_float
  defp exp_type_int(:geo), do: @exp_type_geo
  defp exp_type_int(:hll), do: @exp_type_hll

  # Msgpack array header
  defp array_header(len) when len <= 15, do: <<0x90 + len>>
  defp array_header(len) when len <= 0xFFFF, do: <<0xDC, len::16-big>>
  defp array_header(len), do: <<0xDD, len::32-big>>

  # Msgpack bin format for blob literals (0xC4/C5/C6, not msgpack str)
  defp msgpack_bin(b) when byte_size(b) <= 0xFF, do: <<0xC4, byte_size(b)::8, b::binary>>
  defp msgpack_bin(b) when byte_size(b) <= 0xFFFF, do: <<0xC5, byte_size(b)::16-big, b::binary>>
  defp msgpack_bin(b), do: <<0xC6, byte_size(b)::32-big, b::binary>>
end
