defmodule Aerospike.Protocol.Exp do
  @moduledoc false

  alias Aerospike.Protocol.MessagePack

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
  def encode(%{bytes: bytes}) when is_binary(bytes), do: bytes

  def encode(%{cmd: :bin, val: name, type: type}) when is_binary(name) do
    array_header(3) <>
      MessagePack.pack!(@op_bin) <>
      MessagePack.pack!(exp_type_int(type)) <>
      MessagePack.pack!(name)
  end

  def encode(%{cmd: :digest_modulo, val: n}) when is_integer(n) do
    array_header(2) <>
      MessagePack.pack!(@op_digest_modulo) <>
      MessagePack.pack!(n)
  end

  def encode(%{cmd: cmd, exps: exps}) when is_list(exps) do
    encoded_exps =
      exps
      |> Enum.map(&encode/1)
      |> IO.iodata_to_binary()

    array_header(1 + length(exps)) <>
      MessagePack.pack!(op_code(cmd)) <>
      encoded_exps
  end

  def encode(%{cmd: cmd})
      when cmd in [
             :device_size,
             :is_tombstone,
             :key_exists,
             :last_update,
             :record_size,
             :set_name,
             :ttl,
             :void_time
           ] do
    array_header(1) <> MessagePack.pack!(op_code(cmd))
  end

  def encode(%{val: nil}), do: MessagePack.pack!(nil)
  def encode(%{val: false}), do: MessagePack.pack!(false)
  def encode(%{val: true}), do: MessagePack.pack!(true)
  def encode(%{val: n}) when is_integer(n), do: MessagePack.pack!(n)
  def encode(%{val: f}) when is_float(f), do: MessagePack.pack!(f)
  def encode(%{val: {:string, value}}) when is_binary(value), do: MessagePack.pack!(value)
  def encode(%{val: {:blob, value}}) when is_binary(value), do: msgpack_bin(value)

  defp op_code(:eq), do: @op_eq
  defp op_code(:ne), do: @op_ne
  defp op_code(:gt), do: @op_gt
  defp op_code(:gte), do: @op_gte
  defp op_code(:lt), do: @op_lt
  defp op_code(:lte), do: @op_lte
  defp op_code(:and_), do: @op_and
  defp op_code(:or_), do: @op_or
  defp op_code(:not_), do: @op_not
  defp op_code(:device_size), do: @op_device_size
  defp op_code(:last_update), do: @op_last_update
  defp op_code(:void_time), do: @op_void_time
  defp op_code(:ttl), do: @op_ttl
  defp op_code(:set_name), do: @op_set_name
  defp op_code(:key_exists), do: @op_key_exists
  defp op_code(:is_tombstone), do: @op_is_tombstone
  defp op_code(:record_size), do: @op_record_size

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

  defp array_header(length) when length <= 15, do: <<0x90 + length>>
  defp array_header(length) when length <= 0xFFFF, do: <<0xDC, length::16-big>>
  defp array_header(length), do: <<0xDD, length::32-big>>

  defp msgpack_bin(value) when byte_size(value) <= 0xFF do
    <<0xC4, byte_size(value)::8, value::binary>>
  end

  defp msgpack_bin(value) when byte_size(value) <= 0xFFFF do
    <<0xC5, byte_size(value)::16-big, value::binary>>
  end

  defp msgpack_bin(value) do
    <<0xC6, byte_size(value)::32-big, value::binary>>
  end
end
