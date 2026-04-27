defmodule Aerospike.Protocol.Exp do
  @moduledoc false

  alias Aerospike.Protocol.MessagePack

  @op_unknown 0
  @op_eq 1
  @op_ne 2
  @op_gt 3
  @op_gte 4
  @op_lt 5
  @op_lte 6
  @op_regex 7
  @op_geo 8
  @op_and 16
  @op_or 17
  @op_not 18
  @op_exclusive 19
  @op_add 20
  @op_sub 21
  @op_mul 22
  @op_div 23
  @op_pow 24
  @op_log 25
  @op_mod 26
  @op_abs 27
  @op_floor 28
  @op_ceil 29
  @op_to_int 30
  @op_to_float 31
  @op_int_and 32
  @op_int_or 33
  @op_int_xor 34
  @op_int_not 35
  @op_int_lshift 36
  @op_int_rshift 37
  @op_int_arshift 38
  @op_int_count 39
  @op_int_lscan 40
  @op_int_rscan 41
  @op_min 50
  @op_max 51
  @op_digest_modulo 64
  @op_device_size 65
  @op_last_update 66
  @op_since_update 67
  @op_void_time 68
  @op_ttl 69
  @op_set_name 70
  @op_key_exists 71
  @op_is_tombstone 72
  @op_record_size 74
  @op_key 80
  @op_bin 81
  @op_bin_type 82
  @op_result_remove 100
  @op_var_builtin 122
  @op_cond 123
  @op_var 124
  @op_let 125
  @op_quoted 126
  @op_call 127

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

  @particle_geojson 23

  @spec encode(map()) :: binary()
  def encode(%{bytes: bytes}) when is_binary(bytes), do: bytes

  def encode(%{cmd: :call, type: type, module: module, payload: payload, bin: bin})
      when is_integer(module) and is_binary(payload) do
    array_header(5) <>
      MessagePack.pack!(@op_call) <>
      MessagePack.pack!(exp_type_int(type)) <>
      MessagePack.pack!(module) <>
      payload <>
      encode(bin)
  end

  def encode(%{cmd: :key, type: type}) do
    array_header(2) <>
      MessagePack.pack!(@op_key) <>
      MessagePack.pack!(exp_type_int(type))
  end

  def encode(%{cmd: :bin, val: name, type: type}) when is_binary(name) do
    array_header(3) <>
      MessagePack.pack!(@op_bin) <>
      MessagePack.pack!(exp_type_int(type)) <>
      MessagePack.pack!(name)
  end

  def encode(%{cmd: :loop_var, val: part, type: type}) when is_integer(part) do
    array_header(3) <>
      MessagePack.pack!(@op_var_builtin) <>
      MessagePack.pack!(exp_type_int(type)) <>
      MessagePack.pack!(part)
  end

  def encode(%{cmd: cmd, val: name}) when cmd in [:bin_type, :var] and is_binary(name) do
    array_header(2) <>
      MessagePack.pack!(op_code(cmd)) <>
      MessagePack.pack!(name)
  end

  def encode(%{cmd: :digest_modulo, val: n}) when is_integer(n) do
    array_header(2) <>
      MessagePack.pack!(@op_digest_modulo) <>
      MessagePack.pack!(n)
  end

  def encode(%{cmd: :regex, val: {regex, flags}, exps: [bin]})
      when is_binary(regex) and is_integer(flags) do
    array_header(4) <>
      MessagePack.pack!(@op_regex) <>
      MessagePack.pack!(flags) <>
      MessagePack.pack!(regex) <>
      encode(bin)
  end

  def encode(%{cmd: :def, val: name, exps: [expression]}) when is_binary(name) do
    MessagePack.pack!(name) <> encode(expression)
  end

  def encode(%{cmd: :let, exps: exps}) when is_list(exps) do
    encoded_exps =
      exps
      |> Enum.map(&encode/1)
      |> IO.iodata_to_binary()

    array_header((length(exps) - 1) * 2 + 2) <>
      MessagePack.pack!(@op_let) <>
      encoded_exps
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
             :remove_result,
             :record_size,
             :set_name,
             :since_update,
             :ttl,
             :unknown,
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

  def encode(%{val: {:geo, value}}) when is_binary(value),
    do: msgpack_particle(@particle_geojson, value)

  def encode(%{val: {:list, values}}) when is_list(values),
    do: encode(%{cmd: :quoted, val: values})

  def encode(%{val: {:map, values}}) when is_map(values), do: MessagePack.pack!(values)
  def encode(%{val: :infinity}), do: <<0xD4, 0xFF, 0x01>>
  def encode(%{val: :wildcard}), do: <<0xD4, 0xFF, 0x00>>

  def encode(%{cmd: :quoted, val: value}) do
    array_header(2) <>
      MessagePack.pack!(@op_quoted) <>
      MessagePack.pack!(value)
  end

  @spec module_payload(integer(), [map() | term()]) :: binary()
  def module_payload(op_code, args) when is_integer(op_code) and is_list(args) do
    encoded_args =
      args
      |> Enum.map(&module_arg/1)
      |> IO.iodata_to_binary()

    array_header(1 + length(args)) <>
      MessagePack.pack!(op_code) <>
      encoded_args
  end

  defp op_code(:eq), do: @op_eq
  defp op_code(:ne), do: @op_ne
  defp op_code(:gt), do: @op_gt
  defp op_code(:gte), do: @op_gte
  defp op_code(:lt), do: @op_lt
  defp op_code(:lte), do: @op_lte
  defp op_code(:geo_compare), do: @op_geo
  defp op_code(:and_), do: @op_and
  defp op_code(:or_), do: @op_or
  defp op_code(:not_), do: @op_not
  defp op_code(:exclusive), do: @op_exclusive
  defp op_code(:add), do: @op_add
  defp op_code(:sub), do: @op_sub
  defp op_code(:mul), do: @op_mul
  defp op_code(:div_), do: @op_div
  defp op_code(:pow), do: @op_pow
  defp op_code(:log), do: @op_log
  defp op_code(:mod), do: @op_mod
  defp op_code(:abs), do: @op_abs
  defp op_code(:floor), do: @op_floor
  defp op_code(:ceil), do: @op_ceil
  defp op_code(:to_int), do: @op_to_int
  defp op_code(:to_float), do: @op_to_float
  defp op_code(:int_and), do: @op_int_and
  defp op_code(:int_or), do: @op_int_or
  defp op_code(:int_xor), do: @op_int_xor
  defp op_code(:int_not), do: @op_int_not
  defp op_code(:int_lshift), do: @op_int_lshift
  defp op_code(:int_rshift), do: @op_int_rshift
  defp op_code(:int_arshift), do: @op_int_arshift
  defp op_code(:int_count), do: @op_int_count
  defp op_code(:int_lscan), do: @op_int_lscan
  defp op_code(:int_rscan), do: @op_int_rscan
  defp op_code(:min), do: @op_min
  defp op_code(:max), do: @op_max
  defp op_code(:device_size), do: @op_device_size
  defp op_code(:last_update), do: @op_last_update
  defp op_code(:since_update), do: @op_since_update
  defp op_code(:void_time), do: @op_void_time
  defp op_code(:ttl), do: @op_ttl
  defp op_code(:set_name), do: @op_set_name
  defp op_code(:key_exists), do: @op_key_exists
  defp op_code(:is_tombstone), do: @op_is_tombstone
  defp op_code(:record_size), do: @op_record_size
  defp op_code(:bin_type), do: @op_bin_type
  defp op_code(:remove_result), do: @op_result_remove
  defp op_code(:cond), do: @op_cond
  defp op_code(:var), do: @op_var
  defp op_code(:unknown), do: @op_unknown

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

  defp msgpack_particle(type, value) when byte_size(value) + 1 <= 0xFF do
    <<0xD9, byte_size(value) + 1, type, value::binary>>
  end

  defp msgpack_particle(type, value) when byte_size(value) + 1 <= 0xFFFF do
    <<0xDA, byte_size(value) + 1::16-big, type, value::binary>>
  end

  defp msgpack_particle(type, value) do
    <<0xDB, byte_size(value) + 1::32-big, type, value::binary>>
  end

  defp module_arg(%{bytes: bytes}) when is_binary(bytes), do: bytes
  defp module_arg(arg), do: MessagePack.pack!(arg)
end
