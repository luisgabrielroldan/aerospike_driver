defmodule Aerospike.Op.BitTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Aerospike.Ctx
  alias Aerospike.Op.Bit
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  defp payload(op), do: MessagePack.unpack!(op.data)

  defp assert_op(op, op_type, expected_payload) do
    assert op.op_type == op_type
    assert op.bin_name == "bits"
    refute op.map_cdt
    assert payload(op) == expected_payload
  end

  test "write flag and overflow helpers expose server policy values" do
    assert Bit.write_default() == 0
    assert Bit.write_create_only() == 1
    assert Bit.write_update_only() == 2
    assert Bit.write_no_fail() == 4
    assert Bit.write_partial() == 8

    assert Bit.overflow_fail() == 0
    assert Bit.overflow_saturate() == 2
    assert Bit.overflow_wrap() == 4
  end

  test "modify builders encode reference op codes and payloads" do
    cases = [
      {Bit.resize("bits", 16), [0, 16, 0, 0]},
      {Bit.resize("bits", 16, 4, flags: Bit.write_update_only()), [0, 16, 2, 4]},
      {Bit.insert("bits", 1, <<0x0F>>, flags: Bit.write_update_only()), [1, 1, <<4, 0x0F>>, 2]},
      {Bit.remove("bits", 1, 2, flags: 3), [2, 1, 2, 3]},
      {Bit.set("bits", 0, 8, <<0xF0>>, flags: Bit.write_no_fail()), [3, 0, 8, <<4, 0xF0>>, 4]},
      {Bit.bw_or("bits", 0, 8, <<0x0F>>, flags: 5), [4, 0, 8, <<4, 0x0F>>, 5]},
      {Bit.bw_xor("bits", 0, 8, <<0xFF>>, flags: 6), [5, 0, 8, <<4, 0xFF>>, 6]},
      {Bit.bw_and("bits", 0, 8, <<0xF0>>, flags: 7), [6, 0, 8, <<4, 0xF0>>, 7]},
      {Bit.bw_not("bits", 0, 8, flags: Bit.write_partial()), [7, 0, 8, 8]},
      {Bit.lshift("bits", 0, 8, 2, flags: 9), [8, 0, 8, 2, 9]},
      {Bit.rshift("bits", 0, 8, 3, flags: 10), [9, 0, 8, 3, 10]},
      {Bit.add("bits", 0, 8, 1, flags: 11, overflow_action: Bit.overflow_saturate()),
       [10, 0, 8, 1, 11, 2]},
      {Bit.subtract("bits", 0, 8, 1, flags: 12, overflow_action: Bit.overflow_wrap()),
       [11, 0, 8, 1, 12, 4]},
      {Bit.set_int("bits", 0, 8, 42, flags: 13), [12, 0, 8, 42, 13]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_bit_modify(), expected_payload)
    end)
  end

  test "signed arithmetic combines the signed flag with overflow_action" do
    assert payload(Bit.add("bits", 0, 8, 1, signed: true)) == [10, 0, 8, 1, 0, 1]

    assert payload(
             Bit.subtract("bits", 0, 8, 1,
               signed: true,
               overflow_action: Bit.overflow_wrap()
             )
           ) == [
             11,
             0,
             8,
             1,
             0,
             bor(Bit.overflow_wrap(), 1)
           ]
  end

  test "default-arity wrappers encode expected default flags" do
    cases = [
      {Bit.resize("bits", 16, 4), [0, 16, 0, 4]},
      {Bit.insert("bits", 1, <<0x0F>>), [1, 1, <<4, 0x0F>>, 0]},
      {Bit.remove("bits", 1, 2), [2, 1, 2, 0]},
      {Bit.set("bits", 0, 8, <<0xF0>>), [3, 0, 8, <<4, 0xF0>>, 0]},
      {Bit.bw_or("bits", 0, 8, <<0x0F>>), [4, 0, 8, <<4, 0x0F>>, 0]},
      {Bit.bw_xor("bits", 0, 8, <<0xFF>>), [5, 0, 8, <<4, 0xFF>>, 0]},
      {Bit.bw_and("bits", 0, 8, <<0xF0>>), [6, 0, 8, <<4, 0xF0>>, 0]},
      {Bit.bw_not("bits", 0, 8), [7, 0, 8, 0]},
      {Bit.lshift("bits", 0, 8, 2), [8, 0, 8, 2, 0]},
      {Bit.rshift("bits", 0, 8, 3), [9, 0, 8, 3, 0]},
      {Bit.add("bits", 0, 8, 1), [10, 0, 8, 1, 0, 0]},
      {Bit.subtract("bits", 0, 8, 1), [11, 0, 8, 1, 0, 0]},
      {Bit.set_int("bits", 0, 8, 42), [12, 0, 8, 42, 0]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_bit_modify(), expected_payload)
    end)
  end

  test "read builders encode reference op codes and payloads" do
    cases = [
      {Bit.get("bits", 0, 8), [50, 0, 8]},
      {Bit.count("bits", 0, 8), [51, 0, 8]},
      {Bit.lscan("bits", 0, 8, true), [52, 0, 8, true]},
      {Bit.rscan("bits", 0, 8, false), [53, 0, 8, false]},
      {Bit.get_int("bits", 0, 8), [54, 0, 8]},
      {Bit.get_int("bits", 0, 8, true), [54, 0, 8, 1]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_bit_read(), expected_payload)
    end)
  end

  test "ctx option is encoded through the CDT helper path" do
    ctx = [Ctx.map_key("outer"), Ctx.list_index(0)]
    op = Bit.set("bits", 4, 2, <<0b1100_0000>>, ctx: ctx)

    assert payload(op) == [
             255,
             [0x22, "outer", 0x10, 0],
             [3, 4, 2, <<4, 0b1100_0000>>, 0]
           ]
  end

  test "ctx option is encoded for bit read operations" do
    ctx = [Ctx.map_key("outer"), Ctx.list_index(0)]
    op = Bit.get_int("bits", 0, 8, true, ctx: ctx)

    assert payload(op) == [
             255,
             [0x22, "outer", 0x10, 0],
             [54, 0, 8, 1]
           ]
  end
end
