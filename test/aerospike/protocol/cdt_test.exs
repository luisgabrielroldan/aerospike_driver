defmodule Aerospike.Protocol.CDTTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.CDT
  alias Aerospike.Protocol.MessagePack

  test "list_read_op encodes a list CDT payload without context" do
    op = CDT.list_read_op("tags", 16, [], nil)

    assert %Operation{
             op_type: op_type,
             particle_type: 4,
             bin_name: "tags",
             map_cdt: false
           } = op

    assert op_type == Operation.op_cdt_read()
    assert MessagePack.unpack!(op.data) == [16]
  end

  test "list_modify_op encodes context steps without map classification" do
    ctx = [Ctx.map_key("lists"), Ctx.list_index(-1)]
    op = CDT.list_modify_op("nested", 1, ["vip"], ctx)

    assert %Operation{op_type: op_type, map_cdt: false} = op
    assert op_type == Operation.op_cdt_modify()

    assert MessagePack.unpack!(op.data) == [
             255,
             [0x22, "lists", 0x10, -1],
             [1, "vip"]
           ]
  end

  test "map_modify_op encodes context steps and marks the op as map-shaped" do
    ctx = [Ctx.map_key("roles"), Ctx.list_index(0)]
    op = CDT.map_modify_op("prefs", 67, ["admin"], ctx)

    assert %Operation{op_type: op_type, map_cdt: true} = op
    assert op_type == Operation.op_cdt_modify()

    assert MessagePack.unpack!(op.data) == [
             255,
             [0x22, "roles", 0x10, 0],
             [67, "admin"]
           ]
  end

  test "map_read_op marks the op as map-shaped" do
    op = CDT.map_read_op("prefs", 98, [8, 0])

    assert %Operation{op_type: op_type, map_cdt: true} = op
    assert op_type == Operation.op_cdt_read()
    assert MessagePack.unpack!(op.data) == [98, 8, 0]
  end

  test "bit_read_op encodes bit CDT payloads without map classification" do
    op = CDT.bit_read_op("bits", 50, [0, 8])

    assert %Operation{
             op_type: op_type,
             particle_type: 4,
             bin_name: "bits",
             map_cdt: false
           } = op

    assert op_type == Operation.op_bit_read()
    assert MessagePack.unpack!(op.data) == [50, 0, 8]
  end

  test "bit_modify_op encodes context steps" do
    ctx = [Ctx.map_key("payload"), Ctx.list_index(0)]
    op = CDT.bit_modify_op("nested", 3, [4, 2, {:bytes, <<0b1100_0000>>}, 0], ctx)

    assert %Operation{op_type: op_type, map_cdt: false} = op
    assert op_type == Operation.op_bit_modify()

    assert MessagePack.unpack!(op.data) == [
             255,
             [0x22, "payload", 0x10, 0],
             [3, 4, 2, <<4, 0b1100_0000>>, 0]
           ]
  end

  test "hll_read_op encodes a context-free payload" do
    op = CDT.hll_read_op("hll", 50, [])

    assert %Operation{
             op_type: op_type,
             particle_type: 4,
             bin_name: "hll",
             map_cdt: false
           } = op

    assert op_type == Operation.op_hll_read()
    assert MessagePack.unpack!(op.data) == [50]
    refute function_exported?(CDT, :hll_read_op, 4)
  end

  test "hll_modify_op encodes strings and bytes without context support" do
    op = CDT.hll_modify_op("hll", 1, [["one", {:bytes, <<1, 2>>}], -1, -1, 0])

    assert %Operation{op_type: op_type, map_cdt: false} = op
    assert op_type == Operation.op_hll_modify()

    assert MessagePack.unpack!(op.data) == [
             1,
             ["one", <<4, 1, 2>>],
             -1,
             -1,
             0
           ]

    refute function_exported?(CDT, :hll_modify_op, 4)
  end

  test "cdt_pack_arg wraps nested strings but preserves bytes tuples" do
    assert CDT.cdt_pack_arg("name") == {:particle_string, "name"}
    assert CDT.cdt_pack_arg({:bytes, <<1, 2>>}) == {:bytes, <<1, 2>>}

    assert CDT.cdt_pack_arg(["a", 1, %{"k" => "v"}]) == [
             {:particle_string, "a"},
             1,
             %{{:particle_string, "k"} => {:particle_string, "v"}}
           ]
  end
end
