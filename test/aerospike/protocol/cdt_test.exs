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
