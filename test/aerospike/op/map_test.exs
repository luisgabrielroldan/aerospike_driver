defmodule Aerospike.Op.MapTest do
  use ExUnit.Case, async: true

  alias Aerospike.Op.Map, as: MapOp
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  test "put_items encodes a CDT modify operation" do
    op = MapOp.put_items("prefs", %{"theme" => "dark"})

    assert op.op_type == Operation.op_cdt_modify()
    assert MessagePack.unpack!(op.data) == [68, %{"theme" => "dark"}, 0]
  end

  test "increment encodes a CDT modify operation" do
    op = MapOp.increment("metrics", "count", 5)

    assert op.op_type == Operation.op_cdt_modify()
    assert MessagePack.unpack!(op.data) == [73, "count", 5, 0]
  end

  test "get_by_key includes the default return type" do
    op = MapOp.get_by_key("prefs", "theme")

    assert op.op_type == Operation.op_cdt_read()
    assert MessagePack.unpack!(op.data) == [97, 7, "theme"]
  end

  test "size without ctx encodes the bare opcode payload" do
    op = MapOp.size("prefs")

    assert op.op_type == Operation.op_cdt_read()
    assert MessagePack.unpack!(op.data) == [96]
  end

  test "size uses a read op and map helpers forward ctx and return_type options" do
    ctx = [Aerospike.Ctx.map_key("prefs")]

    size_op = MapOp.size("prefs", ctx: ctx)
    assert size_op.op_type == Operation.op_cdt_read()
    assert MessagePack.unpack!(size_op.data) == [255, [34, "prefs"], [96]]

    put_op = MapOp.put_items("prefs", %{"theme" => "dark"}, ctx: ctx)
    assert MessagePack.unpack!(put_op.data) == [255, [34, "prefs"], [68, %{"theme" => "dark"}, 0]]

    increment_op = MapOp.increment("prefs", "count", 2, ctx: ctx)
    assert MessagePack.unpack!(increment_op.data) == [255, [34, "prefs"], [73, "count", 2, 0]]

    get_op = MapOp.get_by_key("prefs", "theme", return_type: 1, ctx: ctx)
    assert MessagePack.unpack!(get_op.data) == [255, [34, "prefs"], [97, 1, "theme"]]
  end
end
