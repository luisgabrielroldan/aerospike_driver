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
end
