defmodule Aerospike.Op.ListTest do
  use ExUnit.Case, async: true

  alias Aerospike.Op.List, as: ListOp
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  test "append encodes a CDT modify operation" do
    op = ListOp.append("tags", "vip")

    assert op.op_type == Operation.op_cdt_modify()
    assert MessagePack.unpack!(op.data) == [1, "vip"]
  end

  test "size encodes a CDT read operation" do
    op = ListOp.size("tags")

    assert op.op_type == Operation.op_cdt_read()
    assert MessagePack.unpack!(op.data) == [16]
  end

  test "get includes the default return type" do
    op = ListOp.get("tags", 2)

    assert op.op_type == Operation.op_cdt_read()
    assert MessagePack.unpack!(op.data) == [17, 2]
  end
end
