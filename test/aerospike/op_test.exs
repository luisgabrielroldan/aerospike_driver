defmodule Aerospike.OpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Op
  alias Aerospike.Protocol.AsmMsg.Operation

  test "put encodes as WRITE with encoded value" do
    op = Op.put("name", "Ada")
    assert %Operation{op_type: ot, particle_type: pt, bin_name: "name", data: data} = op
    assert ot == Operation.op_write()
    assert pt == Operation.particle_string()
    assert data == "Ada"
  end

  test "get is READ op" do
    op = Op.get("age")
    assert op.op_type == Operation.op_read()
    assert op.bin_name == "age"
  end

  test "get_header marks read_header" do
    op = Op.get_header()
    assert op.op_type == Operation.op_read()
    assert op.read_header == true
    assert op.bin_name == ""
  end

  test "add uses ADD op type" do
    op = Op.add("c", 1)
    assert op.op_type == Operation.op_add()
  end

  test "add encodes float delta" do
    op = Op.add("f", 1.5)
    assert op.op_type == Operation.op_add()
    assert op.particle_type == 2
    assert op.data == <<1.5::64-float-big>>
  end

  test "append and prepend" do
    assert Op.append("s", "x").op_type == Operation.op_append()
    assert Op.prepend("s", "x").op_type == Operation.op_prepend()
  end
end
