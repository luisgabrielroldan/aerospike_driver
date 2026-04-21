defmodule Aerospike.OpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Op
  alias Aerospike.Protocol.AsmMsg.Operation

  test "put normalizes atom bin names and builds a write op" do
    op = Op.put(:count, 7)

    assert op.op_type == Operation.op_write()
    assert op.bin_name == "count"
  end

  test "get and get_header build the expected read variants" do
    assert %Operation{op_type: type, bin_name: "name", read_header: false} = Op.get(:name)
    assert type == Operation.op_read()

    assert %Operation{
             op_type: header_type,
             particle_type: particle_type,
             bin_name: "",
             data: <<>>,
             read_header: true
           } = Op.get_header()

    assert header_type == Operation.op_read()
    assert particle_type == Operation.particle_null()
  end

  test "add, append, and prepend normalize atom bin names" do
    assert %Operation{op_type: add_type, bin_name: "count"} = Op.add(:count, 2)
    assert add_type == Operation.op_add()

    assert %Operation{op_type: append_type, bin_name: "name"} = Op.append(:name, "x")
    assert append_type == Operation.op_append()

    assert %Operation{op_type: prepend_type, bin_name: "name"} = Op.prepend(:name, "x")
    assert prepend_type == Operation.op_prepend()
  end
end
