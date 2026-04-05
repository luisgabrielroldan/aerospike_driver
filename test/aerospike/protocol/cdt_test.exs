defmodule Aerospike.Protocol.CDTTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.CDT
  alias Aerospike.Protocol.MessagePack

  describe "3-arity calls (default ctx = nil)" do
    test "list_read_op/3 uses nil context by default" do
      op = CDT.list_read_op("bin", 16, [])
      assert op.op_type == Operation.op_cdt_read()
      assert op.bin_name == "bin"
      assert [16] = MessagePack.unpack!(op.data)
    end

    test "list_modify_op/3 uses nil context by default" do
      op = CDT.list_modify_op("bin", 1, ["val"])
      assert op.op_type == Operation.op_cdt_modify()
      assert op.bin_name == "bin"
    end

    test "map_read_op/3 uses nil context by default" do
      op = CDT.map_read_op("bin", 96, [])
      assert op.op_type == Operation.op_cdt_read()
      assert op.map_cdt == true
    end

    test "map_modify_op/3 uses nil context by default" do
      op = CDT.map_modify_op("bin", 67, ["k", "v", 0])
      assert op.op_type == Operation.op_cdt_modify()
      assert op.map_cdt == true
    end

    test "bit_read_op/3 uses nil context by default" do
      op = CDT.bit_read_op("bin", 50, [0, 8])
      assert op.op_type == Operation.op_bit_read()
    end

    test "bit_modify_op/3 uses nil context by default" do
      op = CDT.bit_modify_op("bin", 0, [16, 0, 0])
      assert op.op_type == Operation.op_bit_modify()
    end
  end

  describe "encode_payload with empty context" do
    test "empty context list is treated same as nil" do
      op_nil = CDT.list_read_op("bin", 16, [], nil)
      op_empty = CDT.list_read_op("bin", 16, [], [])
      assert op_nil.data == op_empty.data
    end
  end

  describe "cdt_pack_arg/1" do
    test "passes through {:bytes, _} tuples unchanged" do
      assert CDT.cdt_pack_arg({:bytes, <<1, 2, 3>>}) == {:bytes, <<1, 2, 3>>}
    end

    test "wraps strings as particle_string" do
      assert CDT.cdt_pack_arg("hello") == {:particle_string, "hello"}
    end

    test "recursively wraps list elements" do
      assert CDT.cdt_pack_arg(["a", "b"]) == [{:particle_string, "a"}, {:particle_string, "b"}]
    end

    test "recursively wraps map keys and values" do
      result = CDT.cdt_pack_arg(%{"k" => "v"})
      assert result == %{{:particle_string, "k"} => {:particle_string, "v"}}
    end

    test "passes through integers and other terms" do
      assert CDT.cdt_pack_arg(42) == 42
      assert CDT.cdt_pack_arg(nil) == nil
      assert CDT.cdt_pack_arg(true) == true
    end
  end

  describe "context encoding" do
    test "non-empty context wraps with 0xFF eval marker" do
      ctx = [Ctx.map_key("nested")]
      op = CDT.list_modify_op("bin", 1, ["x"], ctx)
      decoded = MessagePack.unpack!(op.data)
      assert hd(decoded) == 0xFF
    end
  end
end
