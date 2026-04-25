defmodule Aerospike.Op.ExpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Exp
  alias Aerospike.Op.Exp, as: ExpOp
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  describe "read/3" do
    test "builds an expression read operation" do
      expression = Exp.int_bin("age")
      op = ExpOp.read(:result, expression)

      assert op.op_type == Operation.op_exp_read()
      assert op.particle_type == Operation.particle_blob()
      assert op.bin_name == "result"
      assert op.data == <<0x92>> <> expression.wire <> MessagePack.pack!(0)
      assert MessagePack.unpack!(op.data) == [[81, 2, "age"], 0]
    end

    test "accepts raw integer flags" do
      expression = Exp.int(42)
      op = ExpOp.read("result", expression, flags: 16)

      assert op.data == <<0x92>> <> expression.wire <> MessagePack.pack!(16)
      assert MessagePack.unpack!(op.data) == [42, 16]
    end

    test "rejects empty expression wire" do
      assert_raise ArgumentError, ~r/expression read expression wire must be non-empty/, fn ->
        ExpOp.read("result", Exp.from_wire(""))
      end
    end
  end

  describe "write/3" do
    test "builds an expression write operation" do
      expression = Exp.int(99)
      op = ExpOp.write(:computed, expression)

      assert op.op_type == Operation.op_exp_modify()
      assert op.particle_type == Operation.particle_blob()
      assert op.bin_name == "computed"
      assert op.data == <<0x92>> <> expression.wire <> MessagePack.pack!(0)
      assert MessagePack.unpack!(op.data) == [99, 0]
    end

    test "accepts raw integer flags" do
      expression = Exp.int(7)
      op = ExpOp.write("computed", expression, flags: 1)

      assert op.data == <<0x92>> <> expression.wire <> MessagePack.pack!(1)
      assert MessagePack.unpack!(op.data) == [7, 1]
    end

    test "rejects invalid flags" do
      assert_raise ArgumentError, ~r/expression write flags must be a non-negative integer/, fn ->
        ExpOp.write("computed", Exp.int(1), flags: :create_only)
      end
    end
  end
end
