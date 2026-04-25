defmodule Aerospike.Protocol.AsmMsg.OperationTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  describe "constants" do
    test "exposes expression operation and blob particle types" do
      assert Operation.op_exp_read() == 7
      assert Operation.op_exp_modify() == 8
      assert Operation.particle_blob() == 4
    end
  end

  describe "from_simple/1" do
    test "admits the narrowed write-scoped mutation family" do
      assert {:ok, %Operation{} = add_operation} = Operation.from_simple({:add, "count", 1})
      assert add_operation.op_type == Operation.op_add()

      assert {:ok, %Operation{} = append_operation} =
               Operation.from_simple({:append, "name", "x"})

      assert append_operation.op_type == Operation.op_append()

      assert {:ok, %Operation{} = prepend_operation} =
               Operation.from_simple({:prepend, "name", "x"})

      assert prepend_operation.op_type == Operation.op_prepend()

      assert {:ok, %Operation{} = touch_operation} = Operation.from_simple(:touch)
      assert touch_operation.op_type == Operation.op_touch()

      assert {:ok, %Operation{} = delete_operation} = Operation.from_simple(:delete)
      assert delete_operation.op_type == Operation.op_delete()
    end

    test "rejects unsupported tuples with a narrow error message" do
      assert {:error, %Aerospike.Error{code: :invalid_argument, message: message}} =
               Operation.from_simple({:explode, "count", 1})

      assert message =~ "supported shapes"
    end
  end

  describe "expression operations" do
    test "exp_read/3 encodes expression bytes directly in the payload array" do
      expression = Aerospike.Exp.int_bin("age")

      assert {:ok, %Operation{} = operation} =
               Operation.exp_read("result", expression, 16)

      assert operation.op_type == Operation.op_exp_read()
      assert operation.particle_type == Operation.particle_blob()
      assert operation.bin_name == "result"
      assert operation.data == <<0x92>> <> expression.wire <> MessagePack.pack!(16)
      assert MessagePack.unpack!(operation.data) == [[81, 2, "age"], 16]
    end

    test "exp_modify/3 encodes as a blob operation" do
      expression = Aerospike.Exp.int(99)

      assert {:ok, %Operation{} = operation} =
               Operation.exp_modify("computed", expression, 1)

      assert operation.op_type == Operation.op_exp_modify()
      assert operation.particle_type == Operation.particle_blob()
      assert operation.data == <<0x92>> <> expression.wire <> MessagePack.pack!(1)
    end

    test "rejects invalid expression operation inputs" do
      assert {:error, %Aerospike.Error{code: :invalid_argument, message: empty_message}} =
               Operation.exp_read("result", Aerospike.Exp.from_wire(""))

      assert empty_message =~ "expression read expression wire must be non-empty"

      assert {:error, %Aerospike.Error{code: :invalid_argument, message: bin_message}} =
               Operation.exp_modify("", Aerospike.Exp.int(1))

      assert bin_message =~ "expression write bin name must be a non-empty binary"

      assert {:error, %Aerospike.Error{code: :invalid_argument, message: flags_message}} =
               Operation.exp_modify("computed", Aerospike.Exp.int(1), -1)

      assert flags_message =~ "expression write flags must be a non-negative integer"
    end
  end
end
