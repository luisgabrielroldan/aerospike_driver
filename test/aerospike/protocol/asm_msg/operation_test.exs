defmodule Aerospike.Protocol.AsmMsg.OperationTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.AsmMsg.Operation

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
end
