defmodule Aerospike.Protocol.OperateFlagsTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.OperateFlags

  describe "scan_ops/1 expression operation classification" do
    test "expression read is read-like and asks for all operation results" do
      result = OperateFlags.scan_ops([op(Operation.op_exp_read())])

      assert (result.info1 &&& AsmMsg.info1_read()) != 0
      assert result.read_bin? == true
      assert result.has_write? == false
      assert result.respond_all? == true
    end

    test "expression modify is write-like and asks for all operation results" do
      result = OperateFlags.scan_ops([op(Operation.op_exp_modify())])

      assert result.info1 == 0
      assert result.read_bin? == false
      assert result.has_write? == true
      assert result.respond_all? == true
    end
  end

  defp op(type), do: %Operation{op_type: type, bin_name: "result"}
end
