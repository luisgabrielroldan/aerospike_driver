defmodule Aerospike.Op.HLLTest do
  use ExUnit.Case, async: true

  alias Aerospike.Op.HLL
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  defp decode_payload(op), do: MessagePack.unpack!(op.data)
  defp opcode(op), do: hd(decode_payload(op))

  describe "modify operations" do
    test "init with defaults" do
      op = HLL.init("hll")
      assert op.op_type == Operation.op_hll_modify()
      assert op.bin_name == "hll"
      assert opcode(op) == 0
    end

    test "init with explicit params" do
      op = HLL.init("hll", 10, 20)
      assert opcode(op) == 0
      payload = decode_payload(op)
      assert Enum.at(payload, 1) == 10
      assert Enum.at(payload, 2) == 20
    end

    test "add with defaults" do
      op = HLL.add("hll", ["a", "b", "c"])
      assert opcode(op) == 1
    end

    test "add with explicit params" do
      op = HLL.add("hll", ["a", "b"], 8, 16)
      assert opcode(op) == 1
    end

    test "set_union" do
      op = HLL.set_union("hll", [{:bytes, <<1, 2, 3>>}])
      assert opcode(op) == 2
    end

    test "refresh_count" do
      op = HLL.refresh_count("hll")
      assert opcode(op) == 3
    end

    test "fold" do
      op = HLL.fold("hll", 6)
      assert opcode(op) == 4
    end
  end

  describe "read operations" do
    test "get_count" do
      op = HLL.get_count("hll")
      assert op.op_type == Operation.op_hll_read()
      assert opcode(op) == 50
    end

    test "get_union" do
      op = HLL.get_union("hll", [{:bytes, <<1, 2, 3>>}])
      assert opcode(op) == 51
    end

    test "get_union_count" do
      op = HLL.get_union_count("hll", [{:bytes, <<1, 2, 3>>}])
      assert opcode(op) == 52
    end

    test "get_intersect_count" do
      op = HLL.get_intersect_count("hll", [{:bytes, <<1, 2, 3>>}])
      assert opcode(op) == 53
    end

    test "get_similarity" do
      op = HLL.get_similarity("hll", [{:bytes, <<1, 2, 3>>}])
      assert opcode(op) == 54
    end

    test "describe" do
      op = HLL.describe("hll")
      assert opcode(op) == 55
    end
  end
end
