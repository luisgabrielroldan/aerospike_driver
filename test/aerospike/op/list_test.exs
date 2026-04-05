defmodule Aerospike.Op.ListTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Op.List, as: ListOp
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  defp decode_payload(op), do: MessagePack.unpack!(op.data)
  defp opcode(op), do: hd(decode_payload(op))

  describe "return type helpers" do
    test "return_none/0" do
      assert ListOp.return_none() == 0
    end

    test "return_value/0" do
      assert ListOp.return_value() == 7
    end

    test "return_count/0" do
      assert ListOp.return_count() == 5
    end

    test "return_index/0" do
      assert ListOp.return_index() == 1
    end
  end

  describe "modify operations" do
    test "set_type" do
      op = ListOp.set_type("list", 1, 0)
      assert op.op_type == Operation.op_cdt_modify()
      assert op.bin_name == "list"
      assert opcode(op) == 0
    end

    test "append without policy" do
      op = ListOp.append("list", "val")
      assert op.op_type == Operation.op_cdt_modify()
      assert opcode(op) == 1
    end

    test "append with policy" do
      op = ListOp.append("list", "val", policy: %{order: 1, flags: 0})
      assert opcode(op) == 1
      payload = decode_payload(op)
      assert length(payload) == 4
    end

    test "append_items without policy" do
      op = ListOp.append_items("list", [1, 2, 3])
      assert opcode(op) == 2
    end

    test "append_items with policy" do
      op = ListOp.append_items("list", [1, 2], policy: %{order: 1, flags: 0})
      assert opcode(op) == 2
      payload = decode_payload(op)
      assert length(payload) == 4
    end

    test "insert without policy" do
      op = ListOp.insert("list", 0, "val")
      assert opcode(op) == 3
    end

    test "insert with policy" do
      op = ListOp.insert("list", 0, "val", policy: %{order: 1, flags: 0})
      assert opcode(op) == 3
      payload = decode_payload(op)
      assert length(payload) == 5
    end

    test "insert_items without policy" do
      op = ListOp.insert_items("list", 0, [1, 2])
      assert opcode(op) == 4
    end

    test "insert_items with policy" do
      op = ListOp.insert_items("list", 0, [1, 2], policy: %{order: 1, flags: 0})
      assert opcode(op) == 4
      payload = decode_payload(op)
      assert length(payload) == 5
    end

    test "pop" do
      op = ListOp.pop("list", 0)
      assert opcode(op) == 5
    end

    test "pop_range" do
      op = ListOp.pop_range("list", 0, 3)
      assert opcode(op) == 6
    end

    test "remove" do
      op = ListOp.remove("list", 0)
      assert opcode(op) == 7
    end

    test "remove_range" do
      op = ListOp.remove_range("list", 0, 3)
      assert opcode(op) == 8
    end

    test "set without policy" do
      op = ListOp.set("list", 0, "x")
      assert opcode(op) == 9
    end

    test "set with policy" do
      op = ListOp.set("list", 0, "x", policy: %{order: 1, flags: 0})
      assert opcode(op) == 9
      payload = decode_payload(op)
      assert length(payload) == 5
    end

    test "trim" do
      op = ListOp.trim("list", 0, 5)
      assert opcode(op) == 10
    end

    test "clear" do
      op = ListOp.clear("list")
      assert opcode(op) == 11
    end

    test "increment without policy" do
      op = ListOp.increment("list", 0, 10)
      assert opcode(op) == 12
    end

    test "increment with policy" do
      op = ListOp.increment("list", 0, 10, policy: %{flags: 2})
      assert opcode(op) == 12
      payload = decode_payload(op)
      assert length(payload) == 4
    end

    test "sort" do
      op = ListOp.sort("list")
      assert opcode(op) == 13
    end

    test "sort with flags" do
      op = ListOp.sort("list", 1)
      assert opcode(op) == 13
    end
  end

  describe "read operations" do
    test "size" do
      op = ListOp.size("list")
      assert op.op_type == Operation.op_cdt_read()
      assert opcode(op) == 16
    end

    test "get" do
      op = ListOp.get("list", 0)
      assert op.op_type == Operation.op_cdt_read()
      assert opcode(op) == 17
    end

    test "get_range" do
      op = ListOp.get_range("list", 0, 5)
      assert opcode(op) == 18
    end

    test "get_range_from" do
      op = ListOp.get_range_from("list", 2)
      assert opcode(op) == 18
    end
  end

  describe "get_by operations" do
    test "get_by_index" do
      op = ListOp.get_by_index("list", 0)
      assert opcode(op) == 19
    end

    test "get_by_index_range_from" do
      op = ListOp.get_by_index_range_from("list", 0)
      assert opcode(op) == 24
    end

    test "get_by_index_range" do
      op = ListOp.get_by_index_range("list", 0, 3)
      assert opcode(op) == 24
    end

    test "get_by_rank" do
      op = ListOp.get_by_rank("list", 0)
      assert opcode(op) == 21
    end

    test "get_by_rank_range_from" do
      op = ListOp.get_by_rank_range_from("list", 0)
      assert opcode(op) == 26
    end

    test "get_by_rank_range" do
      op = ListOp.get_by_rank_range("list", 0, 3)
      assert opcode(op) == 26
    end

    test "get_by_value" do
      op = ListOp.get_by_value("list", 42)
      assert opcode(op) == 22
    end

    test "get_by_value_list" do
      op = ListOp.get_by_value_list("list", [1, 2])
      assert opcode(op) == 23
    end

    test "get_by_value_range" do
      op = ListOp.get_by_value_range("list", 1, 10)
      assert opcode(op) == 25
    end

    test "get_by_value_rel_rank_range" do
      op = ListOp.get_by_value_rel_rank_range("list", 5, 0)
      assert opcode(op) == 27
    end

    test "get_by_value_rel_rank_range_count" do
      op = ListOp.get_by_value_rel_rank_range_count("list", 5, 0, 3)
      assert opcode(op) == 27
    end
  end

  describe "remove_by operations" do
    test "remove_by_index" do
      op = ListOp.remove_by_index("list", 0)
      assert op.op_type == Operation.op_cdt_modify()
      assert opcode(op) == 32
    end

    test "remove_by_index_range_from" do
      op = ListOp.remove_by_index_range_from("list", 0)
      assert opcode(op) == 37
    end

    test "remove_by_index_range" do
      op = ListOp.remove_by_index_range("list", 0, 3)
      assert opcode(op) == 37
    end

    test "remove_by_rank" do
      op = ListOp.remove_by_rank("list", 0)
      assert opcode(op) == 34
    end

    test "remove_by_rank_range_from" do
      op = ListOp.remove_by_rank_range_from("list", 0)
      assert opcode(op) == 39
    end

    test "remove_by_rank_range" do
      op = ListOp.remove_by_rank_range("list", 0, 3)
      assert opcode(op) == 39
    end

    test "remove_by_value" do
      op = ListOp.remove_by_value("list", 42)
      assert opcode(op) == 35
    end

    test "remove_by_value_list" do
      op = ListOp.remove_by_value_list("list", [1, 2])
      assert opcode(op) == 36
    end

    test "remove_by_value_range" do
      op = ListOp.remove_by_value_range("list", 1, 10)
      assert opcode(op) == 38
    end

    test "remove_by_value_rel_rank_range" do
      op = ListOp.remove_by_value_rel_rank_range("list", 5, 0)
      assert opcode(op) == 40
    end

    test "remove_by_value_rel_rank_range_count" do
      op = ListOp.remove_by_value_rel_rank_range_count("list", 5, 0, 3)
      assert opcode(op) == 40
    end
  end

  describe "context support" do
    test "operations accept ctx option" do
      ctx = [Ctx.list_index(0)]
      op = ListOp.append("list", "val", ctx: ctx)

      payload = decode_payload(op)
      assert hd(payload) == 0xFF
    end

    test "return_type option overrides default" do
      op = ListOp.get_by_index("list", 0, return_type: ListOp.return_count())
      payload = decode_payload(op)
      [_opcode, rt | _] = payload
      assert rt == 5
    end
  end
end
