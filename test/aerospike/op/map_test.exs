defmodule Aerospike.Op.MapTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Op.Map, as: MapOp
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  defp decode_payload(op), do: MessagePack.unpack!(op.data)
  defp opcode(op), do: hd(decode_payload(op))

  describe "return type helpers" do
    test "return_none/0" do
      assert MapOp.return_none() == 0
    end

    test "return_index/0" do
      assert MapOp.return_index() == 1
    end

    test "return_reverse_index/0" do
      assert MapOp.return_reverse_index() == 2
    end

    test "return_rank/0" do
      assert MapOp.return_rank() == 3
    end

    test "return_reverse_rank/0" do
      assert MapOp.return_reverse_rank() == 4
    end

    test "return_count/0" do
      assert MapOp.return_count() == 5
    end

    test "return_key/0" do
      assert MapOp.return_key() == 6
    end

    test "return_value/0" do
      assert MapOp.return_value() == 7
    end

    test "return_key_value/0" do
      assert MapOp.return_key_value() == 8
    end

    test "return_exists/0" do
      assert MapOp.return_exists() == 13
    end
  end

  describe "write operations" do
    test "set_policy" do
      op = MapOp.set_policy("map", 1)
      assert op.op_type == Operation.op_cdt_modify()
      assert op.map_cdt == true
      assert opcode(op) == 64
    end

    test "put without flags" do
      op = MapOp.put("map", "k", "v")
      assert opcode(op) == 67
      payload = decode_payload(op)
      assert length(payload) == 4
    end

    test "put with flags" do
      op = MapOp.put("map", "k", "v", policy: %{attr: 1, flags: 2})
      assert opcode(op) == 67
      payload = decode_payload(op)
      assert length(payload) == 5
    end

    test "put_items without flags" do
      op = MapOp.put_items("map", %{"a" => 1})
      assert opcode(op) == 68
      payload = decode_payload(op)
      assert length(payload) == 3
    end

    test "put_items with flags" do
      op = MapOp.put_items("map", %{"a" => 1}, policy: %{attr: 1, flags: 2})
      assert opcode(op) == 68
      payload = decode_payload(op)
      assert length(payload) == 4
    end

    test "increment" do
      op = MapOp.increment("map", "counter", 1)
      assert opcode(op) == 73
    end

    test "decrement" do
      op = MapOp.decrement("map", "counter", 1)
      assert opcode(op) == 74
    end

    test "clear" do
      op = MapOp.clear("map")
      assert opcode(op) == 75
    end
  end

  describe "remove operations" do
    test "remove_by_key" do
      op = MapOp.remove_by_key("map", "k")
      assert op.op_type == Operation.op_cdt_modify()
      assert opcode(op) == 76
    end

    test "remove_by_key_list" do
      op = MapOp.remove_by_key_list("map", ["a", "b"])
      assert opcode(op) == 81
    end

    test "remove_by_key_range" do
      op = MapOp.remove_by_key_range("map", "a", "z")
      assert opcode(op) == 84
    end

    test "remove_by_value" do
      op = MapOp.remove_by_value("map", 42)
      assert opcode(op) == 82
    end

    test "remove_by_value_list" do
      op = MapOp.remove_by_value_list("map", [1, 2])
      assert opcode(op) == 83
    end

    test "remove_by_value_range" do
      op = MapOp.remove_by_value_range("map", 1, 10)
      assert opcode(op) == 86
    end

    test "remove_by_index" do
      op = MapOp.remove_by_index("map", 0)
      assert opcode(op) == 77
    end

    test "remove_by_index_range_from" do
      op = MapOp.remove_by_index_range_from("map", 0)
      assert opcode(op) == 85
    end

    test "remove_by_index_range" do
      op = MapOp.remove_by_index_range("map", 0, 3)
      assert opcode(op) == 85
    end

    test "remove_by_rank" do
      op = MapOp.remove_by_rank("map", 0)
      assert opcode(op) == 79
    end

    test "remove_by_rank_range_from" do
      op = MapOp.remove_by_rank_range_from("map", 0)
      assert opcode(op) == 87
    end

    test "remove_by_rank_range" do
      op = MapOp.remove_by_rank_range("map", 0, 3)
      assert opcode(op) == 87
    end
  end

  describe "read operations" do
    test "size" do
      op = MapOp.size("map")
      assert op.op_type == Operation.op_cdt_read()
      assert op.map_cdt == true
      assert opcode(op) == 96
    end

    test "get_by_key" do
      op = MapOp.get_by_key("map", "k")
      assert opcode(op) == 97
    end

    test "get_by_key_list" do
      op = MapOp.get_by_key_list("map", ["a", "b"])
      assert opcode(op) == 107
    end

    test "get_by_key_range" do
      op = MapOp.get_by_key_range("map", "a", "z")
      assert opcode(op) == 103
    end

    test "get_by_value" do
      op = MapOp.get_by_value("map", 42)
      assert opcode(op) == 102
    end

    test "get_by_value_list" do
      op = MapOp.get_by_value_list("map", [1, 2])
      assert opcode(op) == 108
    end

    test "get_by_value_range" do
      op = MapOp.get_by_value_range("map", 1, 10)
      assert opcode(op) == 105
    end

    test "get_by_index" do
      op = MapOp.get_by_index("map", 0)
      assert opcode(op) == 98
    end

    test "get_by_index_range_from" do
      op = MapOp.get_by_index_range_from("map", 0)
      assert opcode(op) == 104
    end

    test "get_by_index_range" do
      op = MapOp.get_by_index_range("map", 0, 3)
      assert opcode(op) == 104
    end

    test "get_by_rank" do
      op = MapOp.get_by_rank("map", 0)
      assert opcode(op) == 100
    end

    test "get_by_rank_range_from" do
      op = MapOp.get_by_rank_range_from("map", 0)
      assert opcode(op) == 106
    end

    test "get_by_rank_range" do
      op = MapOp.get_by_rank_range("map", 0, 3)
      assert opcode(op) == 106
    end
  end

  describe "context support" do
    test "operations accept ctx option" do
      ctx = [Ctx.map_key("nested")]
      op = MapOp.put("map", "k", "v", ctx: ctx)

      payload = decode_payload(op)
      assert hd(payload) == 0xFF
    end

    test "return_type option overrides default" do
      op = MapOp.get_by_key("map", "k", return_type: MapOp.return_key())
      payload = decode_payload(op)
      [_opcode, rt | _] = payload
      assert rt == 6
    end

    test "remove return_type option overrides default" do
      op = MapOp.remove_by_key("map", "k", return_type: MapOp.return_none())
      payload = decode_payload(op)
      [_opcode, rt | _] = payload
      assert rt == 0
    end
  end
end
