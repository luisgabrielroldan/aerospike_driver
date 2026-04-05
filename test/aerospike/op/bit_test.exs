defmodule Aerospike.Op.BitTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Op.Bit
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  defp decode_payload(op), do: MessagePack.unpack!(op.data)
  defp opcode(op), do: hd(decode_payload(op))

  describe "modify operations" do
    test "resize" do
      op = Bit.resize("bits", 16)
      assert op.op_type == Operation.op_bit_modify()
      assert op.bin_name == "bits"
      assert opcode(op) == 0
    end

    test "insert" do
      op = Bit.insert("bits", 0, <<0xFF>>)
      assert opcode(op) == 1
    end

    test "remove" do
      op = Bit.remove("bits", 0, 4)
      assert opcode(op) == 2
    end

    test "set" do
      op = Bit.set("bits", 0, 8, <<0xFF>>)
      assert opcode(op) == 3
    end

    test "bw_or" do
      op = Bit.bw_or("bits", 0, 8, <<0xFF>>)
      assert opcode(op) == 4
    end

    test "bw_xor" do
      op = Bit.bw_xor("bits", 0, 8, <<0xFF>>)
      assert opcode(op) == 5
    end

    test "bw_and" do
      op = Bit.bw_and("bits", 0, 8, <<0xFF>>)
      assert opcode(op) == 6
    end

    test "bw_not" do
      op = Bit.bw_not("bits", 0, 8)
      assert opcode(op) == 7
    end

    test "lshift" do
      op = Bit.lshift("bits", 0, 8, 2)
      assert opcode(op) == 8
    end

    test "rshift" do
      op = Bit.rshift("bits", 0, 8, 2)
      assert opcode(op) == 9
    end

    test "add unsigned" do
      op = Bit.add("bits", 0, 8, 1)
      assert opcode(op) == 10
    end

    test "add signed" do
      op = Bit.add("bits", 0, 8, 1, signed: true)
      assert opcode(op) == 10
      payload = decode_payload(op)
      action = List.last(payload)
      assert Bitwise.band(action, 1) == 1
    end

    test "subtract unsigned" do
      op = Bit.subtract("bits", 0, 8, 1)
      assert opcode(op) == 11
    end

    test "subtract signed" do
      op = Bit.subtract("bits", 0, 8, 1, signed: true)
      assert opcode(op) == 11
      payload = decode_payload(op)
      action = List.last(payload)
      assert Bitwise.band(action, 1) == 1
    end

    test "set_int" do
      op = Bit.set_int("bits", 0, 8, 42)
      assert opcode(op) == 12
    end
  end

  describe "read operations" do
    test "get" do
      op = Bit.get("bits", 0, 8)
      assert op.op_type == Operation.op_bit_read()
      assert opcode(op) == 50
    end

    test "count" do
      op = Bit.count("bits", 0, 8)
      assert opcode(op) == 51
    end

    test "lscan" do
      op = Bit.lscan("bits", 0, 8, true)
      assert opcode(op) == 52
    end

    test "rscan" do
      op = Bit.rscan("bits", 0, 8, false)
      assert opcode(op) == 53
    end

    test "get_int unsigned" do
      op = Bit.get_int("bits", 0, 8)
      assert opcode(op) == 54
      payload = decode_payload(op)
      assert length(payload) == 3
    end

    test "get_int signed" do
      op = Bit.get_int("bits", 0, 8, true)
      assert opcode(op) == 54
      payload = decode_payload(op)
      assert length(payload) == 4
    end
  end

  describe "options" do
    test "flags option" do
      op = Bit.resize("bits", 16, 0, flags: 1)
      payload = decode_payload(op)
      [_opcode, _byte_size, flags_val | _] = payload
      assert flags_val == 1
    end

    test "ctx option" do
      ctx = [Ctx.list_index(0)]
      op = Bit.get("bits", 0, 8, ctx: ctx)
      payload = decode_payload(op)
      assert hd(payload) == 0xFF
    end
  end
end
