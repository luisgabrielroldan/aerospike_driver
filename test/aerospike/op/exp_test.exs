defmodule Aerospike.Op.ExpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Exp
  alias Aerospike.Op.Exp, as: OpExp
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  describe "read/3" do
    test "returns Operation struct with op type exp_read (7)" do
      op = OpExp.read("result", Exp.int(42))
      assert op.op_type == Operation.op_exp_read()
      assert op.op_type == 7
    end

    test "sets particle type to BLOB (4)" do
      op = OpExp.read("result", Exp.int(42))
      assert op.particle_type == Operation.particle_blob()
      assert op.particle_type == 4
    end

    test "sets bin_name correctly" do
      op = OpExp.read("my_result", Exp.int(1))
      assert op.bin_name == "my_result"
    end

    test "data is a 2-element msgpack array containing expression bytes and flags" do
      exp = Exp.int(42)
      op = OpExp.read("result", exp)
      # fixarray-of-2 header
      assert <<0x92, rest::binary>> = op.data
      # first element is the expression msgpack value, second is the flags int
      {:ok, {_exp_value, flags_bytes}} = MessagePack.unpack(rest)
      assert {:ok, {0, <<>>}} = MessagePack.unpack(flags_bytes)
    end

    test "expression bytes in data match the exp wire" do
      exp = Exp.int_bin("age")
      op = OpExp.read("result", exp)
      <<0x92, rest::binary>> = op.data
      # The expression portion of the array decodes to a list (the bin-read array node)
      {:ok, {decoded_exp, _flags_bytes}} = MessagePack.unpack(rest)
      # int_bin("age") encodes as [81, 2, "age"] — a 3-element msgpack array
      assert [81, 2, "age"] = decoded_exp
    end

    test "accepts custom flags" do
      op = OpExp.read("result", Exp.int(1), flags: 16)
      <<0x92, rest::binary>> = op.data
      {:ok, {_exp_value, flags_bytes}} = MessagePack.unpack(rest)
      assert {:ok, {16, <<>>}} = MessagePack.unpack(flags_bytes)
    end

    test "works with a comparison expression" do
      exp = Exp.gt(Exp.int_bin("score"), Exp.int(100))
      op = OpExp.read("result", exp)
      assert is_binary(op.data)
      assert <<0x92, _rest::binary>> = op.data
    end
  end

  describe "write/3" do
    test "returns Operation struct with op type exp_modify (8)" do
      op = OpExp.write("computed", Exp.int(42))
      assert op.op_type == Operation.op_exp_modify()
      assert op.op_type == 8
    end

    test "sets particle type to BLOB (4)" do
      op = OpExp.write("computed", Exp.int(42))
      assert op.particle_type == Operation.particle_blob()
    end

    test "sets bin_name correctly" do
      op = OpExp.write("my_bin", Exp.int(1))
      assert op.bin_name == "my_bin"
    end

    test "data is a 2-element msgpack array" do
      op = OpExp.write("computed", Exp.int(42))
      assert <<0x92, rest::binary>> = op.data
      {:ok, {_exp_value, flags_bytes}} = MessagePack.unpack(rest)
      assert {:ok, {0, <<>>}} = MessagePack.unpack(flags_bytes)
    end

    test "accepts custom flags" do
      op = OpExp.write("computed", Exp.int(1), flags: 1)
      <<0x92, rest::binary>> = op.data
      {:ok, {_exp_value, flags_bytes}} = MessagePack.unpack(rest)
      assert {:ok, {1, <<>>}} = MessagePack.unpack(flags_bytes)
    end
  end
end
