defmodule Aerospike.Protocol.AsmMsg.OperationTest do
  use ExUnit.Case, async: true
  doctest Aerospike.Protocol.AsmMsg.Operation

  alias Aerospike.Protocol.AsmMsg.Operation

  describe "operation type constants" do
    test "returns correct read type" do
      assert Operation.op_read() == 1
    end

    test "returns correct write type" do
      assert Operation.op_write() == 2
    end

    test "returns correct add type" do
      assert Operation.op_add() == 5
    end

    test "returns correct delete type" do
      assert Operation.op_delete() == 14
    end

    test "returns correct touch type" do
      assert Operation.op_touch() == 11
    end
  end

  describe "particle type constants" do
    test "returns correct null type" do
      assert Operation.particle_null() == 0
    end

    test "returns correct integer type" do
      assert Operation.particle_integer() == 1
    end

    test "returns correct string type" do
      assert Operation.particle_string() == 3
    end

    test "returns correct blob type" do
      assert Operation.particle_blob() == 4
    end

    test "returns correct float type" do
      assert Operation.particle_float() == 2
    end

    test "returns expected values for remaining operation type accessors" do
      assert Operation.op_cdt_read() == 3
      assert Operation.op_cdt_modify() == 4
      assert Operation.op_exp_read() == 7
      assert Operation.op_exp_modify() == 8
      assert Operation.op_append() == 9
      assert Operation.op_prepend() == 10
      assert Operation.op_bit_read() == 12
      assert Operation.op_bit_modify() == 13
      assert Operation.op_hll_read() == 15
      assert Operation.op_hll_modify() == 16
    end

    test "returns expected values for remaining particle type accessors" do
      assert Operation.particle_digest() == 6
      assert Operation.particle_bool() == 17
      assert Operation.particle_hll() == 18
      assert Operation.particle_map() == 19
      assert Operation.particle_list() == 20
      assert Operation.particle_geojson() == 23
    end
  end

  describe "encode/1" do
    test "encodes operation with empty bin name and data" do
      op = %Operation{op_type: 1, particle_type: 0, bin_name: "", data: <<>>}
      encoded = Operation.encode(op)
      # size=4, op_type=1, particle_type=0, reserved=0, name_len=0
      assert encoded == <<0, 0, 0, 4, 1, 0, 0, 0>>
    end

    test "encodes operation with bin name" do
      op = %Operation{op_type: 1, particle_type: 0, bin_name: "bin", data: <<>>}
      encoded = Operation.encode(op)
      # size=7 (4 + 3), op_type=1, particle_type=0, reserved=0, name_len=3, name="bin"
      assert encoded == <<0, 0, 0, 7, 1, 0, 0, 3, "bin">>
    end

    test "encodes operation with data" do
      op = %Operation{op_type: 2, particle_type: 3, bin_name: "bin", data: "value"}
      encoded = Operation.encode(op)
      # size=12 (4 + 3 + 5), op_type=2, particle_type=3, reserved=0, name_len=3
      assert encoded == <<0, 0, 0, 12, 2, 3, 0, 3, "bin", "value">>
    end
  end

  describe "decode/1" do
    test "decodes operation with no bin name or data" do
      binary = <<0, 0, 0, 4, 1, 0, 0, 0>>
      assert {:ok, op, <<>>} = Operation.decode(binary)
      assert op.op_type == 1
      assert op.particle_type == 0
      assert op.bin_name == ""
      assert op.data == <<>>
    end

    test "decodes operation with bin name" do
      binary = <<0, 0, 0, 7, 1, 0, 0, 3, "bin">>
      assert {:ok, op, <<>>} = Operation.decode(binary)
      assert op.op_type == 1
      assert op.bin_name == "bin"
      assert op.data == <<>>
    end

    test "decodes operation with data" do
      binary = <<0, 0, 0, 12, 2, 3, 0, 3, "bin", "value">>
      assert {:ok, op, <<>>} = Operation.decode(binary)
      assert op.op_type == 2
      assert op.particle_type == 3
      assert op.bin_name == "bin"
      assert op.data == "value"
    end

    test "decodes operation with remaining bytes" do
      binary = <<0, 0, 0, 7, 1, 0, 0, 3, "bin", "remaining">>
      assert {:ok, op, "remaining"} = Operation.decode(binary)
      assert op.bin_name == "bin"
    end

    test "returns error for incomplete header" do
      assert {:error, :incomplete_operation_header} = Operation.decode(<<0, 0, 0>>)
    end

    test "returns error for invalid operation size" do
      assert {:error, :invalid_operation_size} =
               Operation.decode(<<0, 0, 0, 3, 1, 0, 0, 0>>)
    end

    test "returns error for incomplete data" do
      # Header says size=20, but data is incomplete
      assert {:error, :incomplete_operation} =
               Operation.decode(<<0, 0, 0, 20, 1, 0, 0, 3, "bin">>)
    end
  end

  describe "encode/decode roundtrip" do
    test "roundtrips read operation" do
      op = Operation.read("mybin")
      assert {:ok, decoded, <<>>} = op |> Operation.encode() |> Operation.decode()
      assert decoded.op_type == Operation.op_read()
      assert decoded.particle_type == Operation.particle_null()
      assert decoded.bin_name == "mybin"
      assert decoded.data == <<>>
    end

    test "roundtrips write_string operation" do
      op = Operation.write_string("name", "Alice")
      assert {:ok, decoded, <<>>} = op |> Operation.encode() |> Operation.decode()
      assert decoded.op_type == Operation.op_write()
      assert decoded.particle_type == Operation.particle_string()
      assert decoded.bin_name == "name"
      assert decoded.data == "Alice"
    end

    test "roundtrips write_integer operation" do
      op = Operation.write_integer("counter", 42)
      assert {:ok, decoded, <<>>} = op |> Operation.encode() |> Operation.decode()
      assert decoded.op_type == Operation.op_write()
      assert decoded.particle_type == Operation.particle_integer()
      assert decoded.bin_name == "counter"
      assert decoded.data == <<42::64-big-signed>>
    end

    test "roundtrips write_float operation" do
      op = Operation.write_float("score", 3.14)
      assert {:ok, decoded, <<>>} = op |> Operation.encode() |> Operation.decode()
      assert decoded.op_type == Operation.op_write()
      assert decoded.particle_type == Operation.particle_float()
      assert decoded.bin_name == "score"
      <<value::64-float-big>> = decoded.data
      assert_in_delta value, 3.14, 0.001
    end

    test "roundtrips write_blob operation" do
      blob = <<1, 2, 3, 4, 5>>
      op = Operation.write_blob("data", blob)
      assert {:ok, decoded, <<>>} = op |> Operation.encode() |> Operation.decode()
      assert decoded.op_type == Operation.op_write()
      assert decoded.particle_type == Operation.particle_blob()
      assert decoded.bin_name == "data"
      assert decoded.data == blob
    end

    test "roundtrips add operation" do
      op = Operation.add("counter", 10)
      assert {:ok, decoded, <<>>} = op |> Operation.encode() |> Operation.decode()
      assert decoded.op_type == Operation.op_add()
      assert decoded.particle_type == Operation.particle_integer()
      assert decoded.bin_name == "counter"
      assert decoded.data == <<10::64-big-signed>>
    end

    test "roundtrips touch operation" do
      op = Operation.touch()
      assert {:ok, decoded, <<>>} = op |> Operation.encode() |> Operation.decode()
      assert decoded.op_type == Operation.op_touch()
      assert decoded.particle_type == Operation.particle_null()
      assert decoded.bin_name == ""
      assert decoded.data == <<>>
    end
  end

  describe "constructor functions" do
    test "read/1 creates correct operation" do
      op = Operation.read("bin")
      assert op.op_type == 1
      assert op.particle_type == 0
      assert op.bin_name == "bin"
    end

    test "write_integer/2 encodes value correctly" do
      op = Operation.write_integer("num", -123)
      assert op.op_type == 2
      assert op.particle_type == 1
      assert op.data == <<-123::64-big-signed>>
    end

    test "write_string/2 creates correct operation" do
      op = Operation.write_string("str", "hello")
      assert op.op_type == 2
      assert op.particle_type == 3
      assert op.data == "hello"
    end
  end
end
