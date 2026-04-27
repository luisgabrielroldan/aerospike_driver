defmodule Aerospike.Protocol.OperationTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Protocol.AsmMsg.Operation

  describe "write/2" do
    test "encodes supported simple values" do
      assert {:ok,
              %Operation{
                op_type: 2,
                particle_type: 1,
                bin_name: "count",
                data: <<7::64-signed-big>>
              }} =
               Operation.write("count", 7)

      assert {:ok, %Operation{op_type: 2, particle_type: 17, bin_name: "enabled", data: <<1>>}} =
               Operation.write("enabled", true)

      assert {:ok, %Operation{op_type: 2, particle_type: 4, bin_name: "blob", data: <<1, 2, 3>>}} =
               Operation.write("blob", {:blob, <<1, 2, 3>>})
    end

    test "rejects unsupported values deterministically" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Operation.write("config", MapSet.new([:nested]))

      assert message =~ "unsupported write particle"
    end

    test "rejects invalid bin names deterministically" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Operation.write(:count, 7)

      assert message =~ "non-empty binary"
    end
  end

  describe "record-level helpers" do
    test "touch/0 builds a touch op" do
      assert %Operation{op_type: 11, bin_name: "", particle_type: 0, data: <<>>} =
               Operation.touch()
    end

    test "delete/0 builds an operate delete op" do
      assert %Operation{op_type: 14, bin_name: "", particle_type: 0, data: <<>>} =
               Operation.delete()
    end
  end

  describe "from_simple/1" do
    test "builds the supported simple operate subset" do
      assert {:ok, %Operation{op_type: 1, bin_name: "count"}} =
               Operation.from_simple({:read, "count"})

      assert {:ok, %Operation{op_type: 5, bin_name: "count"}} =
               Operation.from_simple({:add, "count", 1})

      assert {:ok, %Operation{op_type: 9, bin_name: "name"}} =
               Operation.from_simple({:append, "name", "x"})

      assert {:ok, %Operation{op_type: 10, bin_name: "name"}} =
               Operation.from_simple({:prepend, "name", "x"})

      assert {:ok, %Operation{op_type: 11}} = Operation.from_simple(:touch)
      assert {:ok, %Operation{op_type: 14}} = Operation.from_simple(:delete)
    end

    test "rejects unsupported operate shapes deterministically" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Operation.from_simple({:explode, "count", "x"})

      assert message =~ "unsupported simple operation"
    end
  end
end
