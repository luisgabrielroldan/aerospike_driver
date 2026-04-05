defmodule Aerospike.Protocol.AsmMsg.ValueTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value

  describe "encode_value/1 and decode_value/2 round-trip" do
    test "nil" do
      {pt, data} = Value.encode_value(nil)
      assert Value.decode_value(pt, data) == nil
    end

    test "integers" do
      for n <- [0, -1, 42, 9_223_372_036_854_775_807, -9_223_372_036_854_775_808] do
        {pt, data} = Value.encode_value(n)
        assert Value.decode_value(pt, data) == n
      end
    end

    test "float" do
      {pt, data} = Value.encode_value(3.14)
      assert Value.decode_value(pt, data) == 3.14
    end

    test "string" do
      {pt, data} = Value.encode_value("hello")
      assert Value.decode_value(pt, data) == "hello"
    end

    test "bool" do
      assert Value.decode_value(17, <<1>>) == true
      assert Value.decode_value(17, <<0>>) == false
    end
  end

  test "unknown particle type returns raw tuple" do
    assert Value.decode_value(99, <<1, 2, 3>>) == {:raw, 99, <<1, 2, 3>>}
  end

  test "encode_bin_operations/1" do
    ops = Value.encode_bin_operations(%{name: "Alice", age: 31})
    assert length(ops) == 2
    assert Enum.all?(ops, &(&1.op_type == Operation.op_write()))
    names = Enum.map(ops, & &1.bin_name) |> Enum.sort()
    assert names == ["age", "name"]
  end

  test "encode_bin_operations rejects unsupported values" do
    assert_raise ArgumentError, fn ->
      Value.encode_bin_operations(%{"x" => %{}})
    end
  end

  test "encode_bin_operations rejects invalid bin key types" do
    assert_raise ArgumentError, ~r/bin name must be a string or atom/, fn ->
      Value.encode_bin_operations(%{123 => "value"})
    end
  end

  test "encode_value rejects out of range integer" do
    assert_raise ArgumentError, fn ->
      Value.encode_value(9_223_372_036_854_775_808)
    end
  end

  describe "wire representation" do
    test "nil encodes as particle type 0 with empty binary" do
      assert {0, <<>>} = Value.encode_value(nil)
    end

    test "integer 0 encodes as particle type 1 with 8-byte big-endian zero" do
      assert {1, <<0, 0, 0, 0, 0, 0, 0, 0>>} = Value.encode_value(0)
    end

    test "integer 42 encodes as particle type 1 with 8-byte big-endian" do
      assert {1, <<0, 0, 0, 0, 0, 0, 0, 42>>} = Value.encode_value(42)
    end

    test "integer -1 encodes as particle type 1 with two's complement" do
      assert {1, <<255, 255, 255, 255, 255, 255, 255, 255>>} = Value.encode_value(-1)
    end

    test "float 3.14 encodes as particle type 2 with 8-byte IEEE 754" do
      {pt, data} = Value.encode_value(3.14)
      assert pt == 2
      assert byte_size(data) == 8
      # Verify it decodes back correctly
      <<decoded::64-float-big>> = data
      assert_in_delta decoded, 3.14, 0.0001
    end

    test "string encodes as particle type 3 with raw UTF-8 bytes" do
      assert {3, "hello"} = Value.encode_value("hello")
      assert {3, "café"} = Value.encode_value("café")
    end

    test "bool true encodes as particle type 17 with <<1>>" do
      assert {17, <<1>>} = Value.encode_value(true)
    end

    test "bool false encodes as particle type 17 with <<0>>" do
      assert {17, <<0>>} = Value.encode_value(false)
    end

    test "decoding blob returns tagged tuple" do
      assert {:blob, "binary data"} = Value.decode_value(4, "binary data")
    end
  end

  describe "property" do
    @tag :property
    property "integer round-trip" do
      check all(n <- integer(-9_223_372_036_854_775_808..9_223_372_036_854_775_807)) do
        {pt, data} = Value.encode_value(n)
        assert Value.decode_value(pt, data) == n
      end
    end
  end
end
