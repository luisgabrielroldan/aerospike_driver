defmodule Aerospike.Protocol.AsmMsg.ValueTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.MessagePack

  describe "encode_value/1 and decode_value/2 round-trip" do
    test "nil" do
      {pt, data} = Value.encode_value(nil)
      assert Value.decode_value(pt, data) == {:ok, nil}
    end

    test "integers" do
      for n <- [0, -1, 42, 9_223_372_036_854_775_807, -9_223_372_036_854_775_808] do
        {pt, data} = Value.encode_value(n)
        assert Value.decode_value(pt, data) == {:ok, n}
      end
    end

    test "float" do
      {pt, data} = Value.encode_value(3.14)
      assert Value.decode_value(pt, data) == {:ok, 3.14}
    end

    test "string" do
      {pt, data} = Value.encode_value("hello")
      assert Value.decode_value(pt, data) == {:ok, "hello"}
    end

    test "bool" do
      assert Value.decode_value(17, <<1>>) == {:ok, true}
      assert Value.decode_value(17, <<0>>) == {:ok, false}
    end
  end

  test "unknown particle type returns raw tuple" do
    assert Value.decode_value(99, <<1, 2, 3>>) == {:ok, {:raw, 99, <<1, 2, 3>>}}
  end

  test "encode_bin_operations/1" do
    ops = Value.encode_bin_operations(%{name: "Alice", age: 31})
    assert length(ops) == 2
    assert Enum.all?(ops, &(&1.op_type == Operation.op_write()))
    names = Enum.map(ops, & &1.bin_name) |> Enum.sort()
    assert names == ["age", "name"]
  end

  test "encode_bin_operations rejects unsupported nested values" do
    assert_raise ArgumentError, fn ->
      Value.encode_bin_operations(%{"x" => %{self() => 1}})
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
      assert {:ok, {:blob, "binary data"}} = Value.decode_value(4, "binary data")
    end

    test "map particle round-trip uses particle-wrapped string keys in MessagePack" do
      {pt, data} = Value.encode_value(%{"a" => 1})
      assert pt == Operation.particle_map()
      assert Value.decode_value(pt, data) == {:ok, %{"a" => 1}}
    end

    test "list particle round-trip" do
      {pt, data} = Value.encode_value([1, 2, 3])
      assert pt == Operation.particle_list()
      assert Value.decode_value(pt, data) == {:ok, [1, 2, 3]}
    end

    test "nested map bin round-trip" do
      nested = %{"profile" => %{"geo" => %{"lat" => 0.0}}}
      {pt, data} = Value.encode_value(nested)
      assert pt == Operation.particle_map()
      assert Value.decode_value(pt, data) == {:ok, nested}
    end

    test "geojson particle decodes as tagged tuple" do
      assert Value.decode_value(Operation.particle_geojson(), "{}") == {:ok, {:geojson, "{}"}}
    end

    test "map particle with trailing bytes errors" do
      {pt, data} = Value.encode_value(%{"a" => 1})
      assert {:error, %{code: :parse_error}} = Value.decode_value(pt, data <> <<0>>)
    end

    test "list particle with trailing bytes errors" do
      {pt, data} = Value.encode_value([1, 2])
      assert {:error, %{code: :parse_error}} = Value.decode_value(pt, data <> <<0>>)
    end

    test "map particle invalid msgpack errors" do
      assert {:error, %{code: :parse_error}} =
               Value.decode_value(Operation.particle_map(), <<0xC1>>)
    end

    test "list particle invalid msgpack errors" do
      assert {:error, %{code: :parse_error}} =
               Value.decode_value(Operation.particle_list(), <<0xC1>>)
    end
  end

  describe "encode_value compound types" do
    test "blob encodes as particle type 4 with raw bytes" do
      assert {4, <<1, 2, 3>>} = Value.encode_value({:bytes, <<1, 2, 3>>})
    end

    test "list with strings round-trips through particle encoding" do
      {pt, data} = Value.encode_value(["hello", "world"])
      assert pt == Operation.particle_list()
      assert Value.decode_value(pt, data) == {:ok, ["hello", "world"]}
    end

    test "list with mixed types round-trips" do
      {pt, data} = Value.encode_value([1, "str", true, nil, 3.14])
      assert pt == Operation.particle_list()
      assert {:ok, [1, "str", true, nil, f]} = Value.decode_value(pt, data)
      assert_in_delta f, 3.14, 0.001
    end

    test "list with nested lists round-trips" do
      {pt, data} = Value.encode_value([[1, 2], [3, 4]])
      assert pt == Operation.particle_list()
      assert Value.decode_value(pt, data) == {:ok, [[1, 2], [3, 4]]}
    end

    test "list with blob values round-trips" do
      {pt, data} = Value.encode_value([{:bytes, <<0xFF>>}])
      assert pt == Operation.particle_list()
      assert Value.decode_value(pt, data) == {:ok, [{:blob, <<0xFF>>}]}
    end

    test "list with booleans and nil round-trips" do
      {pt, data} = Value.encode_value([true, false, nil])
      assert pt == Operation.particle_list()
      assert Value.decode_value(pt, data) == {:ok, [true, false, nil]}
    end

    test "map with string values round-trips" do
      map = %{"key" => "val"}
      {pt, data} = Value.encode_value(map)
      assert pt == Operation.particle_map()
      assert Value.decode_value(pt, data) == {:ok, map}
    end

    test "map with nested map round-trips" do
      map = %{"outer" => %{"inner" => 42}}
      {pt, data} = Value.encode_value(map)
      assert pt == Operation.particle_map()
      assert Value.decode_value(pt, data) == {:ok, map}
    end

    test "map with nested list round-trips" do
      map = %{"tags" => [1, 2, 3]}
      {pt, data} = Value.encode_value(map)
      assert pt == Operation.particle_map()
      assert Value.decode_value(pt, data) == {:ok, map}
    end

    test "encode_value rejects unsupported term" do
      assert_raise ArgumentError, ~r/unsupported bin value/, fn ->
        Value.encode_value({:tuple, 1, 2})
      end
    end

    test "encode_value rejects unsupported CDT element in list" do
      assert_raise ArgumentError, ~r/unsupported CDT/, fn ->
        Value.encode_value([self()])
      end
    end

    test "encode_bin_operations with atom keys converts to strings" do
      ops = Value.encode_bin_operations(%{name: "Alice"})
      assert [%Operation{bin_name: "name"}] = ops
    end

    test "encode_bin_operations add rejects non-integer" do
      assert_raise ArgumentError, ~r/add requires integer/, fn ->
        Value.encode_bin_operations(%{"x" => "nope"}, Operation.op_add())
      end
    end

    test "encode_bin_operations append rejects non-binary" do
      assert_raise ArgumentError, ~r/append requires string/, fn ->
        Value.encode_bin_operations(%{"x" => 1}, Operation.op_append())
      end
    end

    test "encode_bin_operations prepend rejects non-binary" do
      assert_raise ArgumentError, ~r/prepend requires string/, fn ->
        Value.encode_bin_operations(%{"x" => 1}, Operation.op_prepend())
      end
    end

    test "map with atom keys round-trips (atom keys become strings)" do
      {pt, data} = Value.encode_value(%{name: "Alice"})
      assert pt == Operation.particle_map()
      assert Value.decode_value(pt, data) == {:ok, %{"name" => "Alice"}}
    end

    test "map particle with ext values preserves ext tuples" do
      ext = {:ext, 0xFF, <<0, 0, 0, 0>>}
      map_data = MessagePack.pack!(%{{:particle_string, "x"} => ext})
      assert {:ok, decoded} = Value.decode_value(Operation.particle_map(), map_data)
      assert %{"x" => {:ext, 0xFF, _}} = decoded
    end

    test "string without particle prefix passes through in map decode" do
      raw_str = MessagePack.pack!(%{"plain" => 1})
      assert {:ok, decoded} = Value.decode_value(Operation.particle_map(), raw_str)
      assert %{"plain" => 1} = decoded
    end
  end

  describe "property" do
    @tag :property
    property "integer round-trip" do
      check all(n <- integer(-9_223_372_036_854_775_808..9_223_372_036_854_775_807)) do
        {pt, data} = Value.encode_value(n)
        assert Value.decode_value(pt, data) == {:ok, n}
      end
    end
  end
end
