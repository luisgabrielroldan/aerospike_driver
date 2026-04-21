defmodule Aerospike.Protocol.MessagePackTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.MessagePack

  describe "pack!/1 and unpack!/1 scalars" do
    test "round-trips nil, booleans, integers, and floats across boundary encodings" do
      terms = [
        nil,
        false,
        true,
        0,
        127,
        128,
        65_535,
        65_536,
        -1,
        -32,
        -33,
        -129,
        -32_769,
        1.5
      ]

      for term <- terms do
        assert MessagePack.unpack!(MessagePack.pack!(term)) == term
      end
    end

    test "round-trips strings, arrays, maps, and ext values" do
      assert MessagePack.unpack!(MessagePack.pack!("hello")) == "hello"
      assert MessagePack.unpack!(MessagePack.pack!([1, "two", false])) == [1, "two", false]

      assert MessagePack.unpack!(MessagePack.pack!(%{"a" => 1, "b" => [2]})) == %{
               "a" => 1,
               "b" => [2]
             }

      assert MessagePack.unpack!(MessagePack.pack!({:ext, 9, <<1, 2, 3, 4>>})) ==
               {:ext, 9, <<1, 2, 3, 4>>}
    end

    test "encodes CDT particle wrappers explicitly" do
      assert MessagePack.unpack!(MessagePack.pack!({:particle_string, "abc"})) == "abc"
      assert MessagePack.unpack!(MessagePack.pack!({:bytes, <<1, 2>>})) == <<4, 1, 2>>
    end
  end

  describe "unpack/1 input validation" do
    test "returns raw binaries for bin payload families" do
      assert MessagePack.unpack!(<<0xC4, 0x03, 1, 2, 3>>) == <<1, 2, 3>>
      assert MessagePack.unpack!(<<0xC5, 0x00, 0x03, 1, 2, 3>>) == <<1, 2, 3>>
      assert MessagePack.unpack!(<<0xC6, 0x00, 0x00, 0x00, 0x03, 1, 2, 3>>) == <<1, 2, 3>>
    end

    test "rejects trailing bytes and incomplete input" do
      assert_raise ArgumentError, ~r/trailing bytes/, fn ->
        MessagePack.unpack!(<<0x01, 0x02>>)
      end

      assert_raise ArgumentError, ~r/incomplete or invalid input/, fn ->
        MessagePack.unpack!(<<0xD9, 0x02, ?o>>)
      end

      assert {:error, :invalid_msgpack} = MessagePack.unpack(<<0xD9, 0x02, ?o>>)
    end

    test "rejects unsupported terms on pack" do
      assert_raise ArgumentError, ~r/unsupported term/, fn ->
        MessagePack.pack!(:atom_not_supported)
      end
    end
  end
end
