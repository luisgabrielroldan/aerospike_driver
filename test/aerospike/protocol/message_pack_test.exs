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

    test "uses the wider string, array, map, and ext headers at their size boundaries" do
      str8 = String.duplicate("a", 40)
      str16 = String.duplicate("b", 300)
      str32 = String.duplicate("c", 70_000)

      assert <<0xD9, 40, _::binary>> = MessagePack.pack!(str8)
      assert <<0xDA, 0x01, 0x2C, _::binary>> = MessagePack.pack!(str16)
      assert <<0xDB, 0x00, 0x01, 0x11, 0x70, _::binary>> = MessagePack.pack!(str32)

      array16 = List.duplicate(1, 16)
      map16 = Map.new(1..16, &{Integer.to_string(&1), &1})
      array32 = List.duplicate(1, 65_536)
      map32 = Map.new(1..65_536, &{Integer.to_string(&1), &1})

      assert <<0xDC, 0x00, 0x10, _::binary>> = MessagePack.pack!(array16)
      assert <<0xDE, 0x00, 0x10, _::binary>> = MessagePack.pack!(map16)
      assert <<0xDD, 0x00, 0x01, 0x00, 0x00, _::binary>> = MessagePack.pack!(array32)
      assert <<0xDF, 0x00, 0x01, 0x00, 0x00, _::binary>> = MessagePack.pack!(map32)

      assert <<0xD4, 7, _::binary>> = MessagePack.pack!({:ext, 7, <<1>>})
      assert <<0xD5, 7, _::binary>> = MessagePack.pack!({:ext, 7, <<1, 2>>})
      assert <<0xD6, 7, _::binary>> = MessagePack.pack!({:ext, 7, <<1, 2, 3, 4>>})
      assert <<0xD7, 7, _::binary>> = MessagePack.pack!({:ext, 7, <<1, 2, 3, 4, 5, 6, 7, 8>>})
      assert <<0xD8, 7, _::binary>> = MessagePack.pack!({:ext, 7, :binary.copy(<<1>>, 16)})
      assert <<0xC7, 20, 7, _::binary>> = MessagePack.pack!({:ext, 7, :binary.copy(<<1>>, 20)})

      assert <<0xC8, 0x01, 0x2C, 7, _::binary>> =
               MessagePack.pack!({:ext, 7, :binary.copy(<<1>>, 300)})

      assert <<0xC9, 0x00, 0x01, 0x11, 0x70, 7, _::binary>> =
               MessagePack.pack!({:ext, 7, :binary.copy(<<1>>, 70_000)})
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

    test "decodes float32 payloads and rejects truncated compound payloads" do
      assert_in_delta MessagePack.unpack!(<<0xCA, 0x3F, 0xC0, 0x00, 0x00>>), 1.5, 1.0e-6
      assert MessagePack.unpack!(<<0xD4, 7, 1>>) == {:ext, 7, <<1>>}
      assert MessagePack.unpack!(<<0xD5, 7, 1, 2>>) == {:ext, 7, <<1, 2>>}
      assert MessagePack.unpack!(<<0xD6, 7, 1, 2, 3, 4>>) == {:ext, 7, <<1, 2, 3, 4>>}

      assert MessagePack.unpack!(<<0xD7, 7, 1, 2, 3, 4, 5, 6, 7, 8>>) ==
               {:ext, 7, <<1, 2, 3, 4, 5, 6, 7, 8>>}

      assert MessagePack.unpack!(<<0xD8, 7, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1>>) ==
               {:ext, 7, :binary.copy(<<1>>, 16)}

      assert MessagePack.unpack!(<<0xC8, 0x00, 0x03, 7, 1, 2, 3>>) == {:ext, 7, <<1, 2, 3>>}

      assert MessagePack.unpack!(<<0xC9, 0x00, 0x00, 0x00, 0x03, 7, 1, 2, 3>>) ==
               {:ext, 7, <<1, 2, 3>>}

      assert {:error, :invalid_msgpack} = MessagePack.unpack(<<0xDC, 0x00, 0x01, 0xD9, 0x02, ?a>>)
      assert {:error, :invalid_msgpack} = MessagePack.unpack(<<0xDE, 0x00, 0x01, 0xA1, ?a>>)
      assert {:error, :invalid_msgpack} = MessagePack.unpack(<<0xC7, 0x04, 0x01, 1, 2>>)
    end

    test "rejects unsupported terms on pack" do
      assert_raise ArgumentError, ~r/unsupported term/, fn ->
        MessagePack.pack!(:atom_not_supported)
      end
    end
  end
end
