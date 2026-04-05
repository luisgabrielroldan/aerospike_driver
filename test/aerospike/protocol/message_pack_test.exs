defmodule Aerospike.Protocol.MessagePackTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Aerospike.Protocol.MessagePack

  describe "pack!/1 and unpack!/1" do
    test "nil" do
      assert MessagePack.unpack!(MessagePack.pack!(nil)) == nil
    end

    test "booleans" do
      assert MessagePack.unpack!(MessagePack.pack!(false)) == false
      assert MessagePack.unpack!(MessagePack.pack!(true)) == true
    end

    test "integers edge cases" do
      for n <- [0, 1, 127, 128, 255, 256, 65_535, 65_536, 4_294_967_295, 4_294_967_296] do
        assert MessagePack.unpack!(MessagePack.pack!(n)) == n
      end

      for n <- [-1, -32, -33, -128, -129, -32_768, -32_769, -2_147_483_648, -2_147_483_649] do
        assert MessagePack.unpack!(MessagePack.pack!(n)) == n
      end
    end

    test "float" do
      assert MessagePack.unpack!(MessagePack.pack!(1.5)) == 1.5
    end

    test "strings including unicode" do
      assert MessagePack.unpack!(MessagePack.pack!("")) == ""
      assert MessagePack.unpack!(MessagePack.pack!("hello")) == "hello"
      assert MessagePack.unpack!(MessagePack.pack!("日本語")) == "日本語"
    end

    test "lists" do
      assert MessagePack.unpack!(MessagePack.pack!([])) == []
      assert MessagePack.unpack!(MessagePack.pack!([1, 2, 3])) == [1, 2, 3]
      assert MessagePack.unpack!(MessagePack.pack!([[1], [2, 3]])) == [[1], [2, 3]]
    end

    test "maps" do
      m = %{"a" => 1, "b" => 2}
      assert MessagePack.unpack!(MessagePack.pack!(m)) == m
    end

    test "ext type roundtrip" do
      ext = {:ext, 42, <<1, 2, 3, 4>>}
      assert MessagePack.unpack!(MessagePack.pack!(ext)) == ext
    end

    test "particle_string tuple encodes STRING particle inside msgpack str" do
      bin = MessagePack.pack!({:particle_string, "hi"})
      # fixstr length 3: 0x03 + ?h ?i
      assert bin == <<0xA3, 3, ?h, ?i>>
      assert MessagePack.unpack!(bin) == <<3, "hi">>
    end

    test "{:bytes, _} encodes BLOB particle inside msgpack str (CDT / Go packBytes)" do
      bin = MessagePack.pack!({:bytes, <<0xFF, 0x00>>})
      assert bin == <<0xA3, 4, 0xFF, 0x00>>
      assert MessagePack.unpack!(bin) == {:blob, <<0xFF, 0x00>>}
    end

    test "raises on unsupported pack term" do
      assert_raise ArgumentError, fn -> MessagePack.pack!(self()) end
    end

    test "raises on trailing unpack bytes" do
      bin = MessagePack.pack!(1) <> <<0>>
      assert_raise ArgumentError, fn -> MessagePack.unpack!(bin) end
    end
  end

  describe "wire format encoding" do
    test "positive fixint (0..127)" do
      assert MessagePack.pack!(0) == <<0>>
      assert MessagePack.pack!(127) == <<127>>
    end

    test "uint8 (128..255)" do
      assert MessagePack.pack!(128) == <<0xCC, 128>>
      assert MessagePack.pack!(255) == <<0xCC, 255>>
    end

    test "uint16 (256..65535)" do
      assert MessagePack.pack!(256) == <<0xCD, 1, 0>>
      assert MessagePack.pack!(65_535) == <<0xCD, 0xFF, 0xFF>>
    end

    test "uint32 (65536..4294967295)" do
      assert MessagePack.pack!(65_536) == <<0xCE, 0, 1, 0, 0>>
    end

    test "uint64 (> 4294967295)" do
      bin = MessagePack.pack!(4_294_967_296)
      assert <<0xCF, _::binary-size(8)>> = bin
    end

    test "negative fixint (-32..-1)" do
      assert MessagePack.pack!(-1) == <<0xFF>>
      assert MessagePack.pack!(-32) == <<0xE0>>
    end

    test "int8 (-128..-33)" do
      assert <<0xD0, _::8>> = MessagePack.pack!(-33)
      assert <<0xD0, _::8>> = MessagePack.pack!(-128)
    end

    test "int16 (-32768..-129)" do
      assert <<0xD1, _::16>> = MessagePack.pack!(-129)
    end

    test "int32 (-2147483648..-32769)" do
      assert <<0xD2, _::32>> = MessagePack.pack!(-32_769)
    end

    test "int64 (< -2147483648)" do
      assert <<0xD3, _::64>> = MessagePack.pack!(-2_147_483_649)
    end

    test "float64" do
      assert <<0xCB, _::64>> = MessagePack.pack!(3.14)
    end

    test "fixstr (len <= 31)" do
      assert <<0xA5, "hello">> = MessagePack.pack!("hello")
    end

    test "str8 (len 32..255)" do
      s = String.duplicate("x", 40)
      assert <<0xD9, 40, _::binary>> = MessagePack.pack!(s)
    end

    test "str16 (len 256..65535)" do
      s = String.duplicate("x", 300)
      assert <<0xDA, _::16, _::binary>> = MessagePack.pack!(s)
    end

    test "str32 (len > 65535)" do
      s = String.duplicate("x", 70_000)
      assert <<0xDB, _::32, _::binary>> = MessagePack.pack!(s)
    end
  end

  describe "unpack wire formats" do
    test "float32" do
      bin = <<0xCA, 1.5::32-float-big>>
      assert_in_delta MessagePack.unpack!(bin), 1.5, 0.001
    end

    test "bin8" do
      data = <<1, 2, 3>>
      bin = <<0xC4, 3>> <> data
      assert MessagePack.unpack!(bin) == data
    end

    test "bin16" do
      data = :binary.copy(<<0xAA>>, 300)
      bin = <<0xC5, 300::16-big>> <> data
      assert MessagePack.unpack!(bin) == data
    end

    test "bin32" do
      data = :binary.copy(<<0xBB>>, 70_000)
      bin = <<0xC6, 70_000::32-big>> <> data
      assert MessagePack.unpack!(bin) == data
    end

    test "array16" do
      elements = for i <- 0..20, do: MessagePack.pack!(i)
      bin = <<0xDC, 21::16-big>> <> IO.iodata_to_binary(elements)
      result = MessagePack.unpack!(bin)
      assert result == Enum.to_list(0..20)
    end

    test "array32" do
      bin = <<0xDD, 2::32-big>> <> MessagePack.pack!(1) <> MessagePack.pack!(2)
      assert MessagePack.unpack!(bin) == [1, 2]
    end

    test "map16" do
      kv_data =
        for i <- 0..20 do
          MessagePack.pack!("k#{i}") <> MessagePack.pack!(i)
        end

      bin = <<0xDE, 21::16-big>> <> IO.iodata_to_binary(kv_data)
      result = MessagePack.unpack!(bin)
      assert map_size(result) == 21
    end

    test "map32" do
      bin =
        <<0xDF, 1::32-big>> <>
          MessagePack.pack!("key") <>
          MessagePack.pack!("val")

      assert MessagePack.unpack!(bin) == %{"key" => "val"}
    end

    test "fixext 1 byte" do
      bin = <<0xD4, 7, 0xAA>>
      assert MessagePack.unpack!(bin) == {:ext, 7, <<0xAA>>}
    end

    test "fixext 2 bytes" do
      bin = <<0xD5, 7, 0xAA, 0xBB>>
      assert MessagePack.unpack!(bin) == {:ext, 7, <<0xAA, 0xBB>>}
    end

    test "fixext 4 bytes" do
      data = <<1, 2, 3, 4>>
      bin = <<0xD6, 7>> <> data
      assert MessagePack.unpack!(bin) == {:ext, 7, data}
    end

    test "fixext 8 bytes" do
      data = :binary.copy(<<0xAB>>, 8)
      bin = <<0xD7, 7>> <> data
      assert MessagePack.unpack!(bin) == {:ext, 7, data}
    end

    test "fixext 16 bytes" do
      data = :binary.copy(<<0xCD>>, 16)
      bin = <<0xD8, 7>> <> data
      assert MessagePack.unpack!(bin) == {:ext, 7, data}
    end

    test "ext8" do
      data = :binary.copy(<<0xEE>>, 50)
      bin = <<0xC7, 50, 7>> <> data
      assert MessagePack.unpack!(bin) == {:ext, 7, data}
    end

    test "ext16" do
      data = :binary.copy(<<0xEE>>, 300)
      bin = <<0xC8, 300::16-big, 7>> <> data
      assert MessagePack.unpack!(bin) == {:ext, 7, data}
    end

    test "ext32" do
      data = :binary.copy(<<0xEE>>, 70_000)
      bin = <<0xC9, 70_000::32-big, 7>> <> data
      assert MessagePack.unpack!(bin) == {:ext, 7, data}
    end

    test "str8 unpack" do
      s = String.duplicate("y", 40)
      bin = <<0xD9, 40>> <> s
      assert MessagePack.unpack!(bin) == s
    end

    test "str16 unpack" do
      s = String.duplicate("y", 300)
      bin = <<0xDA, 300::16-big>> <> s
      assert MessagePack.unpack!(bin) == s
    end

    test "str32 unpack" do
      s = String.duplicate("y", 70_000)
      bin = <<0xDB, 70_000::32-big>> <> s
      assert MessagePack.unpack!(bin) == s
    end

    test "raises on invalid input" do
      assert_raise ArgumentError, fn -> MessagePack.unpack!(<<>>) end
    end

    test "ext32 unpack with truncated payload errors" do
      assert {:error, :invalid_msgpack} = MessagePack.unpack(<<0xC9, 100::32-big, 7>>)
    end

    test "list unpack errors when an element is invalid" do
      assert {:error, :invalid_msgpack} = MessagePack.unpack(<<0x91, 0xC1>>)
    end

    test "map unpack errors when key decode fails" do
      assert {:error, :invalid_msgpack} = MessagePack.unpack(<<0x81, 0xC1, 0x01>>)
    end

    test "map unpack errors when value decode fails" do
      assert {:error, :invalid_msgpack} = MessagePack.unpack(<<0x81, 0x01, 0xC1>>)
    end
  end

  describe "ext packing" do
    test "ext with 1-byte data uses fixext1" do
      assert <<0xD4, 1, 0xAB>> = MessagePack.pack!({:ext, 1, <<0xAB>>})
    end

    test "ext with 2-byte data uses fixext2" do
      assert <<0xD5, 1, _::binary-size(2)>> = MessagePack.pack!({:ext, 1, <<1, 2>>})
    end

    test "ext with 4-byte data uses fixext4" do
      assert <<0xD6, 1, _::binary-size(4)>> = MessagePack.pack!({:ext, 1, <<1, 2, 3, 4>>})
    end

    test "ext with 8-byte data uses fixext8" do
      data = :binary.copy(<<0>>, 8)
      assert <<0xD7, 1, _::binary-size(8)>> = MessagePack.pack!({:ext, 1, data})
    end

    test "ext with 16-byte data uses fixext16" do
      data = :binary.copy(<<0>>, 16)
      assert <<0xD8, 1, _::binary-size(16)>> = MessagePack.pack!({:ext, 1, data})
    end

    test "ext with 50-byte data uses ext8" do
      data = :binary.copy(<<0>>, 50)
      assert <<0xC7, 50, 1, _::binary>> = MessagePack.pack!({:ext, 1, data})
    end

    test "ext with 300-byte data uses ext16" do
      data = :binary.copy(<<0>>, 300)
      assert <<0xC8, _::16, 1, _::binary>> = MessagePack.pack!({:ext, 1, data})
    end

    test "ext with 70000-byte data uses ext32" do
      data = :binary.copy(<<0>>, 70_000)
      assert <<0xC9, _::32, 1, _::binary>> = MessagePack.pack!({:ext, 1, data})
    end
  end

  describe "large collection headers" do
    test "array with 16+ elements uses fixarray up to 15" do
      list = Enum.to_list(1..15)
      bin = MessagePack.pack!(list)
      assert <<0x9F, _::binary>> = bin
    end

    test "array with 16 elements uses array16" do
      list = Enum.to_list(1..16)
      bin = MessagePack.pack!(list)
      assert <<0xDC, _::binary>> = bin
    end

    test "map with 16+ entries uses map16" do
      map = Map.new(1..16, fn i -> {"k#{i}", i} end)
      bin = MessagePack.pack!(map)
      assert <<0xDE, _::binary>> = bin
    end

    test "array32 header when length exceeds uint16" do
      list = List.duplicate(0, 65_536)
      bin = MessagePack.pack!(list)
      assert <<0xDD, 65_536::32-big, _::binary>> = bin
    end

    test "map32 header when entry count exceeds uint16" do
      map = Map.new(1..65_536, fn i -> {i, i} end)
      bin = MessagePack.pack!(map)
      assert <<0xDF, 65_536::32-big, _::binary>> = bin
    end
  end

  describe "property roundtrip" do
    @describetag :property

    property "scalars roundtrip" do
      check all(n <- one_of([integer(-1_000_000..1_000_000), float(), boolean(), constant(nil)])) do
        assert MessagePack.unpack!(MessagePack.pack!(n)) == n
      end
    end

    property "nested lists of small integers roundtrip" do
      check all(
              depth <- integer(0..3),
              list <- list_of(integer(-100..100), max_length: 8)
            ) do
        term = nest_list(list, depth)
        assert MessagePack.unpack!(MessagePack.pack!(term)) == term
      end
    end
  end

  defp nest_list(list, 0), do: list
  defp nest_list(list, d) when d > 0, do: [nest_list(list, d - 1)]
end
