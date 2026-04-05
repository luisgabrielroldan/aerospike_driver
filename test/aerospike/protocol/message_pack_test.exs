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

    test "raises on unsupported pack term" do
      assert_raise ArgumentError, fn -> MessagePack.pack!(self()) end
    end

    test "raises on trailing unpack bytes" do
      bin = MessagePack.pack!(1) <> <<0>>
      assert_raise ArgumentError, fn -> MessagePack.unpack!(bin) end
    end
  end

  describe "property roundtrip" do
    @describetag :property

    property "scalars roundtrip" do
      check all(
              n <- one_of([integer(-1_000_000..1_000_000), float(), boolean(), constant(nil)])
            ) do
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
