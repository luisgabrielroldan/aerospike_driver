defmodule Aerospike.Protocol.MessagePackPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Aerospike.Protocol.MessagePack

  @moduletag :property

  # Boundary integers that probe each encoding format transition.
  # Sorted by format: positive fixint → uint8 → uint16 → uint32 → uint64,
  # and negative fixint → int8 → int16 → int32 → int64.
  @boundary_integers [
    # positive fixint (0x00..0x7F)
    0,
    1,
    127,
    # uint8 (0xCC): 128..255
    128,
    255,
    # uint16 (0xCD): 256..65535
    256,
    65_535,
    # uint32 (0xCE): 65536..4294967295
    65_536,
    4_294_967_295,
    # uint64 (0xCF): 4294967296..18446744073709551615
    4_294_967_296,
    9_223_372_036_854_775_807,
    # negative fixint (0xE0..0xFF): -32..-1
    -1,
    -32,
    # int8 (0xD0): -128..-33
    -33,
    -128,
    # int16 (0xD1): -32768..-129
    -129,
    -32_768,
    # int32 (0xD2): -2147483648..-32769
    -32_769,
    -2_147_483_648,
    # int64 (0xD3): -9223372036854775808..-2147483649
    -2_147_483_649,
    -9_223_372_036_854_775_808
  ]

  # Leaf values that encode and decode symmetrically.
  #
  # Raw binaries are intentionally excluded: the module encodes them using
  # MessagePack str format for Aerospike CDT payload compatibility. A binary
  # whose first byte is the particle-string type (3) or particle-blob type (4)
  # decodes back as a tagged tuple, not the original binary. Using
  # :alphanumeric strings (bytes >= 48) avoids this asymmetry entirely.
  #
  # Integer range is bounded to int64/uint64 to avoid silent truncation for
  # values beyond the 64-bit range (a known limitation documented below).
  defp clean_leaf do
    one_of([
      integer(-9_223_372_036_854_775_808..9_223_372_036_854_775_807),
      float(),
      boolean(),
      constant(nil),
      string(:alphanumeric)
    ])
  end

  # Simple map keys: only hashable leaf types, no lists or nested maps.
  defp map_key do
    one_of([integer(-1_000..1_000), string(:alphanumeric)])
  end

  defp msgpack_value(0), do: clean_leaf()

  defp msgpack_value(depth) when depth > 0 do
    one_of([
      clean_leaf(),
      list_of(msgpack_value(depth - 1), max_length: 5),
      map_of(map_key(), msgpack_value(depth - 1), max_length: 5)
    ])
  end

  # -------------------------------------------------------------------------
  # Property tests
  # -------------------------------------------------------------------------

  describe "property: pack/unpack roundtrip" do
    property "pack!/1 |> unpack!/1 roundtrip for arbitrary nested values" do
      check all(value <- msgpack_value(3)) do
        assert MessagePack.unpack!(MessagePack.pack!(value)) == value
      end
    end

    property "boundary integers roundtrip exactly at each format threshold" do
      check all(n <- member_of(@boundary_integers)) do
        assert MessagePack.unpack!(MessagePack.pack!(n)) == n
      end
    end

    property "pack!/1 always returns a binary for supported types" do
      check all(value <- msgpack_value(3)) do
        assert is_binary(MessagePack.pack!(value))
      end
    end
  end

  describe "property: truncated input safety" do
    property "unpack/1 returns a result tuple and never raises on truncated binary" do
      check all(
              value <-
                one_of([
                  list_of(integer(-100..100), min_length: 2, max_length: 8),
                  map_of(
                    string(:alphanumeric, min_length: 1, max_length: 4),
                    integer(-100..100),
                    min_length: 2,
                    max_length: 5
                  ),
                  string(:alphanumeric, min_length: 5, max_length: 50)
                ]),
              packed = MessagePack.pack!(value),
              cut_at <- integer(0..(byte_size(packed) - 1))
            ) do
        truncated = binary_part(packed, 0, cut_at)
        result = MessagePack.unpack(truncated)
        # Must return a tagged tuple — never raise
        assert match?({:ok, _}, result) or match?({:error, :invalid_msgpack}, result)
      end
    end
  end

  # -------------------------------------------------------------------------
  # Explicit edge-case tests
  # -------------------------------------------------------------------------

  describe "edge cases: empty collections" do
    test "empty map %{} encodes as fixmap 0x80 and decodes" do
      # fixmap with 0 entries: 0x80 | 0 = 0x80
      assert MessagePack.pack!(%{}) == <<0x80>>
      assert MessagePack.unpack!(<<0x80>>) == %{}
    end

    test "empty list [] encodes as fixarray 0x90 and decodes" do
      # fixarray with 0 entries: 0x90 | 0 = 0x90
      assert MessagePack.pack!([]) == <<0x90>>
      assert MessagePack.unpack!(<<0x90>>) == []
    end

    test "empty string \"\" encodes as fixstr 0xA0 and decodes" do
      # fixstr with 0 bytes: 0xA0 | 0 = 0xA0
      assert MessagePack.pack!("") == <<0xA0>>
      assert MessagePack.unpack!(<<0xA0>>) == ""
    end

    test "empty binary <<>> encodes identically to empty string (same Elixir term)" do
      # In Elixir, <<>> and "" are the same term — both encode to fixstr 0xA0
      assert MessagePack.pack!(<<>>) == <<0xA0>>
    end
  end

  describe "edge cases: nested mixed structures" do
    test "list containing map, nil, and boolean roundtrips" do
      value = [%{"a" => [1, 2, 3]}, nil, true]
      assert MessagePack.unpack!(MessagePack.pack!(value)) == value
    end

    test "map with nested list of mixed types roundtrips" do
      value = %{"outer" => [%{"inner" => [1, nil, false]}, 42]}
      assert MessagePack.unpack!(MessagePack.pack!(value)) == value
    end
  end

  describe "edge cases: collection header format boundaries" do
    test "fixmap max: 15 entries use single-byte header 0x8F" do
      # fixmap range: 0x80 | n for n in 0..15 → 0x80..0x8F
      map = Map.new(1..15, fn i -> {i, i} end)
      assert <<0x8F, _::binary>> = MessagePack.pack!(map)
      assert MessagePack.unpack!(MessagePack.pack!(map)) == map
    end

    test "map16: 16 entries upgrade to 3-byte 0xDE header" do
      # map16: 0xDE followed by uint16 count
      map = Map.new(1..16, fn i -> {i, i} end)
      assert <<0xDE, 16::16-big, _::binary>> = MessagePack.pack!(map)
      assert MessagePack.unpack!(MessagePack.pack!(map)) == map
    end

    test "fixarray max: 15 elements use single-byte header 0x9F" do
      # fixarray range: 0x90 | n for n in 0..15 → 0x90..0x9F
      list = Enum.to_list(1..15)
      assert <<0x9F, _::binary>> = MessagePack.pack!(list)
      assert MessagePack.unpack!(MessagePack.pack!(list)) == list
    end

    test "array16: 16 elements upgrade to 3-byte 0xDC header" do
      # array16: 0xDC followed by uint16 count
      list = Enum.to_list(1..16)
      assert <<0xDC, 16::16-big, _::binary>> = MessagePack.pack!(list)
      assert MessagePack.unpack!(MessagePack.pack!(list)) == list
    end
  end

  describe "edge cases: float and integer extremes" do
    test "float 0.0 encodes as float64 with all-zero payload" do
      # IEEE 754 positive zero: sign=0, exponent=0, mantissa=0 → all 64 bits zero
      packed = MessagePack.pack!(0.0)
      assert <<0xCB, 0::64>> = packed
      assert MessagePack.unpack!(packed) == 0.0
    end

    test "float -0.0 encodes with sign bit set, distinct wire bytes from 0.0" do
      # IEEE 754 negative zero: sign=1, exponent=0, mantissa=0 → 0x8000000000000000
      # Elixir equality treats -0.0 == 0.0 in comparisons
      neg_zero_bytes = <<0xCB, 1::1, 0::63>>
      assert MessagePack.pack!(-0.0) == neg_zero_bytes
      assert MessagePack.unpack!(neg_zero_bytes) == 0.0
    end

    test "int64 min (-9223372036854775808) encodes with 0xD3 format" do
      n = -9_223_372_036_854_775_808
      # int64 header: 0xD3
      assert <<0xD3, _::64-signed>> = MessagePack.pack!(n)
      assert MessagePack.unpack!(MessagePack.pack!(n)) == n
    end

    test "int64 max-adjacent positive (9223372036854775807) encodes with 0xCF format" do
      n = 9_223_372_036_854_775_807
      # uint64 header: 0xCF
      assert <<0xCF, _::64>> = MessagePack.pack!(n)
      assert MessagePack.unpack!(MessagePack.pack!(n)) == n
    end

    # The module uses <<n::64-big>> for the uint64 format and <<n::64-signed-big>>
    # for int64. Elixir silently takes the lower 64 bits for integers outside those
    # ranges, producing wrong values instead of raising. This is a known limitation.
    @tag :known_bug
    test "integer beyond uint64 max silently truncates (known limitation)" do
      # 2^64 overflows to 0 in a 64-bit binary — round-trip fails
      n = 18_446_744_073_709_551_616
      decoded = MessagePack.unpack!(MessagePack.pack!(n))

      refute decoded == n,
             "integers beyond uint64 max silently truncate; pack!/unpack! is not correct for n >= 2^64"
    end
  end
end
