defmodule Aerospike.Protocol.FilterTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Filter
  alias Aerospike.Protocol.Filter, as: FilterCodec
  alias Aerospike.Protocol.MessagePack

  test "range filter (integer) layout" do
    filter = Filter.range("age", 10, 20)
    bin = FilterCodec.encode(filter)

    assert byte_size(bin) == 1 + 1 + 3 + 1 + 4 + 8 + 4 + 8

    <<
      1::8,
      3::8,
      "age"::binary,
      1::8,
      begin_size::32-big,
      begin_val::binary-size(begin_size),
      end_size::32-big,
      end_val::binary-size(end_size)
    >> = bin

    assert begin_size == 8
    assert end_size == 8
    assert begin_val == <<10::64-signed-big>>
    assert end_val == <<20::64-signed-big>>
  end

  test "equal filter (string) uses identical begin/end" do
    filter = Filter.equal("city", "PDX")
    bin = FilterCodec.encode(filter)

    <<
      1::8,
      4::8,
      "city"::binary,
      3::8,
      begin_size::32-big,
      begin_val::binary-size(begin_size),
      end_size::32-big,
      end_val::binary-size(end_size)
    >> = bin

    assert begin_size == 3
    assert end_size == 3
    assert begin_val == "PDX"
    assert end_val == "PDX"
  end

  test "named indexes omit the bin name" do
    filter = Filter.equal("age", 21) |> Filter.using_index("age_idx")
    bin = FilterCodec.encode(filter)

    <<
      1::8,
      0::8,
      1::8,
      begin_size::32-big,
      begin_val::binary-size(begin_size),
      end_size::32-big,
      end_val::binary-size(end_size)
    >> = bin

    assert begin_size == 8
    assert end_size == 8
    assert begin_val == <<21::64-signed-big>>
    assert end_val == <<21::64-signed-big>>
  end

  test "geo filters use GeoJSON particle type" do
    region = ~s({"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]})
    bin = "loc" |> Filter.geo_within(region) |> FilterCodec.encode()

    <<
      1::8,
      3::8,
      "loc"::binary,
      23::8,
      begin_size::32-big,
      begin_val::binary-size(begin_size),
      end_size::32-big,
      end_val::binary-size(end_size)
    >> = bin

    assert begin_size == byte_size(region)
    assert end_size == byte_size(region)
    assert begin_val == region
    assert end_val == region
  end

  test "named geo indexes omit the bin name and keep GeoJSON particle type" do
    point = ~s({"type":"Point","coordinates":[0,0]})

    bin =
      "region"
      |> Filter.geo_contains(point)
      |> Filter.using_index("region_geo_idx")
      |> FilterCodec.encode()

    <<1::8, 0::8, 23::8, begin_size::32-big, begin_val::binary-size(begin_size), end_size::32-big,
      end_val::binary-size(end_size)>> = bin

    assert begin_val == point
    assert end_val == point
  end

  test "encode_ctx/1 produces a MessagePack payload" do
    ctx = [Ctx.map_key("roles"), Ctx.list_index(0)]
    encoded = FilterCodec.encode_ctx(ctx)

    assert is_binary(encoded)
    assert byte_size(encoded) > 0
  end

  test "collection index filters use integer particles and ctx preserves strings, bytes, and integers" do
    for filter <- [
          Filter.contains("items", :list, "x"),
          Filter.contains("items", :mapkeys, "x"),
          Filter.contains("items", :mapvalues, "x")
        ] do
      <<1::8, 5::8, "items", 1::8, begin_size::32-big, "x", end_size::32-big, "x">> =
        FilterCodec.encode(filter)

      assert begin_size == 1
      assert end_size == 1
    end

    encoded =
      FilterCodec.encode_ctx([Ctx.map_key("roles"), {0x22, {:bytes, <<1, 2>>}}, {0x10, 7}])

    assert MessagePack.unpack!(encoded) == [
             34,
             "roles",
             34,
             <<4, 1, 2>>,
             16,
             7
           ]
  end
end
