defmodule Aerospike.Protocol.FilterTest do
  use ExUnit.Case, async: true

  alias Aerospike.Filter
  alias Aerospike.Protocol.Filter, as: IndexRange

  test "range filter (integer) layout" do
    f = Filter.range("age", 10, 20)
    bin = IndexRange.encode(f)

    assert byte_size(bin) ==
             1 + 1 + 3 + 1 + 4 + 8 + 4 + 8

    <<
      1::8,
      3::8,
      "age"::binary,
      1::8,
      bsz::32-big,
      begin_val::binary-size(bsz),
      esz::32-big,
      end_val::binary-size(esz)
    >> = bin

    assert bsz == 8
    assert esz == 8
    assert begin_val == <<10::64-signed-big>>
    assert end_val == <<20::64-signed-big>>
  end

  test "equal filter (string) uses identical begin/end" do
    f = Filter.equal("city", "PDX")
    bin = IndexRange.encode(f)

    <<
      1::8,
      4::8,
      "city"::binary,
      3::8,
      bsz::32-big,
      begin_val::binary-size(bsz),
      esz::32-big,
      end_val::binary-size(esz)
    >> = bin

    assert bsz == 3
    assert esz == 3
    assert begin_val == "PDX"
    assert end_val == "PDX"
  end

  test "geo_within uses GEOJSON particle type" do
    region = ~s({"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]})
    f = Filter.geo_within("loc", region)
    bin = IndexRange.encode(f)

    <<
      1::8,
      3::8,
      "loc"::binary,
      23::8,
      bsz::32-big,
      begin_val::binary-size(bsz),
      esz::32-big,
      end_val::binary-size(esz)
    >> = bin

    assert begin_val == region
    assert end_val == region
    assert bsz == byte_size(region)
    assert esz == byte_size(region)
  end
end
