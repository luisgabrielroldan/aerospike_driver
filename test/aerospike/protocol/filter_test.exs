defmodule Aerospike.Protocol.FilterTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Filter
  alias Aerospike.Protocol.Filter, as: FilterCodec

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

  test "encode_ctx/1 produces a MessagePack payload" do
    ctx = [Ctx.map_key("roles"), Ctx.list_index(0)]
    encoded = FilterCodec.encode_ctx(ctx)

    assert is_binary(encoded)
    assert byte_size(encoded) > 0
  end
end
