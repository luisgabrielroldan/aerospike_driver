defmodule Aerospike.Protocol.AsmMsg.ValueTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Geo
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.MessagePack

  test "encode_value/1 supports the write particle subset and rejects unsupported values" do
    assert Value.encode_value(nil) == {:ok, {0, <<>>}}
    assert Value.encode_value(7) == {:ok, {1, <<7::64-signed-big>>}}
    assert Value.encode_value(1.5) == {:ok, {2, <<1.5::64-float-big>>}}
    assert Value.encode_value("abc") == {:ok, {3, "abc"}}
    assert Value.encode_value({:blob, <<1, 2>>}) == {:ok, {4, <<1, 2>>}}
    assert Value.encode_value(true) == {:ok, {17, <<1>>}}
    assert Value.encode_value(false) == {:ok, {17, <<0>>}}

    assert {:error, %Error{code: :invalid_argument, message: message}} =
             Value.encode_value(:unsupported)

    assert message =~ "unsupported write particle"
  end

  test "encode_value/1 supports typed and explicit GeoJSON particles" do
    point_json = Geo.point(-122.5, 45.5) |> Geo.to_json()

    assert Value.encode_value(Geo.point(-122.5, 45.5)) ==
             {:ok, {23, <<0::8, 0::16-big, point_json::binary>>}}

    raw_json = ~s({"type":"LineString","coordinates":[[0,0],[1,1]]})

    assert Value.encode_value({:geojson, raw_json}) ==
             {:ok, {23, <<0::8, 0::16-big, raw_json::binary>>}}
  end

  test "encode_bin_operations/2 sorts bins and enforces op-specific value types" do
    ops = Value.encode_bin_operations(%{"a" => 1, b: 2})
    assert Enum.map(ops, & &1.bin_name) == ["a", "b"]
    assert Enum.all?(ops, &(&1.op_type == Operation.op_write()))

    add_ops = Value.encode_bin_operations(%{"count" => 5}, Operation.op_add())
    assert [%Operation{particle_type: 1, data: <<5::64-signed-big>>}] = add_ops

    append_ops = Value.encode_bin_operations(%{"name" => "ada"}, Operation.op_append())
    assert [%Operation{particle_type: 3, data: "ada"}] = append_ops

    prepend_ops = Value.encode_bin_operations(%{"name" => "ada"}, Operation.op_prepend())
    assert [%Operation{particle_type: 3, data: "ada"}] = prepend_ops

    assert_raise ArgumentError, ~r/bin name must be a string or atom/, fn ->
      Value.encode_bin_operations(%{1 => "bad"})
    end

    assert_raise ArgumentError, ~r/add requires integer values/, fn ->
      Value.encode_bin_operations(%{"count" => "bad"}, Operation.op_add())
    end

    assert_raise ArgumentError, ~r/append requires string values/, fn ->
      Value.encode_bin_operations(%{"name" => 1}, Operation.op_append())
    end

    assert_raise ArgumentError, ~r/prepend requires string values/, fn ->
      Value.encode_bin_operations(%{"name" => 1}, Operation.op_prepend())
    end
  end

  test "decode_value/2 decodes supported particles and preserves unknown ones as raw tuples" do
    assert Value.decode_value(0, <<>>) == {:ok, nil}
    assert Value.decode_value(1, <<7::64-signed-big>>) == {:ok, 7}
    assert Value.decode_value(2, <<1.5::64-float-big>>) == {:ok, 1.5}
    assert Value.decode_value(3, "abc") == {:ok, "abc"}
    assert Value.decode_value(4, <<1, 2>>) == {:ok, {:blob, <<1, 2>>}}
    assert Value.decode_value(17, <<0>>) == {:ok, false}
    assert Value.decode_value(17, <<1>>) == {:ok, true}
    assert Value.decode_value(99, <<7, 8>>) == {:ok, {:raw, 99, <<7, 8>>}}
  end

  test "decode_value/2 decodes GeoJSON particles with server cell headers" do
    json = ~s({"type":"Point","coordinates":[-122.5,45.5]})
    cell = <<1::64-big>>

    assert Value.decode_value(23, <<0::8, 1::16-big, cell::binary, json::binary>>) ==
             {:ok, Geo.point(-122.5, 45.5)}
  end

  test "decode_value/2 decodes GeoJSON particles without server cell headers" do
    json = ~s({"type":"AeroCircle","coordinates":[[-122,45],5000]})

    assert Value.decode_value(23, json) == {:ok, Geo.circle(-122, 45, 5_000)}
  end

  test "decode_value/2 preserves unsupported and malformed GeoJSON particles" do
    raw_json = ~s({"type":"LineString","coordinates":[[0,0],[1,1]]})
    malformed_header = <<0::8, 1::16-big, "short">>

    assert Value.decode_value(23, raw_json) == {:ok, {:geojson, raw_json}}
    assert Value.decode_value(23, malformed_header) == {:ok, {:geojson, malformed_header}}
  end

  test "decode_value/2 decodes list particles returned by collection operations" do
    assert Value.decode_value(20, MessagePack.pack!([14, 0])) == {:ok, [14, 0]}

    assert {:error, %Error{code: :parse_error, message: message}} =
             Value.decode_value(20, <<0x91, 0x01, 0x02>>)

    assert message =~ "trailing bytes"
  end

  test "decode_value/2 decodes map particles returned by collection operations" do
    assert Value.decode_value(19, MessagePack.pack!(%{"a" => 1, "b" => [2]})) ==
             {:ok, %{"a" => 1, "b" => [2]}}

    assert {:error, %Error{code: :parse_error, message: message}} =
             Value.decode_value(19, <<0x81, 0xA1, ?a, 0x01, 0x02>>)

    assert message =~ "trailing bytes"
  end
end
