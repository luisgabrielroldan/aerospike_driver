defmodule Aerospike.Protocol.MessageTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.Message

  doctest Aerospike.Protocol.Message

  describe "encode/3 and decode/1 round-trip" do
    test "preserves version, type, and body" do
      encoded = Message.encode(2, 3, <<10, 20, 30>>)
      assert {:ok, {2, 3, <<10, 20, 30>>}} = Message.decode(encoded)
    end

    test "handles an empty body" do
      encoded = Message.encode(2, 1, <<>>)
      assert {:ok, {2, 1, <<>>}} = Message.decode(encoded)
    end
  end

  describe "decode_header/1" do
    test "returns all three fields verbatim for a non-canonical version" do
      # The header decoder does not enforce version == 2 — that is the
      # caller's job. A frame advertising a future version still parses
      # so callers can build a typed error that names the observed value.
      header = Message.encode_header(3, 3, 42)
      assert {:ok, {3, 3, 42}} = Message.decode_header(header)
    end

    test "returns all three fields verbatim for an unknown type" do
      header = Message.encode_header(2, 99, 7)
      assert {:ok, {2, 99, 7}} = Message.decode_header(header)
    end

    test "rejects a short header" do
      assert {:error, :incomplete_header} = Message.decode_header(<<2, 1, 0>>)
    end
  end

  describe "type constants" do
    test "match the wire protocol spec" do
      assert Message.proto_version() == 2
      assert Message.type_info() == 1
      assert Message.type_as_msg() == 3
      assert Message.type_compressed() == 4
    end
  end

  describe "encode_compressed_payload/1" do
    # Symmetric with `decode_compressed_payload/1`. Wraps a complete
    # AS_MSG frame in a type-4 proto envelope with the 8-byte
    # uncompressed-size prefix Go / Java require. The inner frame is
    # expected to already carry its own 8-byte proto header.
    test "round-trips through decode + zlib.uncompress" do
      inner_body = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>
      inner_frame = Message.encode(2, 3, inner_body)

      wire = Message.encode_compressed_payload(inner_frame)

      assert {:ok, {2, 4, body}} = Message.decode(wire)
      assert {:ok, {size, compressed}} = Message.decode_compressed_payload(body)
      assert size == byte_size(inner_frame)
      assert :zlib.uncompress(compressed) == inner_frame
    end

    test "sets the outer proto length to match the body size" do
      inner_frame = Message.encode(2, 3, :binary.copy(<<0>>, 256))
      wire = Message.encode_compressed_payload(inner_frame)

      <<header::binary-8, body::binary>> = wire
      assert {:ok, {2, 4, length}} = Message.decode_header(header)
      assert length == byte_size(body)
    end
  end

  describe "decode_compressed_payload/1" do
    # A compressed AS_MSG reply's body is an 8-byte big-endian uint64
    # (the uncompressed inner-frame size, header included) followed by
    # the zlib-compressed inner-frame bytes. Layout reference: Go
    # `command.go:3574-3627` + `multi_command.go:150-173`.
    test "splits the 8-byte uncompressed-size prefix from the compressed bytes" do
      compressed = <<1, 2, 3, 4, 5>>
      body = <<42::64-big, compressed::binary>>

      assert {:ok, {42, ^compressed}} = Message.decode_compressed_payload(body)
    end

    test "accepts a zero-length compressed tail" do
      body = <<99::64-big>>
      assert {:ok, {99, <<>>}} = Message.decode_compressed_payload(body)
    end

    test "rejects a body shorter than the 8-byte size prefix" do
      assert {:error, :incomplete_compressed_payload} =
               Message.decode_compressed_payload(<<1, 2, 3, 4, 5, 6, 7>>)
    end

    test "rejects an empty body" do
      assert {:error, :incomplete_compressed_payload} = Message.decode_compressed_payload(<<>>)
    end
  end
end
