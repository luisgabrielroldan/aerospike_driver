defmodule Aerospike.Protocol.MessageTest do
  use ExUnit.Case, async: true
  doctest Aerospike.Protocol.Message

  alias Aerospike.Protocol.Message

  describe "constants" do
    test "proto_version returns 2" do
      assert Message.proto_version() == 2
    end

    test "type_info returns 1" do
      assert Message.type_info() == 1
    end

    test "type_as_msg returns 3" do
      assert Message.type_as_msg() == 3
    end

    test "type_compressed returns 4" do
      assert Message.type_compressed() == 4
    end
  end

  describe "encode_header/3" do
    test "encodes version, type, and length into 8 bytes" do
      header = Message.encode_header(2, 3, 100)
      assert byte_size(header) == 8
      # version=2 at bits 56-63, type=3 at bits 48-55, length=100 at bits 0-47
      # 0x02_03_00_00_00_00_00_64
      assert header == <<2, 3, 0, 0, 0, 0, 0, 100>>
    end

    test "handles zero length" do
      header = Message.encode_header(2, 1, 0)
      assert header == <<2, 1, 0, 0, 0, 0, 0, 0>>
    end

    test "handles large length (48-bit max)" do
      max_length = 0xFFFFFFFFFFFF
      header = Message.encode_header(2, 3, max_length)
      assert header == <<2, 3, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>
    end

    test "known value for 22-byte AS_MSG header" do
      # proto = length | (version << 56) | (type << 48)
      # For a 22-byte AS_MSG body: version=2, type=3, length=22
      header = Message.encode_header(2, 3, 22)
      assert header == <<2, 3, 0, 0, 0, 0, 0, 22>>
    end
  end

  describe "decode_header/1" do
    test "decodes 8-byte header to {version, type, length}" do
      header = <<2, 3, 0, 0, 0, 0, 0, 100>>
      assert Message.decode_header(header) == {:ok, {2, 3, 100}}
    end

    test "handles zero length" do
      header = <<2, 1, 0, 0, 0, 0, 0, 0>>
      assert Message.decode_header(header) == {:ok, {2, 1, 0}}
    end

    test "handles max 48-bit length" do
      header = <<2, 3, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>
      assert Message.decode_header(header) == {:ok, {2, 3, 0xFFFFFFFFFFFF}}
    end

    test "returns error for incomplete header" do
      assert Message.decode_header(<<1, 2, 3>>) == {:error, :incomplete_header}
      assert Message.decode_header(<<>>) == {:error, :incomplete_header}
    end
  end

  describe "encode/3 and decode/1 roundtrip" do
    test "roundtrips with empty payload" do
      encoded = Message.encode(2, 1, "")
      assert {:ok, {2, 1, ""}} = Message.decode(encoded)
    end

    test "roundtrips with short payload" do
      payload = "namespaces\n"
      encoded = Message.encode(2, 1, payload)
      assert {:ok, {2, 1, ^payload}} = Message.decode(encoded)
    end

    test "roundtrips with binary payload" do
      payload = <<0, 1, 2, 3, 255, 254, 253>>
      encoded = Message.encode(2, 3, payload)
      assert {:ok, {2, 3, ^payload}} = Message.decode(encoded)
    end

    test "roundtrips with large payload" do
      payload = :binary.copy(<<0>>, 10_000)
      encoded = Message.encode(2, 3, payload)
      assert {:ok, {2, 3, ^payload}} = Message.decode(encoded)
    end
  end

  describe "decode/1 error cases" do
    test "returns error for incomplete header" do
      assert Message.decode(<<1, 2, 3>>) == {:error, :incomplete_header}
    end

    test "returns error for incomplete body" do
      # Header says 10 bytes but only 5 provided
      incomplete = <<2, 1, 0, 0, 0, 0, 0, 10, "short">>
      assert Message.decode(incomplete) == {:error, :incomplete_body}
    end

    test "handles extra trailing bytes gracefully" do
      # 4-byte payload followed by extra data
      with_extra = <<2, 1, 0, 0, 0, 0, 0, 4, "test", "extra">>
      assert {:ok, {2, 1, "test"}} = Message.decode(with_extra)
    end
  end

  describe "encode_info/1" do
    test "creates info message with type 1" do
      encoded = Message.encode_info("namespaces\n")
      assert {:ok, {2, 1, "namespaces\n"}} = Message.decode(encoded)
    end
  end

  describe "encode_as_msg/1" do
    test "creates AS_MSG with type 3" do
      payload = <<22, 0, 0, 0, 0, 0>>
      encoded = Message.encode_as_msg(payload)
      assert {:ok, {2, 3, ^payload}} = Message.decode(encoded)
    end
  end
end
