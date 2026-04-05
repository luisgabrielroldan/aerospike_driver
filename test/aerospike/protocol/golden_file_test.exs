defmodule Aerospike.Protocol.GoldenFileTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Info
  alias Aerospike.Protocol.Message
  alias Aerospike.Test.Fixtures

  describe "info request fixtures" do
    test "decode namespaces request" do
      fixture = Fixtures.info_request_namespaces()

      assert {:ok, {2, 1, body}} = Message.decode(fixture)
      assert body == "namespaces\n"
    end

    test "encode matches namespaces request fixture" do
      fixture = Fixtures.info_request_namespaces()
      encoded = Info.encode_request(["namespaces"])
      assert encoded == fixture
    end

    test "decode multi-command request" do
      fixture = Fixtures.info_request_multi()

      assert {:ok, {2, 1, body}} = Message.decode(fixture)
      assert body == "node\nbuild\n"
    end

    test "encode matches multi-command request fixture" do
      fixture = Fixtures.info_request_multi()
      encoded = Info.encode_request(["node", "build"])
      assert encoded == fixture
    end
  end

  describe "info response fixtures" do
    test "decode namespaces response" do
      fixture = Fixtures.info_response_namespaces()

      assert {:ok, result} = Info.decode_message(fixture)
      assert result == %{"namespaces" => "test;bar"}
    end
  end

  describe "AS_MSG get command fixture" do
    test "decode get command" do
      fixture = Fixtures.asm_msg_get_simple()

      # Strip protocol header
      <<2, 3, _::binary-6, payload::binary>> = fixture

      assert {:ok, msg} = AsmMsg.decode(payload)

      # Check flags
      import Bitwise
      assert msg.info1 == (AsmMsg.info1_read() ||| AsmMsg.info1_get_all())
      assert msg.info2 == 0

      # Check fields
      assert length(msg.fields) == 3
      [ns_field, set_field, digest_field] = msg.fields

      assert ns_field.type == Field.type_namespace()
      assert ns_field.data == "test"

      assert set_field.type == Field.type_table()
      assert set_field.data == "users"

      assert digest_field.type == Field.type_digest()
      assert byte_size(digest_field.data) == 20

      # No operations for read
      assert msg.operations == []
    end

    test "encode matches get command fixture" do
      import Bitwise

      digest = :binary.copy(<<0xAB>>, 20)

      msg = %AsmMsg{
        info1: AsmMsg.info1_read() ||| AsmMsg.info1_get_all(),
        fields: [
          Field.namespace("test"),
          Field.set("users"),
          Field.digest(digest)
        ]
      }

      encoded = Message.encode_as_msg(AsmMsg.encode(msg))
      fixture = Fixtures.asm_msg_get_simple()

      assert encoded == fixture
    end
  end

  describe "AS_MSG exists command fixture" do
    test "decode exists command" do
      import Bitwise
      fixture = Fixtures.asm_msg_exists_simple()
      <<2, 3, _::binary-6, payload::binary>> = fixture
      assert {:ok, msg} = AsmMsg.decode(payload)
      assert msg.info1 == (AsmMsg.info1_read() ||| AsmMsg.info1_nobindata())
      assert msg.operations == []
    end

    test "encode matches exists command fixture" do
      digest = :binary.copy(<<0xAB>>, 20)
      msg = AsmMsg.exists_command("test", "users", digest)
      encoded = Message.encode_as_msg(AsmMsg.encode(msg))
      assert encoded == Fixtures.asm_msg_exists_simple()
    end
  end

  describe "AS_MSG touch command fixture" do
    test "decode touch command" do
      fixture = Fixtures.asm_msg_touch_simple()
      <<2, 3, _::binary-6, payload::binary>> = fixture
      assert {:ok, msg} = AsmMsg.decode(payload)
      assert msg.info2 == AsmMsg.info2_write()
      assert length(msg.operations) == 1
      [op] = msg.operations
      assert op.op_type == Operation.op_touch()
    end

    test "encode matches touch command fixture" do
      digest = :binary.copy(<<0xCD>>, 20)
      msg = AsmMsg.touch_command("test", "users", digest)
      encoded = Message.encode_as_msg(AsmMsg.encode(msg))
      assert encoded == Fixtures.asm_msg_touch_simple()
    end
  end

  describe "AS_MSG put command fixture" do
    test "decode put command" do
      fixture = Fixtures.asm_msg_put_simple()

      # Strip protocol header
      <<2, 3, _::binary-6, payload::binary>> = fixture

      assert {:ok, msg} = AsmMsg.decode(payload)

      # Check flags
      assert msg.info1 == 0
      assert msg.info2 == AsmMsg.info2_write()

      # Check fields
      assert length(msg.fields) == 3
      [ns_field, set_field, digest_field] = msg.fields

      assert ns_field.data == "test"
      assert set_field.data == "users"
      assert byte_size(digest_field.data) == 20

      # Check operations
      assert length(msg.operations) == 1
      [write_op] = msg.operations

      assert write_op.op_type == Operation.op_write()
      assert write_op.particle_type == Operation.particle_string()
      assert write_op.bin_name == "name"
      assert write_op.data == "Alice"
    end

    test "encode matches put command fixture" do
      digest = :binary.copy(<<0xCD>>, 20)

      msg = %AsmMsg{
        info2: AsmMsg.info2_write(),
        fields: [
          Field.namespace("test"),
          Field.set("users"),
          Field.digest(digest)
        ],
        operations: [
          Operation.write_string("name", "Alice")
        ]
      }

      encoded = Message.encode_as_msg(AsmMsg.encode(msg))
      fixture = Fixtures.asm_msg_put_simple()

      assert encoded == fixture
    end
  end

  describe "AS_MSG response fixtures" do
    test "decode success response with bins" do
      fixture = Fixtures.asm_msg_response_ok()

      # Strip protocol header
      <<2, 3, _::binary-6, payload::binary>> = fixture

      assert {:ok, msg} = AsmMsg.decode(payload)

      # Check metadata
      assert msg.result_code == 0
      assert msg.generation == 5
      assert msg.expiration == 3600
      assert msg.info3 == AsmMsg.info3_last()

      # Check operations (response returns bin values as operations)
      assert length(msg.operations) == 2
      [name_op, count_op] = msg.operations

      assert name_op.bin_name == "name"
      assert name_op.particle_type == Operation.particle_string()
      assert name_op.data == "Alice"

      assert count_op.bin_name == "count"
      assert count_op.particle_type == Operation.particle_integer()
      assert count_op.data == <<42::64-big-signed>>
    end

    test "decode not found response" do
      fixture = Fixtures.asm_msg_response_not_found()

      # Strip protocol header
      <<2, 3, _::binary-6, payload::binary>> = fixture

      assert {:ok, msg} = AsmMsg.decode(payload)

      # KEY_NOT_FOUND
      assert msg.result_code == 2
      assert msg.operations == []
    end
  end

  describe "protocol header roundtrips with fixtures" do
    test "full message roundtrip for get command" do
      fixture = Fixtures.asm_msg_get_simple()

      # Decode
      assert {:ok, {version, type, body}} = Message.decode(fixture)
      assert version == 2
      assert type == 3

      # Re-encode
      re_encoded = Message.encode(version, type, body)
      assert re_encoded == fixture
    end

    test "full message roundtrip for put command" do
      fixture = Fixtures.asm_msg_put_simple()

      assert {:ok, {version, type, body}} = Message.decode(fixture)
      re_encoded = Message.encode(version, type, body)
      assert re_encoded == fixture
    end

    test "full message roundtrip for exists command" do
      fixture = Fixtures.asm_msg_exists_simple()
      assert {:ok, {version, type, body}} = Message.decode(fixture)
      assert Message.encode(version, type, body) == fixture
    end

    test "full message roundtrip for touch command" do
      fixture = Fixtures.asm_msg_touch_simple()
      assert {:ok, {version, type, body}} = Message.decode(fixture)
      assert Message.encode(version, type, body) == fixture
    end

    test "full message roundtrip for response" do
      fixture = Fixtures.asm_msg_response_ok()

      assert {:ok, {version, type, body}} = Message.decode(fixture)
      re_encoded = Message.encode(version, type, body)
      assert re_encoded == fixture
    end
  end
end
