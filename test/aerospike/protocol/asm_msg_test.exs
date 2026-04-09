defmodule Aerospike.Protocol.AsmMsgTest do
  use ExUnit.Case, async: true
  doctest Aerospike.Protocol.AsmMsg

  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation

  describe "info flag constants" do
    test "info1 flags" do
      assert AsmMsg.info1_read() == 0x01
      assert AsmMsg.info1_get_all() == 0x02
      assert AsmMsg.info1_short_query() == 0x04
      assert AsmMsg.info1_batch() == 0x08
      assert AsmMsg.info1_nobindata() == 0x20
      assert AsmMsg.info1_read_mode_ap_all() == 0x40
      assert AsmMsg.info1_compress_response() == 0x80
    end

    test "info2 flags" do
      assert AsmMsg.info2_write() == 0x01
      assert AsmMsg.info2_delete() == 0x02
      assert AsmMsg.info2_generation() == 0x04
      assert AsmMsg.info2_generation_gt() == 0x08
      assert AsmMsg.info2_durable_delete() == 0x10
      assert AsmMsg.info2_create_only() == 0x20
      assert AsmMsg.info2_respond_all_ops() == 0x80
    end

    test "info2 relax_ap_long_query flag" do
      assert AsmMsg.info2_relax_ap_long_query() == 0x40
    end

    test "info3 flags" do
      assert AsmMsg.info3_last() == 0x01
      assert AsmMsg.info3_commit_master() == 0x02
      assert AsmMsg.info3_partition_done() == 0x04
      assert AsmMsg.info3_update_only() == 0x08
      assert AsmMsg.info3_create_or_replace() == 0x10
      assert AsmMsg.info3_replace_only() == 0x20
      assert AsmMsg.info3_sc_read_type() == 0x40
      assert AsmMsg.info3_sc_read_relax() == 0x80
    end

    test "info4/mrt flags" do
      assert AsmMsg.info4_mrt_verify_read() == 0x01
      assert AsmMsg.info4_mrt_roll_forward() == 0x02
      assert AsmMsg.info4_mrt_roll_back() == 0x04
      assert AsmMsg.info4_mrt_on_locking_only() == 0x10
    end
  end

  describe "encode/1" do
    test "encodes minimal message with no fields or operations" do
      msg = %AsmMsg{}
      encoded = IO.iodata_to_binary(AsmMsg.encode(msg))

      # 22-byte header with all zeros except header_size
      assert byte_size(encoded) == 22

      # First byte is header size (22)
      assert binary_part(encoded, 0, 1) == <<22>>

      # Field count at bytes 18-19 (offset from start)
      <<_::binary-18, field_count::16-big, _::binary>> = encoded
      assert field_count == 0

      # Operation count at bytes 20-21
      <<_::binary-20, op_count::16-big>> = encoded
      assert op_count == 0
    end

    test "encodes message with info flags" do
      import Bitwise
      msg = %AsmMsg{info1: 0x03, info2: 0x01, info3: 0x10, info4: 0x02}
      encoded = IO.iodata_to_binary(AsmMsg.encode(msg))

      <<22, info1, info2, info3, info4, _rest::binary>> = encoded
      assert info1 == 0x03
      assert info2 == 0x01
      assert info3 == 0x10
      assert info4 == 0x02
    end

    test "encodes generation and expiration" do
      msg = %AsmMsg{generation: 5, expiration: 3600}
      encoded = IO.iodata_to_binary(AsmMsg.encode(msg))

      <<_header::binary-6, generation::32-big, expiration::32-big, _rest::binary>> = encoded
      assert generation == 5
      assert expiration == 3600
    end

    test "encodes signed timeout" do
      msg = %AsmMsg{timeout: -1}
      encoded = IO.iodata_to_binary(AsmMsg.encode(msg))

      <<_header::binary-14, timeout::32-big-signed, _rest::binary>> = encoded
      assert timeout == -1
    end

    test "encodes fields" do
      msg = %AsmMsg{
        fields: [
          Field.namespace("test"),
          Field.set("users")
        ]
      }

      encoded = IO.iodata_to_binary(AsmMsg.encode(msg))

      # Check field count
      <<_::binary-18, field_count::16-big, _op_count::16-big, rest::binary>> = encoded
      assert field_count == 2

      # First field (namespace)
      {:ok, field1, rest} = Field.decode(rest)
      assert field1.type == Field.type_namespace()
      assert field1.data == "test"

      # Second field (set)
      {:ok, field2, _rest} = Field.decode(rest)
      assert field2.type == Field.type_table()
      assert field2.data == "users"
    end

    test "encodes operations" do
      msg = %AsmMsg{
        operations: [
          Operation.write_string("name", "Alice"),
          Operation.write_integer("age", 30)
        ]
      }

      encoded = IO.iodata_to_binary(AsmMsg.encode(msg))

      # Check operation count
      <<_::binary-20, op_count::16-big, rest::binary>> = encoded
      assert op_count == 2

      # First operation
      {:ok, op1, rest} = Operation.decode(rest)
      assert op1.bin_name == "name"
      assert op1.particle_type == Operation.particle_string()

      # Second operation
      {:ok, op2, _rest} = Operation.decode(rest)
      assert op2.bin_name == "age"
      assert op2.particle_type == Operation.particle_integer()
    end
  end

  describe "decode/1" do
    test "decodes minimal message" do
      # 22-byte header with all zeros except header_size
      binary = <<22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>
      assert {:ok, msg} = AsmMsg.decode(binary)
      assert msg.info1 == 0
      assert msg.fields == []
      assert msg.operations == []
    end

    test "decodes message with info flags" do
      binary = <<22, 0x03, 0x01, 0x10, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>
      assert {:ok, msg} = AsmMsg.decode(binary)
      assert msg.info1 == 0x03
      assert msg.info2 == 0x01
      assert msg.info3 == 0x10
      assert msg.info4 == 0x02
    end

    test "decodes generation, expiration, and timeout" do
      binary =
        <<22, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0xE1, 0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0>>

      assert {:ok, msg} = AsmMsg.decode(binary)
      assert msg.generation == 5
      assert msg.expiration == 57_616
      assert msg.timeout == -1
    end

    test "returns error for invalid header size" do
      binary = <<23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>
      assert {:error, :invalid_header_size} = AsmMsg.decode(binary)
    end

    test "returns error for incomplete header" do
      assert {:error, :incomplete_header} = AsmMsg.decode(<<22, 0, 0>>)
    end

    test "returns error when field data is truncated" do
      # Header declares 1 field but body has only 1 byte (too short for field decoding)
      binary =
        <<22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0xFF>>

      assert {:error, _} = AsmMsg.decode(binary)
    end

    test "returns error when operation data is truncated" do
      # Header declares 0 fields, 1 operation but body has only 1 byte
      binary =
        <<22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xFF>>

      assert {:error, _} = AsmMsg.decode(binary)
    end

    test "returns incomplete_header for empty binary" do
      assert {:error, :incomplete_header} = AsmMsg.decode(<<>>)
    end
  end

  describe "encode/decode roundtrip" do
    test "roundtrips minimal message" do
      msg = %AsmMsg{}
      assert {:ok, decoded} = msg |> AsmMsg.encode() |> IO.iodata_to_binary() |> AsmMsg.decode()
      assert decoded.info1 == 0
      assert decoded.fields == []
      assert decoded.operations == []
    end

    test "roundtrips message with all header fields" do
      msg = %AsmMsg{
        info1: 0x03,
        info2: 0x01,
        info3: 0x10,
        info4: 0x02,
        result_code: 0,
        generation: 100,
        expiration: 7200,
        timeout: 1000
      }

      assert {:ok, decoded} = msg |> AsmMsg.encode() |> IO.iodata_to_binary() |> AsmMsg.decode()
      assert decoded.info1 == msg.info1
      assert decoded.info2 == msg.info2
      assert decoded.info3 == msg.info3
      assert decoded.info4 == msg.info4
      assert decoded.generation == msg.generation
      assert decoded.expiration == msg.expiration
      assert decoded.timeout == msg.timeout
    end

    test "roundtrips message with fields" do
      msg = %AsmMsg{
        fields: [
          Field.namespace("testns"),
          Field.set("testset"),
          Field.digest(:binary.copy(<<0xAB>>, 20))
        ]
      }

      assert {:ok, decoded} = msg |> AsmMsg.encode() |> IO.iodata_to_binary() |> AsmMsg.decode()
      assert length(decoded.fields) == 3

      [ns, set, digest] = decoded.fields
      assert ns.type == Field.type_namespace()
      assert ns.data == "testns"
      assert set.type == Field.type_table()
      assert set.data == "testset"
      assert digest.type == Field.type_digest()
      assert byte_size(digest.data) == 20
    end

    test "roundtrips message with operations" do
      msg = %AsmMsg{
        operations: [
          Operation.write_string("name", "Bob"),
          Operation.write_integer("count", 42),
          Operation.read("status")
        ]
      }

      assert {:ok, decoded} = msg |> AsmMsg.encode() |> IO.iodata_to_binary() |> AsmMsg.decode()
      assert length(decoded.operations) == 3

      [op1, op2, op3] = decoded.operations
      assert op1.bin_name == "name"
      assert op1.data == "Bob"
      assert op2.bin_name == "count"
      assert op2.data == <<42::64-big-signed>>
      assert op3.bin_name == "status"
      assert op3.op_type == Operation.op_read()
    end

    test "roundtrips complete message with fields and operations" do
      digest = :binary.copy(<<0xCD>>, 20)

      msg = %AsmMsg{
        info1: AsmMsg.info1_read(),
        info2: AsmMsg.info2_write(),
        generation: 1,
        expiration: 86_400,
        timeout: 5000,
        fields: [
          Field.namespace("production"),
          Field.set("accounts"),
          Field.digest(digest)
        ],
        operations: [
          Operation.write_string("email", "test@example.com"),
          Operation.write_integer("login_count", 1)
        ]
      }

      assert {:ok, decoded} = msg |> AsmMsg.encode() |> IO.iodata_to_binary() |> AsmMsg.decode()

      assert decoded.info1 == msg.info1
      assert decoded.info2 == msg.info2
      assert decoded.generation == 1
      assert decoded.expiration == 86_400
      assert decoded.timeout == 5000
      assert length(decoded.fields) == 3
      assert length(decoded.operations) == 2
    end
  end

  describe "command constructors" do
    test "read_command creates proper structure" do
      digest = :binary.copy(<<0xFF>>, 20)
      msg = AsmMsg.read_command("ns", "set", digest)

      import Bitwise
      assert msg.info1 == (AsmMsg.info1_read() ||| AsmMsg.info1_get_all())
      assert length(msg.fields) == 3
      assert msg.operations == []
    end

    test "write_command creates proper structure" do
      digest = :binary.copy(<<0xFF>>, 20)
      ops = [Operation.write_string("bin", "value")]
      msg = AsmMsg.write_command("ns", "set", digest, ops)

      assert msg.info2 == AsmMsg.info2_write()
      assert length(msg.fields) == 3
      assert length(msg.operations) == 1
    end

    test "delete_command creates proper structure" do
      digest = :binary.copy(<<0xFF>>, 20)
      msg = AsmMsg.delete_command("ns", "set", digest)

      import Bitwise
      assert msg.info2 == (AsmMsg.info2_write() ||| AsmMsg.info2_delete())
      assert length(msg.fields) == 3
      assert msg.operations == []
    end

    test "exists_command creates proper structure" do
      digest = :binary.copy(<<0xFF>>, 20)
      msg = AsmMsg.exists_command("ns", "set", digest)

      import Bitwise
      assert msg.info1 == (AsmMsg.info1_read() ||| AsmMsg.info1_nobindata())
      assert msg.info2 == 0
      assert length(msg.fields) == 3
      assert msg.operations == []
    end

    test "touch_command creates proper structure" do
      digest = :binary.copy(<<0xFF>>, 20)
      msg = AsmMsg.touch_command("ns", "set", digest)

      assert msg.info2 == AsmMsg.info2_write()
      assert length(msg.fields) == 3
      assert [op] = msg.operations
      assert op.op_type == Operation.op_touch()
    end
  end
end
