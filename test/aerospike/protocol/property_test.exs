defmodule Aerospike.Protocol.PropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Info
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.ResultCode

  @moduletag :property

  describe "Message header encode/decode" do
    property "roundtrips any valid protocol header" do
      check all(
              version <- integer(0..255),
              type <- integer(0..255),
              # 48-bit max length
              length <- integer(0..0x00FFFFFFFFFFFF)
            ) do
        header = Message.encode_header(version, type, length)
        assert {:ok, {^version, ^type, ^length}} = Message.decode_header(header)
      end
    end

    property "roundtrips any message with binary payload" do
      check all(
              version <- integer(0..255),
              type <- integer(0..255),
              # Keep payload reasonable for property tests
              payload <- binary(max_length: 1000)
            ) do
        encoded = Message.encode(version, type, payload)
        assert {:ok, {^version, ^type, ^payload}} = Message.decode(encoded)
      end
    end

    property "header is always exactly 8 bytes" do
      check all(
              version <- integer(0..255),
              type <- integer(0..255),
              length <- integer(0..0x00FFFFFFFFFFFF)
            ) do
        header = Message.encode_header(version, type, length)
        assert byte_size(header) == 8
      end
    end
  end

  describe "Info protocol" do
    property "encode_request/decode_response roundtrip preserves commands" do
      check all(
              commands <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 50), max_length: 10)
            ) do
        encoded = Info.encode_request(commands)

        # Verify it's a valid message
        assert {:ok, {2, 1, body}} = Message.decode(encoded)

        # Verify commands are present (with newline delimiters)
        expected_body = if commands == [], do: "", else: Enum.join(commands, "\n") <> "\n"
        assert body == expected_body
      end
    end

    property "decode_response parses any valid key-value format" do
      check all(
              pairs <-
                list_of(tuple({string(:alphanumeric, min_length: 1), string(:alphanumeric)}),
                  max_length: 10
                ),
              pairs != []
            ) do
        # Build response body
        body =
          Enum.map_join(pairs, "\n", fn {k, v} -> "#{k}\t#{v}" end) <> "\n"

        assert {:ok, result} = Info.decode_response(body)

        expected = Map.new(pairs)

        for {k, v} <- expected do
          assert result[k] == v
        end
      end
    end
  end

  describe "ResultCode mapping" do
    property "from_integer/to_integer roundtrip for all known codes" do
      all_atoms = ResultCode.all_codes()

      check all(atom <- member_of(all_atoms)) do
        {:ok, code} = ResultCode.to_integer(atom)
        {:ok, back} = ResultCode.from_integer(code)
        assert back == atom
      end
    end

    property "message returns non-empty string for known codes" do
      all_atoms = ResultCode.all_codes()

      check all(atom <- member_of(all_atoms)) do
        msg = ResultCode.message(atom)
        assert is_binary(msg)
        assert byte_size(msg) > 0
      end
    end
  end

  describe "Field encode/decode" do
    property "roundtrips any field with binary data" do
      check all(
              type <- integer(0..255),
              data <- binary(max_length: 500)
            ) do
        field = %Field{type: type, data: data}
        encoded = Field.encode(field)

        assert {:ok, decoded, <<>>} = Field.decode(encoded)
        assert decoded.type == type
        assert decoded.data == data
      end
    end

    property "encoded size matches header declaration" do
      check all(
              type <- integer(0..255),
              data <- binary(max_length: 500)
            ) do
        field = %Field{type: type, data: data}
        encoded = Field.encode(field)

        # First 4 bytes are size (including type byte)
        <<size::32-big, _rest::binary>> = encoded
        assert size == byte_size(data) + 1
      end
    end
  end

  describe "Operation encode/decode" do
    property "roundtrips any operation" do
      check all(
              op_type <- integer(0..255),
              particle_type <- integer(0..255),
              # Bin names are limited in Aerospike
              bin_name <- string(:alphanumeric, max_length: 15),
              data <- binary(max_length: 500)
            ) do
        op = %Operation{
          op_type: op_type,
          particle_type: particle_type,
          bin_name: bin_name,
          data: data
        }

        encoded = Operation.encode(op)

        assert {:ok, decoded, <<>>} = Operation.decode(encoded)
        assert decoded.op_type == op_type
        assert decoded.particle_type == particle_type
        assert decoded.bin_name == bin_name
        assert decoded.data == data
      end
    end

    property "encoded size matches header declaration" do
      check all(
              op_type <- integer(0..255),
              particle_type <- integer(0..255),
              bin_name <- string(:alphanumeric, max_length: 15),
              data <- binary(max_length: 500)
            ) do
        op = %Operation{
          op_type: op_type,
          particle_type: particle_type,
          bin_name: bin_name,
          data: data
        }

        encoded = Operation.encode(op)

        # First 4 bytes are size
        <<size::32-big, _rest::binary>> = encoded
        assert size == 4 + byte_size(bin_name) + byte_size(data)
      end
    end
  end

  describe "AsmMsg encode/decode" do
    property "roundtrips header fields" do
      check all(
              info1 <- integer(0..255),
              info2 <- integer(0..255),
              info3 <- integer(0..255),
              info4 <- integer(0..255),
              result_code <- integer(0..255),
              generation <- integer(0..0xFFFFFFFF),
              expiration <- integer(0..0xFFFFFFFF),
              timeout <- integer(-2_147_483_648..2_147_483_647)
            ) do
        msg = %AsmMsg{
          info1: info1,
          info2: info2,
          info3: info3,
          info4: info4,
          result_code: result_code,
          generation: generation,
          expiration: expiration,
          timeout: timeout
        }

        encoded = IO.iodata_to_binary(AsmMsg.encode(msg))

        assert {:ok, decoded} = AsmMsg.decode(encoded)
        assert decoded.info1 == info1
        assert decoded.info2 == info2
        assert decoded.info3 == info3
        assert decoded.info4 == info4
        assert decoded.result_code == result_code
        assert decoded.generation == generation
        assert decoded.expiration == expiration
        assert decoded.timeout == timeout
      end
    end

    property "roundtrips message with fields" do
      check all(
              ns <- string(:alphanumeric, min_length: 1, max_length: 20),
              set <- string(:alphanumeric, min_length: 1, max_length: 20)
            ) do
        # Create a valid digest (20 bytes)
        digest = :binary.copy(<<0xAB>>, 20)

        msg = %AsmMsg{
          fields: [
            Field.namespace(ns),
            Field.set(set),
            Field.digest(digest)
          ]
        }

        encoded = IO.iodata_to_binary(AsmMsg.encode(msg))

        assert {:ok, decoded} = AsmMsg.decode(encoded)
        assert length(decoded.fields) == 3

        [ns_field, set_field, digest_field] = decoded.fields
        assert ns_field.data == ns
        assert set_field.data == set
        assert digest_field.data == digest
      end
    end

    property "roundtrips message with operations" do
      check all(
              bin1 <- string(:alphanumeric, min_length: 1, max_length: 14),
              bin2 <- string(:alphanumeric, min_length: 1, max_length: 14),
              str_val <- string(:alphanumeric, max_length: 100),
              int_val <- integer(-1_000_000..1_000_000)
            ) do
        msg = %AsmMsg{
          operations: [
            Operation.write_string(bin1, str_val),
            Operation.write_integer(bin2, int_val)
          ]
        }

        encoded = IO.iodata_to_binary(AsmMsg.encode(msg))

        assert {:ok, decoded} = AsmMsg.decode(encoded)
        assert length(decoded.operations) == 2

        [op1, op2] = decoded.operations
        assert op1.bin_name == bin1
        assert op1.data == str_val
        assert op2.bin_name == bin2
        assert op2.data == <<int_val::64-big-signed>>
      end
    end

    property "encoded message starts with header size 22" do
      check all(
              info1 <- integer(0..255),
              info2 <- integer(0..255)
            ) do
        msg = %AsmMsg{info1: info1, info2: info2}
        encoded = IO.iodata_to_binary(AsmMsg.encode(msg))

        <<header_size::8, _rest::binary>> = encoded
        assert header_size == 22
      end
    end
  end

  describe "Full protocol message" do
    property "Message wrapping AsmMsg roundtrips" do
      check all(
              info1 <- integer(0..255),
              ns <- string(:alphanumeric, min_length: 1, max_length: 10)
            ) do
        digest = :binary.copy(<<0xCD>>, 20)

        asm_msg = %AsmMsg{
          info1: info1,
          fields: [
            Field.namespace(ns),
            Field.digest(digest)
          ]
        }

        # Encode as complete message
        full_msg = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(asm_msg)))

        # Decode message envelope
        assert {:ok, {2, 3, payload}} = Message.decode(full_msg)

        # Decode AS_MSG
        assert {:ok, decoded} = AsmMsg.decode(payload)
        assert decoded.info1 == info1
        assert length(decoded.fields) == 2
      end
    end
  end
end
