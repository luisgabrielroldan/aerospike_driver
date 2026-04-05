defmodule Aerospike.Protocol.AsmMsg.FieldTest do
  use ExUnit.Case, async: true
  doctest Aerospike.Protocol.AsmMsg.Field

  alias Aerospike.Protocol.AsmMsg.Field

  describe "field type constants" do
    test "returns correct namespace type" do
      assert Field.type_namespace() == 0
    end

    test "returns correct table/set type" do
      assert Field.type_table() == 1
    end

    test "returns correct key type" do
      assert Field.type_key() == 2
    end

    test "returns correct digest type" do
      assert Field.type_digest() == 4
    end

    test "returns correct filter_exp type" do
      assert Field.type_filter_exp() == 43
    end

    test "returns expected values for all field type accessors" do
      assert Field.type_record_version() == 3
      assert Field.type_mrt_id() == 5
      assert Field.type_mrt_deadline() == 6
      assert Field.type_query_id() == 7
      assert Field.type_socket_timeout() == 9
      assert Field.type_records_per_second() == 10
      assert Field.type_pid_array() == 11
      assert Field.type_digest_array() == 12
      assert Field.type_max_records() == 13
      assert Field.type_bval_array() == 15
      assert Field.type_index_name() == 21
      assert Field.type_index_range() == 22
      assert Field.type_index_context() == 23
      assert Field.type_index_expression() == 24
      assert Field.type_index_type() == 26
      assert Field.type_udf_package_name() == 30
      assert Field.type_udf_function() == 31
      assert Field.type_udf_arglist() == 32
      assert Field.type_udf_op() == 33
      assert Field.type_query_binlist() == 40
      assert Field.type_batch_index() == 41
      assert Field.type_batch_index_with_set() == 42
    end
  end

  describe "encode/1" do
    test "encodes empty field" do
      field = %Field{type: 0, data: <<>>}
      encoded = Field.encode(field)
      # size=1 (data_len=0 + 1), type=0, no data
      assert encoded == <<0, 0, 0, 1, 0>>
    end

    test "encodes field with data" do
      field = %Field{type: 0, data: "test"}
      encoded = Field.encode(field)
      # size=5 (4 + 1), type=0, data="test"
      assert encoded == <<0, 0, 0, 5, 0, "test">>
    end

    test "encodes digest field (20 bytes)" do
      digest = :binary.copy(<<0xAB>>, 20)
      field = %Field{type: 4, data: digest}
      encoded = Field.encode(field)
      # size=21 (20 + 1), type=4, data=digest
      assert encoded == <<0, 0, 0, 21, 4>> <> digest
    end
  end

  describe "decode/1" do
    test "decodes empty field" do
      binary = <<0, 0, 0, 1, 0>>
      assert {:ok, field, <<>>} = Field.decode(binary)
      assert field.type == 0
      assert field.data == <<>>
    end

    test "decodes field with data" do
      binary = <<0, 0, 0, 5, 0, "test">>
      assert {:ok, field, <<>>} = Field.decode(binary)
      assert field.type == 0
      assert field.data == "test"
    end

    test "decodes field with remaining bytes" do
      binary = <<0, 0, 0, 5, 0, "test", "remaining">>
      assert {:ok, field, "remaining"} = Field.decode(binary)
      assert field.type == 0
      assert field.data == "test"
    end

    test "returns error for incomplete header" do
      assert {:error, :incomplete_field_header} = Field.decode(<<0, 0>>)
    end

    test "returns error for invalid field size (zero)" do
      assert {:error, :invalid_field_size} = Field.decode(<<0, 0, 0, 0, 0>>)
    end

    test "returns error for incomplete data" do
      # Header says 10 bytes but only 2 provided
      assert {:error, :incomplete_field} = Field.decode(<<0, 0, 0, 10, 0, "ab">>)
    end
  end

  describe "encode/decode roundtrip" do
    test "roundtrips namespace field" do
      field = Field.namespace("test")
      assert {:ok, decoded, <<>>} = field |> Field.encode() |> Field.decode()
      assert decoded.type == Field.type_namespace()
      assert decoded.data == "test"
    end

    test "roundtrips set field" do
      field = Field.set("users")
      assert {:ok, decoded, <<>>} = field |> Field.encode() |> Field.decode()
      assert decoded.type == Field.type_table()
      assert decoded.data == "users"
    end

    test "roundtrips digest field" do
      digest = :binary.copy(<<0xAB>>, 20)
      field = Field.digest(digest)
      assert {:ok, decoded, <<>>} = field |> Field.encode() |> Field.decode()
      assert decoded.type == Field.type_digest()
      assert decoded.data == digest
    end

    test "roundtrips key field" do
      # String key with particle type 3
      field = Field.key(3, "user_123")
      assert {:ok, decoded, <<>>} = field |> Field.encode() |> Field.decode()
      assert decoded.type == Field.type_key()
      # Data includes particle type byte + value
      assert decoded.data == <<3, "user_123">>
    end
  end

  describe "constructor functions" do
    test "namespace/1 creates correct field" do
      field = Field.namespace("test_ns")
      assert field.type == 0
      assert field.data == "test_ns"
    end

    test "set/1 creates correct field" do
      field = Field.set("test_set")
      assert field.type == 1
      assert field.data == "test_set"
    end

    test "digest/1 creates correct field" do
      digest = :binary.copy(<<0xFF>>, 20)
      field = Field.digest(digest)
      assert field.type == 4
      assert field.data == digest
    end

    test "key/2 creates correct field with particle type" do
      field = Field.key(1, <<123::64-big>>)
      assert field.type == 2
      assert field.data == <<1, 123::64-big>>
    end
  end
end
