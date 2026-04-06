defmodule Aerospike.Protocol.ResponseFieldsTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Response

  describe "Field.mrt_id/1" do
    test "encodes positive transaction ID as 8-byte little-endian" do
      field = Field.mrt_id(1)
      assert field.type == Field.type_mrt_id()
      assert byte_size(field.data) == 8
      assert field.data == <<1, 0, 0, 0, 0, 0, 0, 0>>
    end

    test "encodes large transaction ID" do
      txn_id = 0x0102030405060708
      field = Field.mrt_id(txn_id)
      assert field.type == Field.type_mrt_id()
      assert field.data == <<0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01>>
    end

    test "encodes negative transaction ID (signed little-endian)" do
      field = Field.mrt_id(-1)
      assert byte_size(field.data) == 8
      assert field.data == <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>
    end

    test "round-trips: encoded mrt_id can be decoded back" do
      txn_id = 9_876_543_210
      field = Field.mrt_id(txn_id)
      <<decoded::64-signed-little>> = field.data
      assert decoded == txn_id
    end
  end

  describe "Field.mrt_deadline/1" do
    test "encodes deadline as 4-byte little-endian" do
      field = Field.mrt_deadline(1)
      assert field.type == Field.type_mrt_deadline()
      assert byte_size(field.data) == 4
      assert field.data == <<1, 0, 0, 0>>
    end

    test "encodes deadline with known byte layout" do
      field = Field.mrt_deadline(0x01020304)
      assert field.data == <<0x04, 0x03, 0x02, 0x01>>
    end

    test "encodes negative deadline" do
      field = Field.mrt_deadline(-1)
      assert byte_size(field.data) == 4
      assert field.data == <<0xFF, 0xFF, 0xFF, 0xFF>>
    end

    test "round-trips: encoded deadline can be decoded back" do
      deadline = 3600
      field = Field.mrt_deadline(deadline)
      <<decoded::32-signed-little>> = field.data
      assert decoded == deadline
    end
  end

  describe "Field.record_version/1" do
    test "encodes version as 7 bytes" do
      field = Field.record_version(1)
      assert field.type == Field.type_record_version()
      assert byte_size(field.data) == 7
      assert field.data == <<1, 0, 0, 0, 0, 0, 0>>
    end

    test "encodes version with known byte layout" do
      field = Field.record_version(0x0102030405060708)
      assert field.data == <<0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02>>
    end

    test "round-trips: encoded version can be decoded back" do
      version = 0x00AABBCCDDEEFF
      field = Field.record_version(version)
      <<decoded::56-little-unsigned>> = field.data
      assert decoded == version
    end

    test "upper byte is dropped (7-byte wire format)" do
      # Bit 56 and above are dropped
      v_with_upper = 0x01_00_00_00_00_00_00_00
      v_lower_only = 0x00_00_00_00_00_00_00_00
      assert Field.record_version(v_with_upper).data == Field.record_version(v_lower_only).data
    end
  end

  describe "Response.extract_record_version/1" do
    test "returns :none when no fields" do
      msg = %AsmMsg{fields: []}
      assert Response.extract_record_version(msg) == :none
    end

    test "returns :none when RECORD_VERSION field is absent" do
      msg = %AsmMsg{fields: [Field.namespace("test")]}
      assert Response.extract_record_version(msg) == :none
    end

    test "extracts version from fields" do
      version = 42
      msg = %AsmMsg{fields: [Field.record_version(version)]}
      assert {:ok, ^version} = Response.extract_record_version(msg)
    end

    test "extracts version when mixed with other fields" do
      version = 0x00AABBCCDDEEFF

      msg = %AsmMsg{
        fields: [Field.namespace("ns"), Field.record_version(version), Field.set("s")]
      }

      assert {:ok, ^version} = Response.extract_record_version(msg)
    end

    test "round-trips with Field.record_version constructor" do
      version = 9_999_999
      msg = %AsmMsg{fields: [Field.record_version(version)]}
      assert {:ok, ^version} = Response.extract_record_version(msg)
    end
  end

  describe "Response.extract_mrt_deadline/1" do
    test "returns :none when no fields" do
      msg = %AsmMsg{fields: []}
      assert Response.extract_mrt_deadline(msg) == :none
    end

    test "returns :none when MRT_DEADLINE field is absent" do
      msg = %AsmMsg{fields: [Field.namespace("test")]}
      assert Response.extract_mrt_deadline(msg) == :none
    end

    test "extracts deadline from fields" do
      deadline = 3600
      msg = %AsmMsg{fields: [Field.mrt_deadline(deadline)]}
      assert {:ok, ^deadline} = Response.extract_mrt_deadline(msg)
    end

    test "extracts negative deadline" do
      deadline = -1
      msg = %AsmMsg{fields: [Field.mrt_deadline(deadline)]}
      assert {:ok, ^deadline} = Response.extract_mrt_deadline(msg)
    end

    test "extracts deadline when mixed with other fields" do
      deadline = 7200
      msg = %AsmMsg{fields: [Field.namespace("ns"), Field.mrt_deadline(deadline)]}
      assert {:ok, ^deadline} = Response.extract_mrt_deadline(msg)
    end

    test "round-trips with Field.mrt_deadline constructor" do
      deadline = 86_400
      msg = %AsmMsg{fields: [Field.mrt_deadline(deadline)]}
      assert {:ok, ^deadline} = Response.extract_mrt_deadline(msg)
    end
  end
end
