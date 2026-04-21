defmodule Aerospike.Protocol.ResponseTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Response
  alias Aerospike.Record

  @namespace "test"
  @set "spike"
  @user_key "k1"

  describe "parse_record_response/2" do
    test "result_code 0 builds a Record" do
      msg = %AsmMsg{result_code: 0, generation: 3, expiration: 0, operations: []}
      key = Key.new(@namespace, @set, @user_key)

      assert {:ok, %Record{key: ^key, generation: 3, bins: %{}}} =
               Response.parse_record_response(msg, key)
    end

    test "result_code 11 (PARTITION_UNAVAILABLE) becomes a rebalance-class Error" do
      msg = %AsmMsg{result_code: 11}
      key = Key.new(@namespace, @set, @user_key)

      assert {:error, %Error{code: :partition_unavailable} = err} =
               Response.parse_record_response(msg, key)

      assert Error.rebalance?(err),
             "the rebalance-class classification is the retry layer's re-route cue"
    end

    test "result_code 2 (KEY_NOT_FOUND) is a non-rebalance Error" do
      msg = %AsmMsg{result_code: 2}
      key = Key.new(@namespace, @set, @user_key)

      assert {:error, %Error{code: :key_not_found} = err} =
               Response.parse_record_response(msg, key)

      refute Error.rebalance?(err)
    end

    test "result_code 9 (TIMEOUT) is a non-rebalance Error" do
      msg = %AsmMsg{result_code: 9}
      key = Key.new(@namespace, @set, @user_key)

      assert {:error, %Error{code: :timeout} = err} =
               Response.parse_record_response(msg, key)

      refute Error.rebalance?(err)
    end

    test "unknown result code surfaces as :server_error with a descriptive message" do
      # Pick an integer that is not mapped in Protocol.ResultCode.
      msg = %AsmMsg{result_code: 250}
      key = Key.new(@namespace, @set, @user_key)

      assert {:error, %Error{code: :server_error, message: msg_text}} =
               Response.parse_record_response(msg, key)

      assert msg_text =~ "unknown result code 250"
    end

    test "repeated bin reads preserve the record's multi-value shape" do
      msg =
        %AsmMsg{
          result_code: 0,
          operations: [
            %AsmMsg.Operation{
              op_type: 1,
              particle_type: 1,
              bin_name: "count",
              data: <<1::64-signed-big>>
            },
            %AsmMsg.Operation{
              op_type: 1,
              particle_type: 1,
              bin_name: "count",
              data: <<2::64-signed-big>>
            }
          ]
        }

      key = Key.new(@namespace, @set, @user_key)

      assert {:ok, %Record{bins: %{"count" => [1, 2]}}} =
               Response.parse_record_response(msg, key)
    end
  end

  describe "decode_as_msg/1" do
    test "surfaces parse errors as Aerospike.Error structs" do
      assert {:error, %Error{code: :parse_error, message: message}} =
               Response.decode_as_msg(<<1, 2, 3>>)

      assert message =~ "failed to decode AS_MSG reply"
    end
  end

  describe "parse_record_metadata_response/1" do
    test "returns generation and ttl for successful header/write replies" do
      msg = %AsmMsg{result_code: 0, generation: 9, expiration: 1_234}

      assert {:ok, %{generation: 9, ttl: 1_234}} =
               Response.parse_record_metadata_response(msg)
    end

    test "preserves server errors for missing records" do
      msg = %AsmMsg{result_code: 2}

      assert {:error, %Error{code: :key_not_found}} =
               Response.parse_record_metadata_response(msg)
    end
  end

  describe "extract_record_version/1" do
    test "returns the record version when present" do
      msg = %AsmMsg{
        fields: [
          %AsmMsg.Field{
            type: Aerospike.Protocol.AsmMsg.Field.type_record_version(),
            data: <<42::56-little-unsigned>>
          }
        ]
      }

      assert {:ok, 42} = Response.extract_record_version(msg)
    end

    test "returns :none when the field is absent" do
      assert :none = Response.extract_record_version(%AsmMsg{})
    end
  end

  describe "record-shaped operate replies" do
    test "decode into a Record without exposing request-order slots" do
      msg =
        %AsmMsg{
          result_code: 0,
          generation: 4,
          expiration: 20,
          operations: [
            %AsmMsg.Operation{
              op_type: 2,
              particle_type: 0,
              bin_name: "",
              data: <<>>
            },
            %AsmMsg.Operation{
              op_type: 1,
              particle_type: 1,
              bin_name: "count",
              data: <<7::64-signed-big>>
            }
          ]
        }

      key = Key.new(@namespace, @set, @user_key)

      assert {:ok, %Record{key: ^key, generation: 4, ttl: 20, bins: %{"count" => 7}}} =
               Response.parse_record_response(msg, key)
    end
  end
end
