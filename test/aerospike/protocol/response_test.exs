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
  end
end
