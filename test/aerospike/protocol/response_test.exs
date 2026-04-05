defmodule Aerospike.Protocol.ResponseTest do
  use ExUnit.Case, async: true

  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Test.Fixtures

  setup do
    key = Key.new("test", "users", "k1")
    {:ok, key: key}
  end

  test "parse_record_response from asm_msg_response_ok fixture", %{key: key} do
    fixture = Fixtures.asm_msg_response_ok()
    <<2, 3, _::binary-6, payload::binary>> = fixture
    assert {:ok, msg} = AsmMsg.decode(payload)

    assert {:ok, record} = Response.parse_record_response(msg, key)
    assert record.bins["name"] == "Alice"
    assert record.bins["count"] == 42
    assert record.generation == 5
    assert record.ttl == 3600
    assert record.key == key
  end

  test "parse_record_response not found", %{key: key} do
    fixture = Fixtures.asm_msg_response_not_found()
    <<2, 3, _::binary-6, payload::binary>> = fixture
    assert {:ok, msg} = AsmMsg.decode(payload)

    assert {:error, e} = Response.parse_record_response(msg, key)
    assert e.code == :key_not_found
  end

  test "parse_record_response unknown result code", %{key: key} do
    msg = %AsmMsg{result_code: 99_999, operations: [], generation: 0, expiration: 0}

    assert {:error, e} = Response.parse_record_response(msg, key)
    assert e.code == :server_error
  end

  test "parse_write_response ok" do
    msg = %AsmMsg{result_code: 0}
    assert Response.parse_write_response(msg) == :ok
  end

  test "parse_write_response error" do
    msg = %AsmMsg{result_code: 9}
    assert {:error, e} = Response.parse_write_response(msg)
    assert e.code == :timeout
  end

  test "parse_delete_response" do
    assert Response.parse_delete_response(%AsmMsg{result_code: 0}) == {:ok, true}
    assert Response.parse_delete_response(%AsmMsg{result_code: 2}) == {:ok, false}
    assert {:error, e} = Response.parse_delete_response(%AsmMsg{result_code: 9})
    assert e.code == :timeout
  end

  test "parse_exists_response" do
    assert Response.parse_exists_response(%AsmMsg{result_code: 0}) == {:ok, true}
    assert Response.parse_exists_response(%AsmMsg{result_code: 2}) == {:ok, false}
    assert {:error, e} = Response.parse_exists_response(%AsmMsg{result_code: 9})
    assert e.code == :timeout
  end

  test "full message decode path for ok response", %{key: key} do
    fixture = Fixtures.asm_msg_response_ok()
    assert {:ok, {2, 3, body}} = Message.decode(fixture)
    assert {:ok, msg} = AsmMsg.decode(body)
    assert {:ok, _} = Response.parse_record_response(msg, key)
  end
end
