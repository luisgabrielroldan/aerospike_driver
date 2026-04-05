defmodule Aerospike.Protocol.InfoTest do
  use ExUnit.Case, async: true
  doctest Aerospike.Protocol.Info

  alias Aerospike.Protocol.Info
  alias Aerospike.Protocol.Message

  describe "encode_request/1" do
    test "encodes single command" do
      encoded = Info.encode_request(["namespaces"])
      {:ok, {2, 1, body}} = Message.decode(encoded)
      assert body == "namespaces\n"
    end

    test "encodes multiple commands joined with newlines" do
      encoded = Info.encode_request(["node", "build", "version"])
      {:ok, {2, 1, body}} = Message.decode(encoded)
      assert body == "node\nbuild\nversion\n"
    end

    test "encodes empty command list as empty message" do
      encoded = Info.encode_request([])
      {:ok, {2, 1, body}} = Message.decode(encoded)
      assert body == ""
    end

    test "creates info message type (1)" do
      encoded = Info.encode_request(["test"])
      {:ok, {_version, type, _body}} = Message.decode(encoded)
      assert type == Message.type_info()
    end
  end

  describe "decode_response/1" do
    test "parses single key-value pair" do
      assert Info.decode_response("namespaces\ttest\n") ==
               {:ok, %{"namespaces" => "test"}}
    end

    test "parses multiple key-value pairs" do
      response = "node\tBB9050011AC4202\nbuild\t7.0.0.0\n"

      assert Info.decode_response(response) ==
               {:ok, %{"node" => "BB9050011AC4202", "build" => "7.0.0.0"}}
    end

    test "parses key-only entries (no tab separator)" do
      assert Info.decode_response("status\n") ==
               {:ok, %{"status" => ""}}
    end

    test "parses mixed key-value and key-only entries" do
      response = "node\tBB9050\nstatus\nbuild\t7.0.0\n"

      assert Info.decode_response(response) ==
               {:ok, %{"node" => "BB9050", "status" => "", "build" => "7.0.0"}}
    end

    test "parses empty response" do
      assert Info.decode_response("") == {:ok, %{}}
    end

    test "handles value with semicolons (common in Aerospike responses)" do
      response = "namespaces\tns1;ns2;ns3\n"

      assert Info.decode_response(response) ==
               {:ok, %{"namespaces" => "ns1;ns2;ns3"}}
    end

    test "handles value with colons (common in partition info)" do
      response = "replicas\tns:0,1,2,3:node1;ns:4,5,6,7:node2\n"

      assert Info.decode_response(response) ==
               {:ok, %{"replicas" => "ns:0,1,2,3:node1;ns:4,5,6,7:node2"}}
    end

    test "handles value with equals signs" do
      response = "statistics\tmem=1024;disk=2048\n"

      assert Info.decode_response(response) ==
               {:ok, %{"statistics" => "mem=1024;disk=2048"}}
    end

    test "handles multiple tabs in value (only splits on first tab)" do
      response = "data\tvalue\twith\ttabs\n"

      assert Info.decode_response(response) ==
               {:ok, %{"data" => "value\twith\ttabs"}}
    end
  end

  describe "decode_message/1" do
    test "decodes complete info message" do
      msg = Message.encode_info("namespaces\ttest\n")
      assert Info.decode_message(msg) == {:ok, %{"namespaces" => "test"}}
    end

    test "returns error for non-info message type" do
      msg = Message.encode_as_msg("some data")
      assert Info.decode_message(msg) == {:error, :invalid_message_type}
    end

    test "returns error for incomplete header" do
      assert Info.decode_message(<<1, 2, 3>>) == {:error, :incomplete_header}
    end

    test "returns error for incomplete body" do
      # Header says 100 bytes, but only 4 provided
      msg = <<2, 1, 0, 0, 0, 0, 0, 100, "test">>
      assert Info.decode_message(msg) == {:error, :incomplete_body}
    end
  end

  describe "request/response roundtrip" do
    test "encode request then decode matching response" do
      _request = Info.encode_request(["namespaces", "build"])
      # Simulate server response
      response_body = "namespaces\ttest\nbuild\t7.0.0.0\n"
      response_msg = Message.encode_info(response_body)

      assert Info.decode_message(response_msg) ==
               {:ok, %{"namespaces" => "test", "build" => "7.0.0.0"}}
    end
  end
end
