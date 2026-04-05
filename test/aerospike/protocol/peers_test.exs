defmodule Aerospike.Protocol.PeersTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.Peers

  describe "parse_peers_clear_std/1" do
    test "parses empty peer list" do
      assert {:ok, %{generation: 0, default_port: 3000, peers: []}} =
               Peers.parse_peers_clear_std("0,3000,[]")
    end

    test "parses single peer" do
      input = "2,3000,[[BB9DF1DA14658E6,,[192.168.147.4]]]"

      assert {:ok, %{generation: 2, default_port: 3000, peers: [peer]}} =
               Peers.parse_peers_clear_std(input)

      assert peer.node_name == "BB9DF1DA14658E6"
      assert peer.host == "192.168.147.4"
      assert peer.port == 3000
    end

    test "parses multiple peers" do
      input = "4,3000,[[BB9DF1DA14658E6,,[192.168.147.4]],[BB98ACF6384DABA,,[192.168.147.3]]]"

      assert {:ok, %{generation: 4, default_port: 3000, peers: peers}} =
               Peers.parse_peers_clear_std(input)

      assert length(peers) == 2
      assert Enum.any?(peers, &(&1.node_name == "BB9DF1DA14658E6"))
      assert Enum.any?(peers, &(&1.node_name == "BB98ACF6384DABA"))
    end

    test "peer with explicit port overrides default" do
      input = "1,3000,[[NODE1,,[10.0.0.1:4000]]]"

      assert {:ok, %{peers: [peer]}} = Peers.parse_peers_clear_std(input)
      assert peer.host == "10.0.0.1"
      assert peer.port == 4000
    end

    test "peer without port uses default" do
      input = "1,3000,[[NODE1,,[10.0.0.1]]]"

      assert {:ok, %{peers: [peer]}} = Peers.parse_peers_clear_std(input)
      assert peer.host == "10.0.0.1"
      assert peer.port == 3000
    end

    test "returns error for invalid input" do
      assert :error = Peers.parse_peers_clear_std("invalid")
      assert :error = Peers.parse_peers_clear_std("abc,def,[]")
    end

    test "returns error for negative generation" do
      assert :error = Peers.parse_peers_clear_std("-1,3000,[]")
    end

    test "returns error for zero port" do
      assert :error = Peers.parse_peers_clear_std("0,0,[]")
    end

    test "peer with empty address list yields no peers" do
      input = "1,3000,[[NODE1,,[]]]"
      assert {:ok, %{peers: []}} = Peers.parse_peers_clear_std(input)
    end

    test "peer with invalid port falls back to default with full addr as host" do
      input = "1,3000,[[NODE1,,[10.0.0.1:abc]]]"
      assert {:ok, %{peers: [peer]}} = Peers.parse_peers_clear_std(input)
      assert peer.host == "10.0.0.1:abc"
      assert peer.port == 3000
    end

    test "peer with multiple addresses uses the first" do
      input = "1,3000,[[NODE1,,[10.0.0.1:4000,10.0.0.2:5000]]]"
      assert {:ok, %{peers: [peer]}} = Peers.parse_peers_clear_std(input)
      assert peer.host == "10.0.0.1"
      assert peer.port == 4000
    end

    test "peer with comma-only address list yields no peers" do
      input = "1,3000,[[NODE1,,[,]]]"
      assert {:ok, %{peers: []}} = Peers.parse_peers_clear_std(input)
    end

    test "trims whitespace from input" do
      assert {:ok, %{generation: 0, peers: []}} =
               Peers.parse_peers_clear_std("  0,3000,[]  \n")
    end

    test "empty rest after default_port yields no peers" do
      assert {:ok, %{generation: 1, default_port: 3000, peers: []}} =
               Peers.parse_peers_clear_std("1,3000,")
    end
  end

  describe "parse_generation/1" do
    test "parses valid generation" do
      assert Peers.parse_generation("7") == {:ok, 7}
      assert Peers.parse_generation("0") == {:ok, 0}
    end

    test "returns error for invalid input" do
      assert Peers.parse_generation("not_int") == :error
      assert Peers.parse_generation("-1") == :error
    end
  end
end
