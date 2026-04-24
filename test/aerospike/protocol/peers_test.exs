defmodule Aerospike.Protocol.PeersTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.Peers

  test "parse_peers_clear_std/1 parses first addresses and falls back to the default port" do
    assert {:ok,
            %{
              generation: 7,
              default_port: 3000,
              peers: [
                %{node_name: "A1", host: "10.0.0.1", port: 3100},
                %{node_name: "B1", host: "example.com:bad", port: 3000},
                %{node_name: "C1", host: "host-only", port: 3000}
              ]
            }} =
             Peers.parse_peers_clear_std(
               "7,3000,[[A1,,[10.0.0.1:3100,10.0.0.2:3101]],[B1,tls,[example.com:bad]],[C1,,[host-only]]]"
             )
  end

  test "parse_peers_clear_std/1 accepts empty peer lists and rejects malformed headers" do
    assert {:ok, %{generation: 0, default_port: 3000, peers: []}} =
             Peers.parse_peers_clear_std("0,3000,[]")

    assert {:ok, %{generation: 1, default_port: 3000, peers: []}} =
             Peers.parse_peers_clear_std("1,3000,")

    assert :error = Peers.parse_peers_clear_std("bad")
    assert :error = Peers.parse_peers_clear_std("-1,3000,[]")
    assert :error = Peers.parse_peers_clear_std("1,0,[]")
  end

  test "parse_peers_clear_std/1 trims input and skips peers with empty address lists" do
    assert {:ok, %{generation: 2, default_port: 3000, peers: [%{node_name: "A1"}]}} =
             Peers.parse_peers_clear_std(" 2,3000,[[A1,,[10.0.0.1]],[B1,,[]]] \n")
  end

  test "parse_peers_clear_std/1 skips peers whose first address collapses to nil" do
    assert {:ok, %{generation: 3, default_port: 3000, peers: []}} =
             Peers.parse_peers_clear_std("3,3000,[[A1,,[,]]]")
  end

  test "parse_generation/1 accepts trimmed non-negative integers only" do
    assert Peers.parse_generation(" 42 \n") == {:ok, 42}
    assert Peers.parse_generation("-1") == :error
    assert Peers.parse_generation("4x") == :error
  end
end
