defmodule Aerospike.Protocol.Peers do
  @moduledoc false
  # Parses the `peers-clear-std` info response used for cluster node discovery.
  # The server returns a generation counter, default port, and a nested list of
  # peer entries. The Cluster module uses this to discover and verify new nodes.

  # Matches individual peer entries: [node_name,tls_name,[addr1,addr2,...]]
  # Captures: 1=node_name, 2=tls_name (may be empty), 3=comma-separated addresses.
  @peer_regex ~r/\[([^,\[\]]+),([^,\[\]]*),\[([^\]]*)\]\]/

  @typedoc """
  One peer from `peers-clear-std`.

  Field `node_name` is the cluster node identifier (ETS key in the nodes table). It is not
  renamed to `name`; public APIs use `t:Aerospike.node_info/0` with a `name` field instead.
  """
  @type peer :: %{node_name: String.t(), host: String.t(), port: :inet.port_number()}

  @type parse_result :: %{
          generation: non_neg_integer(),
          default_port: :inet.port_number(),
          peers: [peer()]
        }

  @doc """
  Parses `peers-clear-std` info value.

  Format: `generation,default_port,[[node_name,tls_name,[addr1,addr2,...]],...]`

  Empty peer list: `0,3000,[]`
  """
  @spec parse_peers_clear_std(String.t()) :: {:ok, parse_result()} | :error
  def parse_peers_clear_std(s) when is_binary(s) do
    s = String.trim(s)

    case String.split(s, ",", parts: 3) do
      [gen_s, port_s, rest] ->
        with {gen, ""} <- Integer.parse(gen_s),
             {port, ""} <- Integer.parse(port_s),
             true <- gen >= 0 and port > 0 do
          peers = parse_peer_entries(rest, port)
          {:ok, %{generation: gen, default_port: port, peers: peers}}
        else
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp parse_peer_entries("[]", _default_port), do: []
  defp parse_peer_entries("", _default_port), do: []

  # Scans the nested bracket structure with regex rather than writing a recursive
  # parser — the format is regular enough that this is reliable.
  defp parse_peer_entries(rest, default_port) do
    @peer_regex
    |> Regex.scan(rest)
    |> Enum.flat_map(fn
      [_, node_name, _tls, addrs_str] ->
        case first_address(addrs_str, default_port) do
          {:ok, host, port} -> [%{node_name: node_name, host: host, port: port}]
          :error -> []
        end

      _ ->
        []
    end)
  end

  # Uses only the first address from the peer's address list.
  # Peers may advertise multiple addresses (e.g. for different networks).
  defp first_address("", _default_port), do: :error

  defp first_address(addrs_str, default_port) do
    addr =
      addrs_str
      |> String.split(",", trim: true)
      |> List.first()

    case addr do
      nil ->
        :error

      addr ->
        {host, port} = parse_host_port(addr, default_port)
        {:ok, host, port}
    end
  end

  # Splits "host:port"; falls back to default_port when the port is missing or invalid.
  defp parse_host_port(addr, default_port) do
    case String.split(addr, ":", parts: 2) do
      [host, port_s] ->
        case Integer.parse(port_s) do
          {port, ""} when port > 0 -> {host, port}
          _ -> {addr, default_port}
        end

      [host] ->
        {host, default_port}
    end
  end

  @doc """
  Parses a simple integer info field (e.g. `peers-generation`, `partition-generation`).
  """
  @spec parse_generation(String.t()) :: {:ok, non_neg_integer()} | :error
  def parse_generation(s) when is_binary(s) do
    case Integer.parse(String.trim(s)) do
      {n, ""} when n >= 0 -> {:ok, n}
      _ -> :error
    end
  end
end
