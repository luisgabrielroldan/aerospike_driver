defmodule Demo.Examples.Info do
  @moduledoc """
  Demonstrates cluster administration commands: `Aerospike.info/3`,
  `Aerospike.info_node/4`, `Aerospike.nodes/1`, and `Aerospike.node_names/1`.
  """

  require Logger

  @conn :aero

  def run do
    list_nodes()
    query_namespaces()
    query_node_stats()
  end

  defp list_nodes do
    {:ok, nodes} = Aerospike.nodes(@conn)
    Logger.info("  Cluster nodes: #{length(nodes)}")

    for %{name: name, host: host, port: port} <- nodes do
      Logger.info("    #{name} @ #{host}:#{port}")
    end

    {:ok, names} = Aerospike.node_names(@conn)
    Logger.info("  Node names: #{Enum.join(names, ", ")}")
  end

  defp query_namespaces do
    {:ok, response} = Aerospike.info(@conn, "namespaces")
    namespaces = String.split(response, ";", trim: true)
    Logger.info("  Namespaces: #{Enum.join(namespaces, ", ")}")

    unless length(namespaces) > 0 do
      raise "Expected at least one namespace"
    end
  end

  defp query_node_stats do
    {:ok, [%{name: node_name} | _]} = Aerospike.nodes(@conn)

    {:ok, response} = Aerospike.info_node(@conn, node_name, "build")
    Logger.info("  Server build (via info_node): #{response}")
  end
end
