defmodule Demo.Examples.Info do
  @moduledoc """
  Demonstrates cluster administration commands:
  `Demo.PrimaryClusterRepo.info/1`, `info_node/2`, `nodes/0`, and `node_names/0`.
  """

  require Logger

  @repo Demo.PrimaryClusterRepo

  def run do
    list_nodes()
    query_namespaces()
    query_node_stats()
  end

  defp list_nodes do
    {:ok, nodes} = @repo.nodes()
    Logger.info("  Cluster nodes: #{length(nodes)}")

    for %{name: name, host: host, port: port} <- nodes do
      Logger.info("    #{name} @ #{host}:#{port}")
    end

    {:ok, names} = @repo.node_names()
    Logger.info("  Node names: #{Enum.join(names, ", ")}")
  end

  defp query_namespaces do
    {:ok, response} = @repo.info("namespaces")
    namespaces = String.split(response, ";", trim: true)
    Logger.info("  Namespaces: #{Enum.join(namespaces, ", ")}")

    unless length(namespaces) > 0 do
      raise "Expected at least one namespace"
    end
  end

  defp query_node_stats do
    {:ok, [%{name: node_name} | _]} = @repo.nodes()

    {:ok, response} = @repo.info_node(node_name, "build")
    Logger.info("  Server build (via info_node): #{response}")
  end
end
