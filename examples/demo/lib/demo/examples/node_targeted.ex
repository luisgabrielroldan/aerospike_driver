defmodule Demo.Examples.NodeTargeted do
  @moduledoc """
  Demonstrates node-targeted scan and query execution with `node: node_name`.

  Discovers an active node name from the cluster, then sends scan and
  secondary-index query requests to that node through the regular facade
  options.
  """

  require Logger

  alias Aerospike.Filter
  alias Aerospike.Query
  alias Aerospike.Scan

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_node_targeted"
  @index_name "demo_node_targeted_score_idx"
  @score_bin "score"
  @size 24

  def run do
    cleanup_stale()
    write_records()
    node_name = discover_node_name()
    scan_target_node(node_name)
    query_target_node(node_name)
    verify_unknown_node_rejected()
    cleanup()
  end

  defp cleanup_stale do
    @repo.drop_index(@namespace, @index_name)

    for i <- 1..@size do
      @repo.delete(Aerospike.key(@namespace, @set, "nt_#{i}"))
    end
  end

  defp write_records do
    Logger.info("  Writing #{@size} records for node-targeted scan/query...")

    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "nt_#{i}")
      bins = %{"name" => "node_targeted_#{i}", @score_bin => i}
      :ok = @repo.put!(key, bins)
    end
  end

  defp discover_node_name do
    {:ok, [node_name | _]} = @repo.node_names()
    Logger.info("  Targeting active node #{node_name}.")
    node_name
  end

  defp scan_target_node(node_name) do
    Logger.info("  Counting set records through node-targeted scan...")

    scan = Scan.new(@namespace, @set)
    {:ok, count} = @repo.count(scan, node: node_name)

    if count < 0 do
      raise "Expected a non-negative scan count, got #{count}"
    end

    Logger.info("    Node-targeted scan count returned #{count}.")
  end

  defp query_target_node(node_name) do
    create_index()

    Logger.info("  Querying indexed records through node-targeted query...")

    query =
      Query.new(@namespace, @set)
      |> Query.where(Filter.range(@score_bin, 5, 20))
      |> Query.max_records(@size)

    {:ok, records} = @repo.all(query, node: node_name)

    for record <- records do
      score = record.bins[@score_bin]

      unless score >= 5 and score <= 20 do
        raise "Expected #{@score_bin} 5..20, got #{score}"
      end
    end

    Logger.info("    Node-targeted query returned #{length(records)} indexed records.")
  end

  defp create_index do
    Logger.info("  Creating numeric index on '#{@score_bin}'...")

    {:ok, task} =
      @repo.create_index(@namespace, @set,
        bin: @score_bin,
        name: @index_name,
        type: :numeric
      )

    :ok = Aerospike.IndexTask.wait(task, timeout: 15_000)
    Process.sleep(500)
    Logger.info("  Index ready.")
  end

  defp verify_unknown_node_rejected do
    scan = Scan.new(@namespace, @set)

    case @repo.count(scan, node: "missing-demo-node") do
      {:error, %Aerospike.Error{code: :invalid_node}} ->
        Logger.info("  Unknown node names are rejected.")

      other ->
        raise "Expected invalid_node for unknown scan target, got #{inspect(other)}"
    end
  end

  defp cleanup do
    @repo.drop_index(@namespace, @index_name)

    for i <- 1..@size do
      @repo.delete(Aerospike.key(@namespace, @set, "nt_#{i}"))
    end
  end
end
