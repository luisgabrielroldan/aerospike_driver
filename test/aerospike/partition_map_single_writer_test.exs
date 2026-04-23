defmodule Aerospike.Cluster.PartitionMapSingleWriterTest do
  @moduledoc """
  Architectural guard: every mutation of the partition-map ETS tables
  must flow through `Aerospike.Cluster.PartitionMapWriter`.

  `PartitionMap.update/5`, `PartitionMap.put_node_gen/3`,
  `PartitionMap.drop_node/2`, `PartitionMap.delete_node_gen/2`,
  `RetryPolicy.put/2`, `:ets.insert(meta_tab, {:ready, ...})`, and
  `:ets.insert(meta_tab, {:active_nodes, ...})` may appear only inside
  the writer, except for the TableOwner's one-time table creation seeds
  and the merge module's `PartitionMap.update/5`. Any other call site is
  a regression of the single-writer invariant.
  """
  use ExUnit.Case, async: true

  @lib_dir Path.expand("../../lib/aerospike", __DIR__)
  @cluster_dir Path.join(@lib_dir, "cluster")
  @writer_path Path.join(@cluster_dir, "partition_map_writer.ex")
  @merge_path Path.join(@cluster_dir, "partition_map_merge.ex")
  @table_owner_path Path.join(@cluster_dir, "table_owner.ex")

  # `PartitionMap.update/5` is allowed inside both the merge and the
  # writer; the writer calls merge, not `update` directly, but the merge
  # calls `update` by construction. The other four writer entry points
  # are issued directly from the writer.
  @update_allowed [@merge_path]
  @writer_only_allowed [@writer_path]

  # `:ets.insert(..., {:ready, ...})` is issued by the writer during
  # `recompute_ready` and by the `TableOwner` exactly once, at table
  # creation, to seed the flag as `false`. Any other caller would bypass
  # the writer during a tend cycle.
  @ready_insert_allowed [@writer_path, @table_owner_path]
  @active_nodes_insert_allowed [@writer_path, @table_owner_path]
  @retry_policy_put_allowed [@writer_path, @table_owner_path]

  test "PartitionMap.update/5 callers live only inside merge" do
    callers = scan(~r/PartitionMap\.update\(/)
    assert Enum.sort(callers) == Enum.sort(@update_allowed), caller_report("update/5", callers)
  end

  test "PartitionMap.put_node_gen/3 callers live only inside the writer" do
    callers = scan(~r/PartitionMap\.put_node_gen\(/)

    assert Enum.sort(callers) == Enum.sort(@writer_only_allowed),
           caller_report("put_node_gen/3", callers)
  end

  test "PartitionMap.drop_node/2 callers live only inside the writer" do
    callers = scan(~r/PartitionMap\.drop_node\(/)

    assert Enum.sort(callers) == Enum.sort(@writer_only_allowed),
           caller_report("drop_node/2", callers)
  end

  test "PartitionMap.delete_node_gen/2 callers live only inside the writer" do
    callers = scan(~r/PartitionMap\.delete_node_gen\(/)

    assert Enum.sort(callers) == Enum.sort(@writer_only_allowed),
           caller_report("delete_node_gen/2", callers)
  end

  test ":ets.insert of the :ready meta flag happens only inside the writer" do
    callers = scan(~r/:ets\.insert\([^,]+,\s*\{:ready,/)

    assert Enum.sort(callers) == Enum.sort(@ready_insert_allowed),
           caller_report(":ready insert", callers)
  end

  test "RetryPolicy.put/2 callers live only inside the writer or table owner" do
    callers = scan(~r/RetryPolicy\.put\(/)

    assert Enum.sort(callers) == Enum.sort(@retry_policy_put_allowed),
           caller_report("RetryPolicy.put/2", callers)
  end

  test ":ets.insert of the :active_nodes meta row happens only inside the writer" do
    callers = scan(~r/:ets\.insert\([^,]+,\s*\{:active_nodes,/)

    assert Enum.sort(callers) == Enum.sort(@active_nodes_insert_allowed),
           caller_report(":active_nodes insert", callers)
  end

  defp scan(regex) do
    @lib_dir
    |> Path.join("**/*.ex")
    |> Path.wildcard()
    |> Enum.filter(fn path ->
      path
      |> File.read!()
      |> String.split("\n")
      |> Enum.any?(fn line ->
        trimmed = String.trim_leading(line)
        not String.starts_with?(trimmed, "#") and Regex.match?(regex, line)
      end)
    end)
  end

  defp caller_report(label, callers) do
    "expected single-writer enforcement for #{label}, but got callers: " <>
      inspect(callers)
  end
end
