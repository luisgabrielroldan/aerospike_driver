defmodule Aerospike.ClusterTest do
  use ExUnit.Case, async: true

  alias Aerospike.Cluster
  alias Aerospike.Cluster.PartitionMap
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Key
  alias Aerospike.RetryPolicy

  setup context do
    name = :"cluster_#{:erlang.phash2(context.test)}"
    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)

    on_exit(fn ->
      stop_process(owner)
    end)

    %{name: name, owner: owner, tables: tables}
  end

  describe "tables/1" do
    test "resolves the published table names from the cluster name", ctx do
      assert Cluster.tables(ctx.name) == ctx.tables
    end

    test "resolves the published table names from a registered cluster pid", ctx do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      true = Process.register(pid, ctx.name)

      on_exit(fn ->
        Process.exit(pid, :kill)
      end)

      assert Cluster.tables(pid) == ctx.tables
    end

    test "rejects opaque pids that are not registered cluster identities" do
      pid = spawn(fn -> Process.sleep(:infinity) end)

      on_exit(fn ->
        Process.exit(pid, :kill)
      end)

      assert_raise ArgumentError,
                   ~r/cluster identity must be an atom or a pid registered under one/,
                   fn ->
                     Cluster.tables(pid)
                   end
    end
  end

  describe "ready?/1" do
    test "reads the published ready flag directly from meta", ctx do
      refute Cluster.ready?(ctx.name)

      :ets.insert(ctx.tables.meta, {:ready, true})

      assert Cluster.ready?(ctx.name)
    end
  end

  describe "retry_policy/1" do
    test "reads the published retry policy from meta", ctx do
      policy = %{max_retries: 5, sleep_between_retries_ms: 25, replica_policy: :master}
      assert RetryPolicy.put(ctx.tables.meta, policy)

      assert Cluster.retry_policy(ctx.name) == policy
    end
  end

  describe "active_nodes/1, node_names/1, and active_node?/2" do
    test "reads the published active node snapshot", ctx do
      :ets.insert(ctx.tables.meta, {:active_nodes, ["A1", "B1"]})

      assert Cluster.active_nodes(ctx.name) == ["A1", "B1"]
      assert Cluster.node_names(ctx.name) == ["A1", "B1"]
      assert Cluster.active_node?(ctx.name, "A1")
      refute Cluster.active_node?(ctx.name, "C1")
    end
  end

  describe "route_for_write/2" do
    test "delegates master routing through Router", ctx do
      key = Key.new("test", "set", 1)
      seed_partition(ctx.tables, key, ["A1", "B1"])
      :ets.insert(ctx.tables.meta, {:ready, true})

      assert {:ok, "A1"} = Cluster.route_for_write(ctx.name, key)
    end

    test "preserves :cluster_not_ready routing refusals", ctx do
      key = Key.new("test", "set", 1)
      seed_partition(ctx.tables, key, ["A1"])

      assert {:error, :cluster_not_ready} = Cluster.route_for_write(ctx.name, key)
    end
  end

  describe "route_for_read/4" do
    test "delegates sequence routing through Router", ctx do
      key = Key.new("test", "set", 1)
      seed_partition(ctx.tables, key, ["A1", "B1", "C1"])
      :ets.insert(ctx.tables.meta, {:ready, true})

      assert {:ok, "A1"} = Cluster.route_for_read(ctx.name, key, :sequence, 0)
      assert {:ok, "B1"} = Cluster.route_for_read(ctx.name, key, :sequence, 1)
      assert {:ok, "C1"} = Cluster.route_for_read(ctx.name, key, :sequence, 2)
    end

    test "preserves :no_master when the partition has no live replica", ctx do
      key = Key.new("test", "set", 1)
      seed_partition(ctx.tables, key, [nil, nil])
      :ets.insert(ctx.tables.meta, {:ready, true})

      assert {:error, :no_master} = Cluster.route_for_read(ctx.name, key, :sequence, 0)
    end
  end

  defp seed_partition(%{owners: owners}, key, replicas, regime \\ 1) do
    :ok = PartitionMap.update(owners, key.namespace, Key.partition_id(key), regime, replicas)
  end

  defp stop_process(pid) do
    if Process.alive?(pid) do
      try do
        GenServer.stop(pid)
      catch
        :exit, {:noproc, _} -> :ok
        :exit, :noproc -> :ok
      end
    else
      :ok
    end
  end
end
