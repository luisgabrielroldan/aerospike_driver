defmodule Aerospike.RouterUnitTest do
  use ExUnit.Case, async: false

  alias Aerospike.Key
  alias Aerospike.PartitionFilter
  alias Aerospike.Router
  alias Aerospike.Tables

  setup do
    name = :"router_utest_#{:erlang.unique_integer([:positive])}"

    :ets.new(Tables.nodes(name), [:set, :public, :named_table, read_concurrency: true])
    :ets.new(Tables.partitions(name), [:set, :public, :named_table, read_concurrency: true])
    :ets.new(Tables.meta(name), [:set, :public, :named_table])

    on_exit(fn ->
      for t <- [Tables.nodes(name), Tables.partitions(name), Tables.meta(name)] do
        try do
          :ets.delete(t)
        catch
          :error, :badarg -> :ok
        end
      end
    end)

    {:ok, name: name}
  end

  test "returns cluster_not_ready when meta flag absent", %{name: name} do
    key = Key.new("test", "x", "y")
    assert {:error, %{code: :cluster_not_ready}} = Router.run(name, key, <<>>)
  end

  test "returns invalid_cluster_partition_map when partition missing", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    key = Key.new("test", "x", "y")
    assert {:error, %{code: :invalid_cluster_partition_map}} = Router.run(name, key, <<>>)
  end

  test "returns invalid_node when node missing from nodes table", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    key = Key.new("test", "x", "y")
    pid = Key.partition_id(key)
    :ets.insert(Tables.partitions(name), {{key.namespace, pid, 0}, "missing_node"})
    assert {:error, %{code: :invalid_node}} = Router.run(name, key, <<>>)
  end

  test "returns invalid_node when node entry has nil pool_pid", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    key = Key.new("test", "x", "y")
    pid = Key.partition_id(key)
    :ets.insert(Tables.partitions(name), {{key.namespace, pid, 0}, "node1"})
    :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: nil}})
    assert {:error, %{code: :invalid_node}} = Router.run(name, key, <<>>)
  end

  test "returns invalid_node when pool process is dead", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    key = Key.new("test", "x", "y")
    pid = Key.partition_id(key)

    dead_pid = spawn(fn -> :ok end)
    ref = Process.monitor(dead_pid)
    receive do: ({:DOWN, ^ref, _, _, _} -> :ok)

    :ets.insert(Tables.partitions(name), {{key.namespace, pid, 0}, "node1"})
    :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: dead_pid}})

    assert {:error, %{code: :invalid_node}} =
             Router.run(name, key, <<>>, pool_checkout_timeout: 100)
  end

  test "returns pool_timeout when checkout exceeds timeout", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    key = Key.new("test", "x", "y")
    pid = Key.partition_id(key)

    {:ok, pool} =
      NimblePool.start_link(
        worker: {Aerospike.Test.SlowPoolWorker, []},
        pool_size: 1
      )

    on_exit(fn ->
      try do
        GenServer.stop(pool, :normal, 100)
      catch
        :exit, _ -> :ok
      end
    end)

    :ets.insert(Tables.partitions(name), {{key.namespace, pid, 0}, "node1"})
    :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: pool}})

    assert {:error, %{code: :pool_timeout}} =
             Router.run(name, key, <<>>, pool_checkout_timeout: 1)
  end

  @tag capture_log: true
  test "returns network_error when pool exits with unexpected reason", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    key = Key.new("test", "x", "y")
    pid = Key.partition_id(key)

    {:ok, fake_pool} = Agent.start(fn -> :ok end)

    :ets.insert(Tables.partitions(name), {{key.namespace, pid, 0}, "node1"})
    :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: fake_pool}})

    assert {:error, %{code: :network_error}} =
             Router.run(name, key, <<>>, pool_checkout_timeout: 100)
  end

  @tag capture_log: true
  test "replica :sequence routes like :master for replica index 0", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    key = Key.new("test", "x", "replica_seq")
    pid = Key.partition_id(key)

    :ets.insert(Tables.partitions(name), {{key.namespace, pid, 0}, "master_node"})
    :ets.insert(Tables.nodes(name), {"master_node", %{pool_pid: nil}})

    assert {:error, %{code: :invalid_node}} = Router.run(name, key, <<>>, replica: :sequence)
  end

  @tag capture_log: true
  test "replica atoms :master/:any route to replica index 0", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    key = Key.new("test", "x", "replica_atom_test")
    pid = Key.partition_id(key)

    # Only insert partition for replica index 0
    :ets.insert(Tables.partitions(name), {{key.namespace, pid, 0}, "master_node"})
    :ets.insert(Tables.nodes(name), {"master_node", %{pool_pid: nil}})

    # Both :master and :any should find replica index 0 and hit the pool (which errors on nil pid)
    assert {:error, %{code: :invalid_node}} = Router.run(name, key, <<>>, replica: :master)
    assert {:error, %{code: :invalid_node}} = Router.run(name, key, <<>>, replica: :any)
  end

  test "honours non-zero replica_index", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    key = Key.new("test", "x", "y")
    pid = Key.partition_id(key)

    :ets.insert(Tables.partitions(name), {{key.namespace, pid, 1}, "replica_node"})
    :ets.insert(Tables.nodes(name), {"replica_node", %{pool_pid: nil}})

    assert {:error, %{code: :invalid_cluster_partition_map}} =
             Router.run(name, key, <<>>, replica_index: 0)

    assert {:error, %{code: :invalid_node}} =
             Router.run(name, key, <<>>, replica_index: 1)
  end

  test "group_by_node/3 groups keys by node preserving order", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})

    k1 = Key.new("test", "s", "router-batch-a")
    k2 = Key.new("test", "s", "router-batch-b")
    p1 = Key.partition_id(k1)
    p2 = Key.partition_id(k2)

    {:ok, pool} = Agent.start(fn -> :ok end)

    on_exit(fn ->
      try do
        Agent.stop(pool, :normal, 100)
      catch
        :exit, _ -> :ok
      end
    end)

    :ets.insert(Tables.partitions(name), {{k1.namespace, p1, 0}, "node_batch"})
    :ets.insert(Tables.partitions(name), {{k2.namespace, p2, 0}, "node_batch"})
    :ets.insert(Tables.nodes(name), {"node_batch", %{pool_pid: pool}})

    assert {:ok, %{"node_batch" => %{pool_pid: ^pool, entries: [{0, ^k1}, {1, ^k2}]}}} =
             Router.group_by_node(name, [k1, k2])
  end

  test "expand_partition_filter nil is all partitions", _ctx do
    {full, partial} = Router.expand_partition_filter(nil)
    assert partial == []
    assert full == Enum.to_list(0..4_095)
    assert length(full) == 4_096
  end

  test "expand_partition_filter by_id and by_range", _ctx do
    assert {[42], []} == Router.expand_partition_filter(PartitionFilter.by_id(42))

    assert {Enum.to_list(0..9), []} ==
             Router.expand_partition_filter(PartitionFilter.by_range(0, 10))
  end

  test "expand_partition_filter splits cursor partitions by digest", _ctx do
    d = :crypto.strong_rand_bytes(20)

    pf = %PartitionFilter{
      begin: 0,
      count: 4_096,
      digest: nil,
      partitions: [
        %{id: 1, digest: nil, bval: nil},
        %{id: 2, digest: d, bval: nil},
        %{id: 3, digest: nil}
      ]
    }

    assert {[1, 3], [%{id: 2, digest: ^d, bval: nil}]} = Router.expand_partition_filter(pf)
  end

  test "group_partitions_by_node all partitions on one node", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    ns = "test_scan"
    {:ok, pool} = Agent.start(fn -> :ok end)

    on_exit(fn ->
      try do
        Agent.stop(pool, :normal, 100)
      catch
        :exit, _ -> :ok
      end
    end)

    for i <- 0..4_095 do
      :ets.insert(Tables.partitions(name), {{ns, i, 0}, "node1"})
    end

    :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: pool}})

    ids = Enum.to_list(0..4_095)

    assert {:ok, %{"node1" => %{pool_pid: ^pool, parts_full: ^ids, parts_partial: []}}} =
             Router.group_partitions_by_node(name, ns, ids, 0)
  end

  test "group_partitions_by_node two nodes even/odd", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    ns = "test_scan2"
    {:ok, pool_a} = Agent.start(fn -> :ok end)
    {:ok, pool_b} = Agent.start(fn -> :ok end)

    on_exit(fn ->
      for p <- [pool_a, pool_b] do
        try do
          Agent.stop(p, :normal, 100)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    for i <- 0..4_095 do
      node = if rem(i, 2) == 0, do: "node_a", else: "node_b"
      :ets.insert(Tables.partitions(name), {{ns, i, 0}, node})
    end

    :ets.insert(Tables.nodes(name), {"node_a", %{pool_pid: pool_a}})
    :ets.insert(Tables.nodes(name), {"node_b", %{pool_pid: pool_b}})

    ids = Enum.to_list(0..4_095)
    {:ok, groups} = Router.group_partitions_by_node(name, ns, ids, 0)

    evens = Enum.filter(ids, &(rem(&1, 2) == 0))
    odds = Enum.filter(ids, &(rem(&1, 2) == 1))

    assert %{
             "node_a" => %{pool_pid: ^pool_a, parts_full: ^evens, parts_partial: []},
             "node_b" => %{pool_pid: ^pool_b, parts_full: ^odds, parts_partial: []}
           } = groups
  end

  test "group_partitions_by_node subset of partitions only", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    ns = "test_scan3"
    {:ok, pool} = Agent.start(fn -> :ok end)

    on_exit(fn ->
      try do
        Agent.stop(pool, :normal, 100)
      catch
        :exit, _ -> :ok
      end
    end)

    for i <- [0, 1, 2, 99] do
      :ets.insert(Tables.partitions(name), {{ns, i, 0}, "node1"})
    end

    :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: pool}})

    assert {:ok, %{"node1" => %{pool_pid: ^pool, parts_full: [0, 1, 2], parts_partial: []}}} =
             Router.group_partitions_by_node(name, ns, [0, 1, 2], 0)
  end

  test "group_partitions_by_node errors when partition missing from ETS", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    ns = "test_scan4"
    {:ok, pool} = Agent.start(fn -> :ok end)

    on_exit(fn ->
      try do
        Agent.stop(pool, :normal, 100)
      catch
        :exit, _ -> :ok
      end
    end)

    :ets.insert(Tables.partitions(name), {{ns, 0, 0}, "node1"})
    :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: pool}})

    assert {:error, %{code: :invalid_cluster_partition_map}} =
             Router.group_partitions_by_node(name, ns, [0, 1], 0)
  end

  test "group_partitions_by_node returns cluster_not_ready when meta flag absent", %{name: name} do
    assert {:error, %{code: :cluster_not_ready}} =
             Router.group_partitions_by_node(name, "ns", [0], 0)
  end

  test "random_node_pool/1 returns cluster_not_ready when meta flag absent", %{name: name} do
    assert {:error, %{code: :cluster_not_ready}} = Router.random_node_pool(name)
  end

  test "random_node_pool/1 returns invalid_node with empty nodes table", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    assert {:error, %{code: :invalid_node}} = Router.random_node_pool(name)
  end

  test "random_node_pool/1 skips inactive nodes", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    :ets.insert(Tables.nodes(name), {"inactive_node", %{pool_pid: self(), active: false}})
    assert {:error, %{code: :invalid_node}} = Router.random_node_pool(name)
  end

  test "random_node_pool/1 returns an active node pool", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    fake_pid = self()
    :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: fake_pid, active: true}})
    assert {:ok, ^fake_pid, "node1"} = Router.random_node_pool(name)
  end

  test "node_pool/2 returns cluster_not_ready when meta flag absent", %{name: name} do
    assert {:error, %{code: :cluster_not_ready}} = Router.node_pool(name, "node1")
  end

  test "node_pool/2 returns invalid_node when node not in ETS", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    assert {:error, %{code: :invalid_node}} = Router.node_pool(name, "missing")
  end

  test "node_pool/2 returns pool for existing node", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
    fake_pid = self()
    :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: fake_pid, active: true}})
    assert {:ok, ^fake_pid, "node1"} = Router.node_pool(name, "node1")
  end
end
