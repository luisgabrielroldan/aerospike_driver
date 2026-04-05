defmodule Aerospike.RouterUnitTest do
  use ExUnit.Case, async: false

  alias Aerospike.Key
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
end
