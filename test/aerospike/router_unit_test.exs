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
end
