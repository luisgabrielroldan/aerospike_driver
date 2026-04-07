defmodule Aerospike.Integration.MultiNodeTest do
  @moduledoc """
  Integration tests that require a multi-node Aerospike cluster.

  Start with: docker compose --profile cluster up -d

  These tests verify peer discovery, multi-node partition routing, seed
  failover, and cross-node data availability.
  """

  use ExUnit.Case, async: false

  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  @moduletag :cluster

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()

    unless cluster_running?(host, port) do
      flunk("""
      Multi-node cluster not running. Start with:

          docker compose --profile cluster up -d
      """)
    end

    name = :"multi_node_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      tend_interval: 60_000
    ]

    {:ok, _sup} = start_supervised({Aerospike, opts})
    await_cluster_ready(name)

    {:ok, conn: name, host: host, port: port}
  end

  describe "peer discovery" do
    test "discovers all cluster nodes from a single seed", %{conn: conn} do
      nodes = :ets.tab2list(Tables.nodes(conn))
      assert match?([_, _, _ | _], nodes), "expected at least 3 nodes, got #{length(nodes)}"
    end

    test "each discovered node has a running pool", %{conn: conn} do
      nodes = :ets.tab2list(Tables.nodes(conn))

      for {node_name, %{pool_pid: pool_pid}} <- nodes do
        assert is_pid(pool_pid),
               "node #{node_name} should have a pool pid, got: #{inspect(pool_pid)}"

        assert Process.alive?(pool_pid),
               "pool for node #{node_name} should be alive"
      end
    end
  end

  describe "partition map" do
    test "partitions are distributed across multiple nodes", %{conn: conn} do
      partitions = :ets.tab2list(Tables.partitions(conn))
      assert partitions != []

      node_names =
        partitions
        |> Enum.map(fn {_key, node_name} -> node_name end)
        |> Enum.uniq()

      assert match?([_, _ | _], node_names),
             "expected partitions on at least 2 nodes, got: #{inspect(node_names)}"
    end

    test "all 4096 master partitions are covered for test namespace", %{conn: conn} do
      master_partitions =
        :ets.tab2list(Tables.partitions(conn))
        |> Enum.filter(fn
          {{"test", _pid, 0}, _node} -> true
          _ -> false
        end)

      assert length(master_partitions) == 4096,
             "expected 4096 master partitions for 'test' namespace, got #{length(master_partitions)}"
    end

    test "replica partitions exist with replication-factor 2", %{conn: conn} do
      replica_partitions =
        :ets.tab2list(Tables.partitions(conn))
        |> Enum.filter(fn
          {{"test", _pid, replica_index}, _node} -> replica_index == 1
          _ -> false
        end)

      assert length(replica_partitions) == 4096,
             "expected 4096 replica partitions (replication-factor 2), got #{length(replica_partitions)}"
    end
  end

  describe "cross-node routing" do
    test "different keys route to different nodes", %{conn: conn, host: host, port: port} do
      keys = for _ <- 1..50, do: Helpers.unique_key("test", "multi_node_itest")
      on_exit(fn -> Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port)) end)

      node_names =
        Enum.map(keys, fn key ->
          :ok = Aerospike.put(conn, key, %{"v" => 1})
          {:ok, _, node_name} = route_key(conn, key)
          node_name
        end)
        |> Enum.uniq()

      assert match?([_, _ | _], node_names),
             "expected keys to route to at least 2 different nodes, got: #{inspect(node_names)}"
    end

    test "put/get works for keys on each discovered node", %{conn: conn, host: host, port: port} do
      node_names =
        :ets.tab2list(Tables.nodes(conn))
        |> Enum.map(fn {name, _} -> name end)

      keys_by_node = find_keys_for_nodes(conn, node_names)

      for {node_name, key} <- keys_by_node do
        on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

        assert :ok = Aerospike.put(conn, key, %{"node" => node_name}),
               "put should succeed for key routed to #{node_name}"

        assert {:ok, record} = Aerospike.get(conn, key),
               "get should succeed for key routed to #{node_name}"

        assert record.bins["node"] == node_name
      end
    end
  end

  describe "seed failover" do
    test "connects via second seed when first is unreachable", %{host: host, port: port} do
      name = :"failover_itest_#{System.unique_integer([:positive])}"

      opts = [
        name: name,
        hosts: ["127.0.0.1:19999", "#{host}:#{port}"],
        pool_size: 2,
        connect_timeout: 2_000,
        tend_interval: 60_000
      ]

      {:ok, _sup} = start_supervised({Aerospike, opts}, id: :failover_test)
      await_cluster_ready(name)

      nodes = :ets.tab2list(Tables.nodes(name))
      assert nodes != [], "should discover at least 1 node via fallback seed"
    end
  end

  describe "data availability" do
    test "bulk writes and reads across cluster", %{conn: conn, host: host, port: port} do
      count = 100

      keys =
        for i <- 1..count do
          key = Helpers.unique_key("test", "multi_node_itest")
          :ok = Aerospike.put(conn, key, %{"i" => i, "data" => "value_#{i}"})
          {key, i}
        end

      on_exit(fn ->
        Enum.each(keys, fn {key, _} -> Helpers.cleanup_key(key, host: host, port: port) end)
      end)

      for {key, i} <- keys do
        assert {:ok, record} = Aerospike.get(conn, key)
        assert record.bins["i"] == i
        assert record.bins["data"] == "value_#{i}"
      end
    end

    test "delete works for keys on any node", %{conn: conn, host: host, port: port} do
      keys = for _ <- 1..20, do: Helpers.unique_key("test", "multi_node_itest")
      on_exit(fn -> Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port)) end)

      Enum.each(keys, fn key ->
        :ok = Aerospike.put(conn, key, %{"v" => 1})
      end)

      Enum.each(keys, fn key ->
        assert {:ok, true} = Aerospike.delete(conn, key)
        assert {:ok, false} = Aerospike.exists(conn, key)
      end)
    end

    test "exists works for keys on any node", %{conn: conn, host: host, port: port} do
      keys = for _ <- 1..20, do: Helpers.unique_key("test", "multi_node_itest")
      on_exit(fn -> Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port)) end)

      for key <- keys do
        assert {:ok, false} = Aerospike.exists(conn, key)
        :ok = Aerospike.put(conn, key, %{"v" => 1})
        assert {:ok, true} = Aerospike.exists(conn, key)
      end
    end
  end

  # --- helpers ---

  defp route_key(conn, key) do
    partition_id = Aerospike.Key.partition_id(key)

    case :ets.lookup(Tables.partitions(conn), {"test", partition_id, 0}) do
      [{_, node_name}] -> {:ok, partition_id, node_name}
      [] -> {:error, :no_partition}
    end
  end

  defp find_keys_for_nodes(conn, node_names) do
    target_set = MapSet.new(node_names)

    Stream.repeatedly(fn -> Helpers.unique_key("test", "multi_node_itest") end)
    |> Enum.reduce_while({%{}, 0}, fn key, {found, attempts} ->
      if all_nodes_found?(found, target_set) or attempts > 500 do
        {:halt, {found, attempts}}
      else
        {:cont, {maybe_add_key(conn, key, found, target_set), attempts + 1}}
      end
    end)
    |> elem(0)
    |> Map.to_list()
  end

  defp all_nodes_found?(found, target_set) do
    MapSet.subset?(target_set, MapSet.new(Map.keys(found)))
  end

  defp maybe_add_key(conn, key, found, target_set) do
    case route_key(conn, key) do
      {:ok, _pid, node_name} ->
        if MapSet.member?(target_set, node_name) and not Map.has_key?(found, node_name),
          do: Map.put(found, node_name, key),
          else: found

      _ ->
        found
    end
  end

  defp cluster_running?(host, port) do
    case :gen_tcp.connect(~c"#{host}", port, [], 2_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        true

      _ ->
        false
    end
  end

  defp await_cluster_ready(name, timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    poll_until(deadline, timeout, "cluster #{name} not ready", fn ->
      match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key()))
    end)
  end

  defp poll_until(deadline, timeout, message, check_fn) do
    cond do
      check_fn.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("#{message} within #{timeout}ms")

      true ->
        Process.sleep(50)
        poll_until(deadline, timeout, message, check_fn)
    end
  end
end
