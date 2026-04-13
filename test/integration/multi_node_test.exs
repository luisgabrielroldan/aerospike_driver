defmodule Aerospike.Integration.MultiNodeTest do
  @moduledoc """
  Integration tests that require a multi-node Aerospike cluster.

  Start with: docker compose --profile cluster up -d

  These tests verify peer discovery, multi-node partition routing, seed
  failover, and cross-node data availability.
  """

  use ExUnit.Case, async: false

  import Aerospike.Op

  alias Aerospike.ExecuteTask
  alias Aerospike.Filter
  alias Aerospike.Page
  alias Aerospike.Query
  alias Aerospike.Scan
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

  describe "node-targeted scan/query APIs" do
    test "scan node wrappers stay on the selected node", %{conn: conn, host: host, port: port} do
      node_names =
        :ets.tab2list(Tables.nodes(conn))
        |> Enum.map(fn {name, _} -> name end)

      set_name = "scan_node_itest_#{System.unique_integer([:positive])}"

      keys_by_node =
        conn
        |> find_keys_for_nodes(node_names, set_name)
        |> Map.new()

      assert map_size(keys_by_node) == length(node_names),
             "expected one routed key per node, got #{inspect(keys_by_node)}"

      for {node_name, key} <- keys_by_node do
        on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)
        assert :ok = Aerospike.put(conn, key, %{"owner_node" => node_name, "kind" => "scan"})
      end

      target_node = hd(node_names)

      scan =
        Scan.new("test", set_name)
        |> Scan.max_records(10)

      assert {:ok, records} = Aerospike.scan_all_node(conn, target_node, scan)
      assert_records_belong_to_node(conn, records, target_node)

      assert {:ok, 1} = Aerospike.scan_count_node(conn, target_node, scan)

      assert {:ok, %Page{records: page_records, cursor: nil, done?: true}} =
               Aerospike.scan_page_node(conn, target_node, scan)

      assert_records_belong_to_node(conn, page_records, target_node)

      stream_records =
        Aerospike.scan_stream_node!(conn, target_node, scan)
        |> Enum.to_list()

      assert_records_belong_to_node(conn, stream_records, target_node)
    end

    test "query node wrappers stay on the selected node", %{conn: conn, host: host, port: port} do
      node_names =
        :ets.tab2list(Tables.nodes(conn))
        |> Enum.map(fn {name, _} -> name end)

      set_name = "query_node_itest_#{System.unique_integer([:positive])}"
      index_name = "qni_#{System.unique_integer([:positive])}"

      keys_by_node =
        conn
        |> find_keys_for_nodes(node_names, set_name)
        |> Map.new()

      assert map_size(keys_by_node) == length(node_names),
             "expected one routed key per node, got #{inspect(keys_by_node)}"

      for {node_name, key} <- keys_by_node do
        on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)
        assert :ok = Aerospike.put(conn, key, %{"owner_node" => node_name, "bucket" => 1})
      end

      on_exit(fn ->
        if :ets.whereis(Tables.meta(conn)) != :undefined do
          _ = Aerospike.drop_index(conn, "test", index_name)
        end
      end)

      assert {:ok, task} =
               Aerospike.create_index(conn, "test", set_name,
                 bin: "bucket",
                 name: index_name,
                 type: :numeric
               )

      assert :ok = Aerospike.IndexTask.wait(task, timeout: 30_000, poll_interval: 200)
      Process.sleep(500)

      target_node = hd(node_names)

      query =
        Query.new("test", set_name)
        |> Query.where(Filter.range("bucket", 1, 1) |> Filter.using_index(index_name))
        |> Query.max_records(10)

      assert {:ok, records} = Aerospike.query_all_node(conn, target_node, query)
      assert_records_belong_to_node(conn, records, target_node)

      assert {:ok, 1} = Aerospike.query_count_node(conn, target_node, query)

      assert {:ok, %Page{records: page_records, cursor: nil, done?: true}} =
               Aerospike.query_page_node(conn, target_node, query)

      assert_records_belong_to_node(conn, page_records, target_node)

      assert {:ok, stream} = Aerospike.query_stream_node(conn, target_node, query)

      stream_records = Enum.to_list(stream)

      assert_records_belong_to_node(conn, stream_records, target_node)
    end

    test "query_execute_node stays on the selected node and returns a waitable task", %{
      conn: conn,
      host: host,
      port: port
    } do
      node_names =
        :ets.tab2list(Tables.nodes(conn))
        |> Enum.map(fn {name, _} -> name end)

      set_name = "query_execute_node_itest_#{System.unique_integer([:positive])}"
      index_name = "qeni_#{System.unique_integer([:positive])}"

      keys_by_node = seed_node_owned_query_records(conn, node_names, set_name, host, port)
      target_node = hd(node_names)
      query = indexed_bucket_query(set_name, index_name)

      create_numeric_index!(conn, set_name, index_name)

      assert {:ok, %ExecuteTask{} = task} =
               Aerospike.query_execute_node(conn, target_node, query, [put("state", "executed")])

      assert task.kind == :query_execute
      assert task.node_name == target_node
      assert :ok = ExecuteTask.wait(task, timeout: 30_000, poll_interval: 200)
      assert {:ok, :complete} = ExecuteTask.status(task)

      assert_only_target_node_changed(conn, keys_by_node, target_node, "executed")
    end

    test "query_udf_node stays on the selected node and returns a waitable task", %{
      conn: conn,
      host: host,
      port: port
    } do
      node_names =
        :ets.tab2list(Tables.nodes(conn))
        |> Enum.map(fn {name, _} -> name end)

      set_name = "query_udf_node_itest_#{System.unique_integer([:positive])}"
      index_name = "quni_#{System.unique_integer([:positive])}"
      package = "query_udf_node_#{System.unique_integer([:positive])}"
      server_name = "#{package}.lua"

      keys_by_node = seed_node_owned_query_records(conn, node_names, set_name, host, port)
      target_node = hd(node_names)
      query = indexed_bucket_query(set_name, index_name)

      create_numeric_index!(conn, set_name, index_name)
      register_put_value_udf!(conn, package, server_name)

      assert {:ok, %ExecuteTask{} = task} =
               Aerospike.query_udf_node(conn, target_node, query, package, "put_value", [
                 "state",
                 "udf"
               ])

      assert task.kind == :query_udf
      assert task.node_name == target_node
      assert :ok = ExecuteTask.wait(task, timeout: 30_000, poll_interval: 200)
      assert {:ok, :complete} = ExecuteTask.status(task)

      assert_only_target_node_changed(conn, keys_by_node, target_node, "udf")
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

  defp find_keys_for_nodes(conn, node_names, set_name \\ "multi_node_itest") do
    target_set = MapSet.new(node_names)

    Stream.repeatedly(fn -> Helpers.unique_key("test", set_name) end)
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

  defp assert_records_belong_to_node(conn, records, target_node) do
    assert length(records) == 1

    Enum.each(records, fn record ->
      assert record.bins["owner_node"] == target_node
      assert {:ok, _partition_id, ^target_node} = route_key(conn, record.key)
    end)
  end

  defp seed_node_owned_query_records(conn, node_names, set_name, host, port) do
    keys_by_node =
      conn
      |> find_keys_for_nodes(node_names, set_name)
      |> Map.new()

    assert map_size(keys_by_node) == length(node_names),
           "expected one routed key per node, got #{inspect(keys_by_node)}"

    for {node_name, key} <- keys_by_node do
      on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

      assert :ok =
               Aerospike.put(conn, key, %{
                 "owner_node" => node_name,
                 "bucket" => 1,
                 "state" => "new"
               })
    end

    keys_by_node
  end

  defp indexed_bucket_query(set_name, index_name) do
    Query.new("test", set_name)
    |> Query.where(Filter.range("bucket", 1, 1) |> Filter.using_index(index_name))
    |> Query.max_records(10)
  end

  defp create_numeric_index!(conn, set_name, index_name) do
    on_exit(fn ->
      if :ets.whereis(Tables.meta(conn)) != :undefined do
        _ = Aerospike.drop_index(conn, "test", index_name)
      end
    end)

    assert {:ok, task} =
             Aerospike.create_index(conn, "test", set_name,
               bin: "bucket",
               name: index_name,
               type: :numeric
             )

    assert :ok = Aerospike.IndexTask.wait(task, timeout: 30_000, poll_interval: 200)
    Process.sleep(500)
  end

  defp register_put_value_udf!(conn, package, server_name) do
    udf_source = """
    function put_value(rec, bin_name, value)
      rec[bin_name] = value
      aerospike:update(rec)
      return value
    end
    """

    on_exit(fn ->
      if :ets.whereis(Tables.meta(conn)) != :undefined do
        _ = Aerospike.remove_udf(conn, server_name)
      end
    end)

    assert {:ok, task} = Aerospike.register_udf(conn, udf_source, server_name)
    assert :ok = Aerospike.RegisterTask.wait(task, timeout: 10_000, poll_interval: 200)
    package
  end

  defp assert_only_target_node_changed(conn, keys_by_node, target_node, target_state) do
    Enum.each(keys_by_node, fn {node_name, key} ->
      assert {:ok, record} = Aerospike.get(conn, key)
      expected_state = if node_name == target_node, do: target_state, else: "new"
      assert record.bins["owner_node"] == node_name
      assert record.bins["state"] == expected_state
    end)
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
