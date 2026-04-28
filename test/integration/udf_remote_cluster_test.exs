defmodule Aerospike.Integration.UdfRemoteClusterTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :cluster

  alias Aerospike.Cluster.Router
  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.ExecuteTask
  alias Aerospike.Filter
  alias Aerospike.IndexTask
  alias Aerospike.Key
  alias Aerospike.Query
  alias Aerospike.RegisterTask
  alias Aerospike.Test.IntegrationSupport

  @seeds [{"localhost", 3000}, {"localhost", 3010}, {"localhost", 3020}]
  @namespace "test"

  @source """
  function put_value(rec, bin_name, value)
    rec[bin_name] = value
    aerospike:update(rec)
    return rec[bin_name]
  end
  """

  setup do
    IntegrationSupport.probe_aerospike!(
      @seeds,
      "Run `docker compose --profile cluster up -d aerospike aerospike2 aerospike3` in `aerospike_driver/` first."
    )

    IntegrationSupport.wait_for_cluster_ready!(@seeds, @namespace, 60_000)

    name = IntegrationSupport.unique_atom("spike_udf_remote_cluster")

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: Enum.map(@seeds, fn {host, port} -> "#{host}:#{port}" end),
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 2
      )

    IntegrationSupport.wait_for_tender_ready!(name, 5_000)

    on_exit(fn ->
      IntegrationSupport.stop_supervisor_quietly(sup)
    end)

    %{cluster: name}
  end

  test "Lua record UDFs work through remote cluster routing and node targeting", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    set = IntegrationSupport.unique_name("udf_remote_cluster")
    index_name = IntegrationSupport.unique_name("udf_remote_bucket_idx")
    package = IntegrationSupport.unique_name("udf_remote_pkg")
    server_name = "#{package}.lua"

    try do
      assert {:ok, register_task} = Aerospike.register_udf(cluster, @source, server_name)
      assert :ok = RegisterTask.wait(register_task, timeout: 15_000, poll_interval: 200)

      node_keys = keys_for_distinct_nodes(cluster, set, 3)

      for {node_name, key} <- node_keys do
        assert {:ok, _metadata} =
                 Aerospike.put(cluster, key, %{
                   "bucket" => 1,
                   "owner_node" => node_name,
                   "state" => "seed"
                 })
      end

      for {node_name, key} <- node_keys do
        value = "apply:#{node_name}"

        assert {:ok, ^value} =
                 Aerospike.apply_udf(cluster, key, package, "put_value", ["state", value])

        assert_state(cluster, key, value)
      end

      batch_keys = node_keys |> Enum.reverse() |> Enum.map(fn {_node_name, key} -> key end)

      assert {:ok, batch_results} =
               Aerospike.batch_udf(cluster, batch_keys, package, "put_value", ["state", "batch"],
                 timeout: 15_000
               )

      assert Enum.map(batch_results, & &1.key) == batch_keys
      assert Enum.all?(batch_results, &match?(%{status: :ok, error: nil}, &1))

      for {_node_name, key} <- node_keys do
        assert_state(cluster, key, "batch")
      end

      create_index!(cluster, set, index_name)

      assert {:ok, %ExecuteTask{} = all_nodes_task} =
               Aerospike.query_udf(
                 cluster,
                 query(set, index_name),
                 package,
                 "put_value",
                 ["state", "query"],
                 timeout: 15_000,
                 task_timeout: 30_000
               )

      assert all_nodes_task.kind == :query_udf
      assert all_nodes_task.node_name == nil
      assert :ok = ExecuteTask.wait(all_nodes_task, timeout: 30_000, poll_interval: 200)

      for {_node_name, key} <- node_keys do
        assert_state_eventually(cluster, key, "query")
      end

      for {_node_name, key} <- node_keys do
        assert {:ok, "batch"} =
                 Aerospike.apply_udf(cluster, key, package, "put_value", ["state", "batch"])
      end

      {target_node, target_key} = hd(node_keys)

      assert {:ok, %ExecuteTask{} = target_task} =
               Aerospike.query_udf(
                 cluster,
                 query(set, index_name),
                 package,
                 "put_value",
                 ["state", "target"],
                 node: target_node,
                 timeout: 15_000,
                 task_timeout: 30_000
               )

      assert target_task.kind == :query_udf
      assert target_task.node_name == target_node
      assert :ok = ExecuteTask.wait(target_task, timeout: 30_000, poll_interval: 200)

      assert_state_eventually(cluster, target_key, "target")

      for {node_name, key} <- tl(node_keys) do
        assert node_name != target_node
        assert_state(cluster, key, "batch")
      end
    after
      safe_cleanup(fn -> Aerospike.truncate(cluster, @namespace, set) end)
      safe_cleanup(fn -> Aerospike.drop_index(cluster, @namespace, index_name) end)
      safe_cleanup(fn -> Aerospike.remove_udf(cluster, server_name) end)
    end
  end

  defp create_index!(cluster, set, index_name) do
    assert {:ok, index_task} =
             Aerospike.create_index(cluster, @namespace, set,
               bin: "bucket",
               name: index_name,
               type: :numeric
             )

    assert :ok = IndexTask.wait(index_task, timeout: 30_000, poll_interval: 200)
  end

  defp query(set, index_name) do
    Query.new(@namespace, set)
    |> Query.where(Filter.range("bucket", 1, 1) |> Filter.using_index(index_name))
  end

  defp assert_state(cluster, key, value) do
    assert {:ok, record} = Aerospike.get(cluster, key)
    assert record.bins["state"] == value
  end

  defp assert_state_eventually(cluster, key, value) do
    IntegrationSupport.assert_eventually(
      "expected #{inspect(key)} state to become #{inspect(value)}",
      fn ->
        case Aerospike.get(cluster, key) do
          {:ok, record} -> record.bins["state"] == value
          {:error, %Error{}} -> false
        end
      end
    )
  end

  defp keys_for_distinct_nodes(cluster, set, count) when is_integer(count) and count > 0 do
    tables = Tender.tables(cluster)

    0..50_000
    |> Enum.reduce_while(%{}, fn suffix, acc ->
      key = Key.new(@namespace, set, "udf-remote-#{suffix}")

      case Router.pick_for_read(tables, key, :master, 0) do
        {:ok, node_name} ->
          acc
          |> Map.put_new(node_name, key)
          |> maybe_halt_distinct_node_search(count)

        {:error, reason} ->
          flunk("expected a routed key while building UDF cluster proof, got #{inspect(reason)}")
      end
    end)
    |> case do
      node_keys when map_size(node_keys) == count ->
        Enum.sort_by(node_keys, fn {node_name, _key} -> node_name end)

      node_keys ->
        flunk(
          "expected #{count} routed nodes from the live cluster, found #{map_size(node_keys)}"
        )
    end
  end

  defp maybe_halt_distinct_node_search(node_keys, count) do
    if map_size(node_keys) == count do
      {:halt, node_keys}
    else
      {:cont, node_keys}
    end
  end

  defp safe_cleanup(fun) when is_function(fun, 0) do
    _ = fun.()
    :ok
  catch
    :exit, _reason -> :ok
  end
end
