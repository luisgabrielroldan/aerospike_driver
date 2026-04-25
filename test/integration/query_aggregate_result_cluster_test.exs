defmodule Aerospike.Integration.QueryAggregateResultClusterTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :cluster

  alias Aerospike.Cluster.Router
  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Filter
  alias Aerospike.IndexTask
  alias Aerospike.Key
  alias Aerospike.Query
  alias Aerospike.RegisterTask
  alias Aerospike.Test.IntegrationSupport

  @seeds [{"localhost", 3000}, {"localhost", 3010}, {"localhost", 3020}]
  @namespace "test"
  @fixture Path.expand("../support/fixtures/aggregate_udf.lua", __DIR__)

  setup do
    IntegrationSupport.probe_aerospike!(
      @seeds,
      "Run `docker compose --profile cluster up -d aerospike aerospike2 aerospike3` in `aerospike_driver_spike/` first."
    )

    IntegrationSupport.wait_for_cluster_ready!(@seeds, @namespace, 15_000)

    name = IntegrationSupport.unique_atom("spike_query_aggregate_result_cluster")
    set = IntegrationSupport.unique_name("agg_result_cluster")
    index_name = IntegrationSupport.unique_name("agg_result_cluster_age_idx")
    package = IntegrationSupport.unique_name("agg_result_cluster_udf")
    server_name = "#{package}.lua"

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

    assert {:ok, register_task} = Aerospike.register_udf(name, @fixture, server_name)
    assert :ok = RegisterTask.wait(register_task, timeout: 10_000, poll_interval: 200)

    assert {:ok, index_task} =
             Aerospike.create_index(name, @namespace, set,
               bin: "age",
               name: index_name,
               type: :numeric
             )

    assert :ok = IndexTask.wait(index_task, timeout: 30_000, poll_interval: 200)

    on_exit(fn ->
      if Process.whereis(name) do
        cleanup_cluster(name, set, index_name, server_name)
      end

      IntegrationSupport.stop_supervisor_quietly(sup)
    end)

    %{cluster: name, set: set, package: package}
  end

  test "combines aggregate partials emitted by multiple cluster nodes", %{
    cluster: cluster,
    set: set,
    package: package
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    [{_node_a, key_a}, {_node_b, key_b}, {_node_c, key_c}] =
      keys_for_distinct_nodes(cluster, set, 3)

    assert {:ok, _metadata} = Aerospike.put(cluster, key_a, %{"age" => 101, "label" => "a"})
    assert {:ok, _metadata} = Aerospike.put(cluster, key_b, %{"age" => 202, "label" => "b"})
    assert {:ok, _metadata} = Aerospike.put(cluster, key_c, %{"age" => 303, "label" => "c"})

    query = age_query(set, 100, 400)

    partials =
      IntegrationSupport.eventually!("aggregate query emits multiple partial values", fn ->
        case aggregate_partials(cluster, query, package) do
          partials when length(partials) > 1 ->
            if Enum.sum(partials) == 606 do
              {:ok, partials}
            else
              :retry
            end

          _partials ->
            :retry
        end
      end)

    assert length(partials) > 1

    assert {:ok, 606} =
             Aerospike.query_aggregate_result(cluster, query, package, "sum_age", ["age"],
               source_path: @fixture,
               timeout: 15_000
             )
  end

  test "maps local reduction runtime failures after multiple server partials", %{
    cluster: cluster,
    set: set,
    package: package
  } do
    [{_node_a, key_a}, {_node_b, key_b}] = keys_for_distinct_nodes(cluster, set, 2)

    assert {:ok, _metadata} = Aerospike.put(cluster, key_a, %{"age" => 11, "label" => "a"})
    assert {:ok, _metadata} = Aerospike.put(cluster, key_b, %{"age" => 22, "label" => "b"})

    query = age_query(set, 10, 30)

    IntegrationSupport.assert_eventually(
      "aggregate query emits values from more than one node",
      fn ->
        aggregate_partials(cluster, query, package) |> length() > 1
      end
    )

    assert {:error, %Error{code: :query_generic, message: message}} =
             Aerospike.query_aggregate_result(cluster, query, package, "client_failure", [],
               source_path: @fixture,
               timeout: 15_000
             )

    assert message =~ "client reduction failed"
  end

  defp aggregate_partials(cluster, query, package) do
    case Aerospike.query_aggregate(cluster, query, package, "sum_age", ["age"], timeout: 15_000) do
      {:ok, stream} -> Enum.to_list(stream)
      {:error, %Error{}} -> []
    end
  end

  defp keys_for_distinct_nodes(cluster, set, count) when is_integer(count) and count > 0 do
    tables = Tender.tables(cluster)

    0..50_000
    |> Enum.reduce_while(%{}, fn suffix, acc ->
      key = Key.new(@namespace, set, "aggregate-#{suffix}")

      case Router.pick_for_read(tables, key, :master, 0) do
        {:ok, node_name} ->
          halt_if_complete(Map.put_new(acc, node_name, key), count)

        {:error, reason} ->
          flunk(
            "expected a routed key while building aggregate query proof, got #{inspect(reason)}"
          )
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

  defp halt_if_complete(node_keys, count) when map_size(node_keys) == count do
    {:halt, node_keys}
  end

  defp halt_if_complete(node_keys, _count), do: {:cont, node_keys}

  defp age_query(set, min, max) do
    Query.new(@namespace, set)
    |> Query.where(Filter.range("age", min, max))
  end

  defp cleanup_cluster(cluster, set, index_name, server_name) do
    safe_cleanup(fn -> Aerospike.truncate(cluster, @namespace, set) end)
    safe_cleanup(fn -> Aerospike.drop_index(cluster, @namespace, index_name) end)
    safe_cleanup(fn -> Aerospike.remove_udf(cluster, server_name) end)
  end

  defp safe_cleanup(fun) when is_function(fun, 0) do
    _ = fun.()
    :ok
  catch
    :exit, _reason -> :ok
  end
end
