defmodule Aerospike.Integration.BatchGetTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :cluster

  alias Aerospike.Cluster.Router
  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Op
  alias Aerospike.Record
  alias Aerospike.Test.IntegrationSupport

  @seeds [{"localhost", 3000}, {"localhost", 3010}, {"localhost", 3020}]
  @namespace "test"
  setup do
    IntegrationSupport.probe_aerospike!(
      @seeds,
      "Run `docker compose --profile cluster up -d aerospike aerospike2 aerospike3` in `aerospike_driver/` first."
    )

    IntegrationSupport.wait_for_cluster_ready!(@seeds, @namespace, 60_000)

    name = IntegrationSupport.unique_atom("spike_batch_get_cluster")

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
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    %{cluster: name}
  end

  test "batch_get returns caller-ordered results across multiple live cluster nodes", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    set = IntegrationSupport.unique_name("spike_batch")
    [{node_a, key_a}, {node_b, key_b}, {node_c, key_c}] = keys_for_distinct_nodes(cluster, set, 3)
    missing_key = key_for_node(cluster, set, node_a, "missing")

    assert Enum.sort([node_a, node_b, node_c]) |> Enum.uniq() |> length() == 3

    assert {:ok, %{generation: generation_a}} =
             Aerospike.put(cluster, key_a, %{"node" => node_a, "value" => 11})

    assert {:ok, %{generation: generation_b}} =
             Aerospike.put(cluster, key_b, %{"node" => node_b, "value" => 22})

    assert {:ok, %{generation: generation_c}} =
             Aerospike.put(cluster, key_c, %{"node" => node_c, "value" => 33})

    assert generation_a >= 1
    assert generation_b >= 1
    assert generation_c >= 1

    assert {:ok, [first, second, third, fourth]} =
             Aerospike.batch_get(cluster, [key_b, missing_key, key_a, key_c], :all,
               timeout: 5_000
             )

    assert {:ok, %Record{key: ^key_b, bins: %{"node" => ^node_b, "value" => 22}}} = first
    assert {:error, %Error{code: :key_not_found}} = second
    assert {:ok, %Record{key: ^key_a, bins: %{"node" => ^node_a, "value" => 11}}} = third
    assert {:ok, %Record{key: ^key_c, bins: %{"node" => ^node_c, "value" => 33}}} = fourth
  end

  test "batch_get returns only requested bins across multiple live cluster nodes", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    set = IntegrationSupport.unique_name("spike_batch_bins")
    [{node_a, key_a}, {node_b, key_b}, {node_c, key_c}] = keys_for_distinct_nodes(cluster, set, 3)
    missing_key = key_for_node(cluster, set, node_a, "missing")

    assert {:ok, _} =
             Aerospike.put(cluster, key_a, %{"node" => node_a, "value" => 11, "hidden" => "a"})

    assert {:ok, _} =
             Aerospike.put(cluster, key_b, %{"node" => node_b, "value" => 22, "hidden" => "b"})

    assert {:ok, _} =
             Aerospike.put(cluster, key_c, %{"node" => node_c, "value" => 33, "hidden" => "c"})

    assert {:ok, [first, second, third, fourth]} =
             Aerospike.batch_get(cluster, [key_b, missing_key, key_a, key_c], [:node, "value"],
               timeout: 5_000
             )

    assert {:ok, %Record{key: ^key_b, bins: %{"node" => ^node_b, "value" => 22}}} = first
    assert {:error, %Error{code: :key_not_found}} = second
    assert {:ok, %Record{key: ^key_a, bins: %{"node" => ^node_a, "value" => 11}}} = third
    assert {:ok, %Record{key: ^key_c, bins: %{"node" => ^node_c, "value" => 33}}} = fourth
  end

  test "batch_get_header returns header-only records with empty bins in caller order", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    set = IntegrationSupport.unique_name("spike_batch_header")
    [{node_a, key_a}, {node_b, key_b}, {node_c, key_c}] = keys_for_distinct_nodes(cluster, set, 3)
    missing_key = key_for_node(cluster, set, node_a, "missing")

    assert {:ok, _} = Aerospike.put(cluster, key_a, %{"node" => node_a, "value" => 11})
    assert {:ok, _} = Aerospike.put(cluster, key_b, %{"node" => node_b, "value" => 22})
    assert {:ok, _} = Aerospike.put(cluster, key_c, %{"node" => node_c, "value" => 33})

    assert {:ok, [first, second, third, fourth]} =
             Aerospike.batch_get_header(cluster, [key_b, missing_key, key_a, key_c],
               timeout: 5_000
             )

    assert {:ok, %Record{key: ^key_b, bins: %{}, generation: generation_b, ttl: ttl_b}} = first
    assert generation_b >= 1
    assert ttl_b >= 0
    assert {:error, %Error{code: :key_not_found}} = second
    assert {:ok, %Record{key: ^key_a, bins: %{}, generation: generation_a, ttl: ttl_a}} = third
    assert generation_a >= 1
    assert ttl_a >= 0
    assert {:ok, %Record{key: ^key_c, bins: %{}, generation: generation_c, ttl: ttl_c}} = fourth
    assert generation_c >= 1
    assert ttl_c >= 0
  end

  test "batch_get_operate returns read-only operation records across multiple live cluster nodes",
       %{
         cluster: cluster
       } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    set = IntegrationSupport.unique_name("spike_batch_operate")
    [{node_a, key_a}, {node_b, key_b}, {node_c, key_c}] = keys_for_distinct_nodes(cluster, set, 3)
    missing_key = key_for_node(cluster, set, node_a, "missing")

    assert {:ok, _} = Aerospike.put(cluster, key_a, %{"node" => node_a, "value" => 11})
    assert {:ok, _} = Aerospike.put(cluster, key_b, %{"node" => node_b, "value" => 22})
    assert {:ok, _} = Aerospike.put(cluster, key_c, %{"node" => node_c, "value" => 33})

    assert {:ok, [first, second, third, fourth]} =
             Aerospike.batch_get_operate(
               cluster,
               [key_b, missing_key, key_a, key_c],
               [Op.get("node"), Op.get("value")],
               timeout: 5_000
             )

    assert {:ok, %Record{key: ^key_b, bins: %{"node" => ^node_b, "value" => 22}}} = first
    assert {:error, %Error{code: :key_not_found}} = second
    assert {:ok, %Record{key: ^key_a, bins: %{"node" => ^node_a, "value" => 11}}} = third
    assert {:ok, %Record{key: ^key_c, bins: %{"node" => ^node_c, "value" => 33}}} = fourth
  end

  test "batch_exists returns booleans in caller order across multiple live cluster nodes", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    set = IntegrationSupport.unique_name("spike_batch_exists")
    [{node_a, key_a}, {node_b, key_b}, {node_c, key_c}] = keys_for_distinct_nodes(cluster, set, 3)
    missing_key = key_for_node(cluster, set, node_a, "missing")

    assert {:ok, _} = Aerospike.put(cluster, key_a, %{"node" => node_a, "value" => 11})
    assert {:ok, _} = Aerospike.put(cluster, key_b, %{"node" => node_b, "value" => 22})
    assert {:ok, _} = Aerospike.put(cluster, key_c, %{"node" => node_c, "value" => 33})

    assert {:ok, [first, second, third, fourth]} =
             Aerospike.batch_exists(cluster, [key_b, missing_key, key_a, key_c], timeout: 5_000)

    assert {:ok, true} = first
    assert {:ok, false} = second
    assert {:ok, true} = third
    assert {:ok, true} = fourth
  end

  defp keys_for_distinct_nodes(cluster, set, count) when is_integer(count) and count > 0 do
    tables = Tender.tables(cluster)

    0..50_000
    |> Enum.reduce_while(%{}, fn suffix, acc ->
      key = Key.new(@namespace, set, "live-#{suffix}")

      case Router.pick_for_read(tables, key, :master, 0) do
        {:ok, node_name} ->
          halt_if_complete(Map.put_new(acc, node_name, key), count)

        {:error, reason} ->
          flunk("expected a routed key while building live batch proof, got #{inspect(reason)}")
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

  defp key_for_node(cluster, set, node_name, prefix) do
    tables = Tender.tables(cluster)

    0..50_000
    |> Enum.find_value(fn suffix ->
      key = Key.new(@namespace, set, "#{prefix}-#{suffix}")

      case Router.pick_for_read(tables, key, :master, 0) do
        {:ok, ^node_name} ->
          key

        {:ok, _other} ->
          nil

        {:error, reason} ->
          flunk("expected a routed key for #{node_name}, got #{inspect(reason)}")
      end
    end)
    |> case do
      %Key{} = key -> key
      nil -> flunk("expected to find a key routed to #{node_name}")
    end
  end
end
