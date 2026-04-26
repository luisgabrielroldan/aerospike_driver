defmodule Aerospike.Integration.ScanTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :cluster

  alias Aerospike
  alias Aerospike.Cluster.Router
  alias Aerospike.Cluster.Tender
  alias Aerospike.Key
  alias Aerospike.Record
  alias Aerospike.Scan
  alias Aerospike.Test.IntegrationSupport

  @seeds [{"localhost", 3000}, {"localhost", 3010}, {"localhost", 3020}]
  @namespace "test"

  setup do
    IntegrationSupport.probe_aerospike!(
      @seeds,
      "Run `docker compose --profile cluster up -d aerospike aerospike2 aerospike3` in `aerospike_driver/` first."
    )

    IntegrationSupport.wait_for_cluster_ready!(@seeds, @namespace, 15_000)
    name = IntegrationSupport.unique_atom("spike_scan_cluster")

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

  test "scan stream fans out across live cluster nodes and yields all seeded records", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    set = IntegrationSupport.unique_name("spike_scan")
    [{node_a, key_a}, {node_b, key_b}, {node_c, key_c}] = keys_for_distinct_nodes(cluster, set, 3)

    assert {:ok, _} = Aerospike.put(cluster, key_a, %{"node" => node_a, "value" => 11})
    assert {:ok, _} = Aerospike.put(cluster, key_b, %{"node" => node_b, "value" => 22})
    assert {:ok, _} = Aerospike.put(cluster, key_c, %{"node" => node_c, "value" => 33})

    scan = Scan.new(@namespace, set)

    IntegrationSupport.assert_eventually("scan fan-out returns all seeded records", fn ->
      records = Aerospike.scan_stream!(cluster, scan) |> Enum.to_list()

      Enum.sort_by(records, & &1.bins["value"]) |> Enum.map(& &1.bins["value"]) == [11, 22, 33]
    end)

    IntegrationSupport.assert_eventually("scan_count reaches the seeded total", fn ->
      match?({:ok, 3}, Aerospike.scan_count(cluster, scan))
    end)

    IntegrationSupport.assert_eventually("scan_all returns the seeded total", fn ->
      match?({:ok, [%Record{}, %Record{}, %Record{}]}, Aerospike.scan_all(cluster, scan))
    end)
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
          flunk("expected a routed key while building live scan proof, got #{inspect(reason)}")
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
end
