defmodule Aerospike.Integration.BatchGetTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :cluster

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Record
  alias Aerospike.Router
  alias Aerospike.Tender

  @seeds [{"localhost", 3000}, {"localhost", 3010}, {"localhost", 3020}]
  @namespace "test"

  setup do
    probe_aerospike!(@seeds)
    name = :"spike_batch_get_cluster_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: Enum.map(, fn {host, port} -> "#{host}:#{port}" end),
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 2
      )

    :ok = Tender.tend_now(name)

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

    set = "spike_batch_#{System.unique_integer([:positive])}"
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

  defp probe_aerospike!(seeds) do
    Enum.each(seeds, fn {host, port} ->
      case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 1_000) do
        {:ok, sock} ->
          :gen_tcp.close(sock)
          :ok

        {:error, reason} ->
          raise "Aerospike not reachable at #{host}:#{port} (#{inspect(reason)}). " <>
                  "Run `docker compose --profile cluster up -d aerospike aerospike2 aerospike3` in `aerospike_driver/` first."
      end
    end)
  end
end
