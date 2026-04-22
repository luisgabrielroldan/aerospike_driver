defmodule Aerospike.Integration.IndexQueryTest do
  use ExUnit.Case, async: false

  alias Aerospike.Cursor
  alias Aerospike.Filter
  alias Aerospike.Key
  alias Aerospike.Page
  alias Aerospike.Query
  alias Aerospike.Router
  alias Aerospike.Tender

  @moduletag :integration

  @host "localhost"
  @port 3_000
  @namespace "test"

  setup do
    probe_aerospike!(@host, @port)
    name = :"spike_index_query_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        seeds: [{@host, @port}],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 2
      )

    :ok = Aerospike.Tender.tend_now(name)

    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    %{cluster: name}
  end

  test "creates a temporary secondary index and queries it through the public filter builder", %{
    cluster: cluster
  } do
    set = "idx_query_#{System.unique_integer([:positive, :monotonic])}"
    index_name = "age_idx_#{System.unique_integer([:positive, :monotonic])}"
    {node_name, keys} = keys_for_one_node(cluster, set, 5)

    keys =
      Enum.zip(keys, 20..24)
      |> Enum.map(fn {key, age} ->
        assert {:ok, _metadata} = Aerospike.put(cluster, key, %{"age" => age, "state" => "seed"})
        key
      end)

    try do
      assert {:ok, task} =
               Aerospike.create_index(cluster, @namespace, set,
                 bin: "age",
                 name: index_name,
                 type: :numeric
               )

      assert :ok = Aerospike.IndexTask.wait(task, timeout: 30_000, poll_interval: 200)

      query =
        Query.new(@namespace, set)
        |> Query.where(Filter.range("age", 20, 24))
        |> Query.max_records(20)

      assert {:ok, records} = Aerospike.query_all(cluster, query)

      ages =
        records
        |> Enum.map(& &1.bins["age"])
        |> Enum.sort()

      assert ages == [20, 21, 22, 23, 24]

      paged_query =
        Query.new(@namespace, set)
        |> Query.where(Filter.range("age", 20, 24))
        |> Query.max_records(2)

      assert {:ok, %Page{} = page1} = Aerospike.query_page(cluster, paged_query, node: node_name)
      assert page1.cursor != nil

      assert {:ok, %Page{} = page2} =
               Aerospike.query_page(cluster, paged_query,
                 node: node_name,
                 cursor: Cursor.encode(page1.cursor)
               )

      assert Enum.all?(page1.records ++ page2.records, fn record ->
               record.bins["age"] in 20..24
             end)

      assert page2.cursor != nil or page2.done? == true
    after
      Enum.each(keys, &cleanup_key(cluster, &1))
      _ = Aerospike.drop_index(cluster, @namespace, index_name)
    end
  end

  defp cleanup_key(cluster, key) do
    _ = Aerospike.delete(cluster, key)
  end

  defp keys_for_one_node(cluster, set, count) when is_integer(count) and count > 0 do
    tables = Tender.tables(cluster)

    0..50_000
    |> Enum.reduce_while(%{}, fn suffix, acc ->
      key = Key.new(@namespace, set, "idx-live-#{suffix}")
      route_key_for_node(acc, tables, key, count)
    end)
    |> case do
      {node_name, keys} ->
        {node_name, keys}

      _ ->
        flunk("expected #{count} routed keys on one node for live query pagination proof")
    end
  end

  defp route_key_for_node(acc, tables, key, count) do
    case Router.pick_for_read(tables, key, :master, 0) do
      {:ok, node_name} ->
        advance_routed_keys(acc, node_name, key, count)

      {:error, reason} ->
        flunk("expected a routed key while building live query proof, got #{inspect(reason)}")
    end
  end

  defp advance_routed_keys(acc, node_name, key, count) do
    keys = Map.get(acc, node_name, [])
    next = Map.put(acc, node_name, [key | keys])

    if length(next[node_name]) == count do
      {:halt, {node_name, Enum.reverse(next[node_name])}}
    else
      {:cont, next}
    end
  end

  defp probe_aerospike!(host, port) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 1_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, reason} ->
        raise "Aerospike not reachable at #{host}:#{port} (#{inspect(reason)}). Run `docker compose up -d` first."
    end
  end
end
