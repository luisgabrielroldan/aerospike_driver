defmodule Aerospike.Integration.IndexQueryTest do
  use ExUnit.Case, async: false

  alias Aerospike.Cluster.Router
  alias Aerospike.Cluster.Tender
  alias Aerospike.Cursor
  alias Aerospike.Exp
  alias Aerospike.Filter
  alias Aerospike.Geo
  alias Aerospike.Key
  alias Aerospike.Page
  alias Aerospike.Query
  alias Aerospike.Test.IntegrationSupport

  @moduletag :integration

  @host "localhost"
  @port 3_000
  @namespace "test"
  setup do
    IntegrationSupport.probe_aerospike!(@host, @port)

    IntegrationSupport.wait_for_cluster_ready!([{@host, @port}], @namespace, 15_000,
      expected_size: 3
    )

    name = IntegrationSupport.unique_atom("spike_index_query")

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["#{@host}:#{@port}"],
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

  test "creates a temporary secondary index and queries it through the public filter builder", %{
    cluster: cluster
  } do
    set = IntegrationSupport.unique_name("idx_query")
    index_name = IntegrationSupport.unique_name("age_idx")
    {_node_name, keys} = keys_for_one_node(cluster, set, 5)

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

      IntegrationSupport.assert_eventually("secondary index query returns all seeded ages", fn ->
        assert {:ok, records_stream} = Aerospike.query_stream(cluster, query)

        ages =
          records_stream
          |> Enum.to_list()
          |> Enum.map(& &1.bins["age"])
          |> Enum.sort()

        ages == [20, 21, 22, 23, 24]
      end)

      IntegrationSupport.assert_eventually(
        "query include_bin_data false returns headers only",
        fn ->
          assert {:ok, records_stream} =
                   Aerospike.query_stream(cluster, query, include_bin_data: false)

          records = Enum.to_list(records_stream)

          length(records) == 5 and Enum.all?(records, &(&1.bins == %{}))
        end
      )

      paged_query =
        Query.new(@namespace, set)
        |> Query.where(Filter.range("age", 20, 24))
        |> Query.max_records(2)

      page1 =
        IntegrationSupport.eventually!("query_page returns a resumable first page", fn ->
          case Aerospike.query_page(cluster, paged_query) do
            {:ok, %Page{cursor: cursor} = page} when not is_nil(cursor) ->
              {:ok, page}

            {:ok, %Page{done?: true}} ->
              :retry

            {:ok, %Page{cursor: nil}} ->
              :retry

            {:error, _} ->
              :retry
          end
        end)

      assert {:ok, %Page{} = page2} =
               Aerospike.query_page(cluster, paged_query, cursor: Cursor.encode(page1.cursor))

      assert Enum.all?(page1.records ++ page2.records, fn record ->
               record.bins["age"] in 20..24
             end)

      assert page2.cursor != nil or page2.done? == true
    after
      Enum.each(keys, &cleanup_key(cluster, &1))
      _ = Aerospike.drop_index(cluster, @namespace, index_name)
    end
  end

  test "creates an expression-backed index and queries it by name", %{cluster: cluster} do
    set = IntegrationSupport.unique_name("expr_idx_query")
    index_name = IntegrationSupport.unique_name("age_expr_idx")
    expression = Exp.int_bin("age")
    version = fetch_server_version!(cluster)

    if supports_expression_indexes?(version) do
      keys = seed_expression_index_records(cluster, set)

      try do
        assert {:ok, task} =
                 Aerospike.create_expression_index(cluster, @namespace, set, expression,
                   name: index_name,
                   type: :numeric
                 )

        assert :ok = Aerospike.IndexTask.wait(task, timeout: 30_000, poll_interval: 200)

        query =
          Query.new(@namespace, set)
          |> Query.where(Filter.range("age", 18, 40) |> Filter.using_index(index_name))
          |> Query.max_records(20)

        IntegrationSupport.assert_eventually("expression index query returns matching ages", fn ->
          assert {:ok, records_stream} = Aerospike.query_stream(cluster, query)

          ages =
            records_stream
            |> Enum.to_list()
            |> Enum.map(& &1.bins["age"])
            |> Enum.sort()

          ages == [24, 31]
        end)
      after
        _ = Aerospike.drop_index(cluster, @namespace, index_name)
        Enum.each(keys, &cleanup_key(cluster, &1))
      end
    else
      assert {:error, %Aerospike.Error{code: :parameter_error, message: message}} =
               Aerospike.create_expression_index(cluster, @namespace, set, expression,
                 name: index_name,
                 type: :numeric
               )

      assert message ==
               "expression-backed secondary indexes require Aerospike server 8.1.0 or newer"
    end
  end

  test "creates temporary geo indexes and queries point and region bins", %{cluster: cluster} do
    set = IntegrationSupport.unique_name("geo_idx_query")
    point_index = IntegrationSupport.unique_name("geo_point_idx")
    region_index = IntegrationSupport.unique_name("geo_region_idx")
    keys = seed_geo_records(cluster, set)

    try do
      assert {:ok, point_task} =
               Aerospike.create_index(cluster, @namespace, set,
                 bin: "loc",
                 name: point_index,
                 type: :geo2dsphere
               )

      assert {:ok, region_task} =
               Aerospike.create_index(cluster, @namespace, set,
                 bin: "region",
                 name: region_index,
                 type: :geo2dsphere
               )

      assert :ok = Aerospike.IndexTask.wait(point_task, timeout: 30_000, poll_interval: 200)
      assert :ok = Aerospike.IndexTask.wait(region_task, timeout: 30_000, poll_interval: 200)

      pnw_region =
        Geo.polygon([
          [
            {-125.0, 44.0},
            {-120.0, 44.0},
            {-120.0, 49.0},
            {-125.0, 49.0},
            {-125.0, 44.0}
          ]
        ])

      within_query =
        Query.new(@namespace, set)
        |> Query.where(Filter.geo_within("loc", pnw_region))
        |> Query.max_records(20)

      IntegrationSupport.assert_eventually("geo_within query returns matching points", fn ->
        assert {:ok, records_stream} = Aerospike.query_stream(cluster, within_query)
        records = Enum.to_list(records_stream)
        names = records |> Enum.map(& &1.bins["name"]) |> Enum.sort()

        Enum.all?(records, &match?(%Geo.Point{}, &1.bins["loc"])) and
          Enum.all?(records, &geo_region_value?/1) and
          names == ["portland", "seattle"]
      end)

      contains_query =
        Query.new(@namespace, set)
        |> Query.where(Filter.geo_contains_point("region", -122.68, 45.52))
        |> Query.max_records(20)

      IntegrationSupport.assert_eventually("geo_contains query returns containing regions", fn ->
        assert {:ok, records_stream} = Aerospike.query_stream(cluster, contains_query)
        records = Enum.to_list(records_stream)
        names = records |> Enum.map(& &1.bins["name"]) |> Enum.sort()

        Enum.all?(records, &match?(%Geo.Point{}, &1.bins["loc"])) and
          Enum.all?(records, &geo_region_value?/1) and
          names == ["portland"]
      end)
    after
      Enum.each(keys, &cleanup_key(cluster, &1))
      _ = Aerospike.drop_index(cluster, @namespace, point_index)
      _ = Aerospike.drop_index(cluster, @namespace, region_index)
    end
  end

  defp cleanup_key(cluster, key) do
    _ = Aerospike.delete(cluster, key)
  end

  defp seed_geo_records(cluster, set) do
    records = [
      {"portland", -122.6765, 45.5231},
      {"seattle", -122.3321, 47.6062},
      {"san_francisco", -122.4194, 37.7749},
      {"los_angeles", -118.2437, 34.0522},
      {"denver", -104.9903, 39.7392}
    ]

    Enum.map(records, fn {name, lng, lat} ->
      key = Key.new(@namespace, set, name)
      point = Geo.point(lng, lat)
      region = geo_box(lng, lat, 2.0)

      assert {:ok, _metadata} =
               Aerospike.put(cluster, key, %{"name" => name, "loc" => point, "region" => region})

      key
    end)
  end

  defp geo_region_value?(%{bins: %{"region" => %Geo.Polygon{}}}), do: true
  defp geo_region_value?(%{bins: %{"region" => {:geojson, json}}}) when is_binary(json), do: true
  defp geo_region_value?(_record), do: false

  defp seed_expression_index_records(cluster, set) do
    records = [
      {"teen", 17},
      {"adult-1", 24},
      {"adult-2", 31},
      {"senior", 65}
    ]

    Enum.map(records, fn {user_key, age} ->
      key = Key.new(@namespace, set, user_key)
      assert {:ok, _metadata} = Aerospike.put(cluster, key, %{"age" => age})
      key
    end)
  end

  defp geo_box(center_lng, center_lat, half_degrees) do
    Geo.polygon([
      [
        {center_lng - half_degrees, center_lat - half_degrees},
        {center_lng + half_degrees, center_lat - half_degrees},
        {center_lng + half_degrees, center_lat + half_degrees},
        {center_lng - half_degrees, center_lat + half_degrees},
        {center_lng - half_degrees, center_lat - half_degrees}
      ]
    ])
  end

  defp fetch_server_version!(cluster) do
    assert {:ok, build} = Aerospike.info(cluster, "build")

    case Regex.run(~r/^v?\d+(?:\.\d+){0,3}/, build) do
      [matched] ->
        matched
        |> String.trim_leading("v")
        |> String.split(".")
        |> Enum.map(&String.to_integer/1)
        |> Kernel.++([0, 0, 0, 0])
        |> Enum.take(4)
        |> List.to_tuple()

      _ ->
        flunk("unable to parse Aerospike build string: #{inspect(build)}")
    end
  end

  defp supports_expression_indexes?(version) when is_tuple(version), do: version >= {8, 1, 0, 0}

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
end
