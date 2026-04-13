defmodule Aerospike.Integration.QueryScanParityTest do
  use ExUnit.Case, async: false

  alias Aerospike.Filter
  alias Aerospike.Geo
  alias Aerospike.Page
  alias Aerospike.PartitionFilter
  alias Aerospike.Query
  alias Aerospike.Scan
  alias Aerospike.Test.Helpers

  @moduletag :integration

  @namespace "test"

  @query_set "query_page_parity_itest"
  @query_index "query_page_parity_qval_idx"

  @partition_set "partition_filter_parity_itest"

  @geo_set "geo_parity_itest"
  @geo_point_index "geo_parity_loc_idx"
  @geo_region_index "geo_parity_region_idx"

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"query_scan_parity_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      recv_timeout: 60_000,
      tend_interval: 60_000,
      defaults: [
        read: [timeout: 5_000],
        write: [timeout: 5_000],
        scan: [timeout: 60_000],
        query: [timeout: 60_000]
      ]
    ]

    {:ok, _sup} = start_supervised({Aerospike, opts})
    Helpers.await_cluster_ready(name)

    {:ok, conn: name, host: host, port: port}
  end

  test "query pagination with page/3 and cursor covers full range", %{
    conn: conn,
    host: host,
    port: port
  } do
    {keys, set_name, index_name} = seed_query_paginate_records(conn)

    on_exit(fn ->
      Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port))
      Helpers.cleanup_index(@namespace, index_name, host: host, port: port)
    end)

    assert {:ok, task} =
             Aerospike.create_index(conn, @namespace, set_name,
               bin: "qval",
               name: index_name,
               type: :numeric
             )

    assert :ok = Aerospike.IndexTask.wait(task, timeout: 30_000, poll_interval: 200)
    Process.sleep(500)

    query =
      Query.new(@namespace, set_name)
      |> Query.where(Filter.range("qval", 10, 80))
      |> Query.max_records(5)

    assert {:ok, %Page{records: page1_records, cursor: cursor, done?: done?}} =
             Aerospike.page(conn, query)

    page1_values = page1_records |> Enum.map(& &1.bins["qval"]) |> Enum.sort()
    assert Enum.all?(page1_values, &(&1 >= 10 and &1 <= 80))
    assert page1_values != []

    if done? do
      assert cursor == nil
    else
      assert {:ok, %Page{records: page2_records}} = Aerospike.page(conn, query, cursor: cursor)
      page2_values = page2_records |> Enum.map(& &1.bins["qval"]) |> Enum.sort()
      assert Enum.all?(page2_values, &(&1 >= 10 and &1 <= 80))
    end
  end

  test "query stream!/2 returns only values inside filter range", %{
    conn: conn,
    host: host,
    port: port
  } do
    {keys, set_name, index_name} = seed_query_paginate_records(conn)

    on_exit(fn ->
      Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port))
      Helpers.cleanup_index(@namespace, index_name, host: host, port: port)
    end)

    assert {:ok, task} =
             Aerospike.create_index(conn, @namespace, set_name,
               bin: "qval",
               name: index_name,
               type: :numeric
             )

    assert :ok = Aerospike.IndexTask.wait(task, timeout: 30_000, poll_interval: 200)
    Process.sleep(500)

    query =
      Query.new(@namespace, set_name)
      |> Query.where(Filter.range("qval", 50, 100))
      |> Query.max_records(50)

    values =
      Aerospike.stream!(conn, query)
      |> Enum.map(& &1.bins["qval"])
      |> Enum.sort()

    assert values == Enum.to_list(50..100//5)
  end

  test "scan with PartitionFilter.by_id/1 returns only records from one partition", %{
    conn: conn,
    host: host,
    port: port
  } do
    keys =
      for i <- 1..50 do
        key = Aerospike.key(@namespace, @partition_set, "pf_#{i}")
        :ok = Aerospike.put!(conn, key, %{"idx" => i})
        key
      end

    on_exit(fn -> Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port)) end)

    target_key = Aerospike.key(@namespace, @partition_set, "pf_1")
    partition_id = Aerospike.Key.partition_id(target_key)

    scan =
      Scan.new(@namespace, @partition_set)
      |> Scan.partition_filter(PartitionFilter.by_id(partition_id))
      |> Scan.max_records(200)

    assert {:ok, records} = Aerospike.all(conn, scan)
    assert records != []

    assert Enum.all?(records, fn record ->
             Aerospike.Key.partition_id(record.key) == partition_id
           end)
  end

  test "scan with PartitionFilter.by_range/2 visits a subset of partitions", %{
    conn: conn,
    host: host,
    port: port
  } do
    keys =
      for i <- 1..50 do
        key = Aerospike.key(@namespace, @partition_set, "pfr_#{i}")
        :ok = Aerospike.put!(conn, key, %{"idx" => i})
        key
      end

    on_exit(fn -> Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port)) end)

    subset_scan =
      Scan.new(@namespace, @partition_set)
      |> Scan.partition_filter(PartitionFilter.by_range(0, 512))
      |> Scan.max_records(200)

    full_scan =
      Scan.new(@namespace, @partition_set)
      |> Scan.max_records(200)

    assert {:ok, subset_records} = Aerospike.all(conn, subset_scan)
    assert {:ok, all_records} = Aerospike.all(conn, full_scan)

    assert length(subset_records) <= length(all_records)
    assert all_records != []
  end

  test "geojson queries with geo_within and geo_contains", %{conn: conn, host: host, port: port} do
    locations = [
      {"portland", -122.6765, 45.5231},
      {"seattle", -122.3321, 47.6062},
      {"san_francisco", -122.4194, 37.7749},
      {"los_angeles", -118.2437, 34.0522},
      {"denver", -104.9903, 39.7392}
    ]

    keys =
      for {name, lng, lat} <- locations do
        key = Aerospike.key(@namespace, @geo_set, name)
        point = Geo.point(lng, lat)
        region = geo_box(lng, lat, 2.0)
        :ok = Aerospike.put!(conn, key, %{"name" => name, "loc" => point, "region" => region})
        key
      end

    on_exit(fn ->
      Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port))
      Helpers.cleanup_index(@namespace, @geo_point_index, host: host, port: port)
      Helpers.cleanup_index(@namespace, @geo_region_index, host: host, port: port)
    end)

    assert {:ok, point_task} =
             Aerospike.create_index(conn, @namespace, @geo_set,
               bin: "loc",
               name: @geo_point_index,
               type: :geo2dsphere
             )

    assert {:ok, region_task} =
             Aerospike.create_index(conn, @namespace, @geo_set,
               bin: "region",
               name: @geo_region_index,
               type: :geo2dsphere
             )

    assert :ok = Aerospike.IndexTask.wait(point_task, timeout: 30_000, poll_interval: 200)
    assert :ok = Aerospike.IndexTask.wait(region_task, timeout: 30_000, poll_interval: 200)
    Process.sleep(500)

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
      Query.new(@namespace, @geo_set)
      |> Query.where(Filter.geo_within("loc", pnw_region))
      |> Query.max_records(20)

    assert {:ok, within_records} = Aerospike.all(conn, within_query)
    within_names = within_records |> Enum.map(& &1.bins["name"]) |> Enum.sort()
    assert Enum.all?(within_records, &match?(%Geo.Point{}, &1.bins["loc"]))
    assert Enum.all?(within_records, &match?(%Geo.Polygon{}, &1.bins["region"]))

    assert "portland" in within_names
    assert "seattle" in within_names
    refute "los_angeles" in within_names
    refute "denver" in within_names

    portland_point = Geo.point(-122.68, 45.52)

    contains_query =
      Query.new(@namespace, @geo_set)
      |> Query.where(Filter.geo_contains("region", portland_point))
      |> Query.max_records(20)

    assert {:ok, contains_records} = Aerospike.all(conn, contains_query)
    contains_names = contains_records |> Enum.map(& &1.bins["name"]) |> Enum.sort()
    assert Enum.all?(contains_records, &match?(%Geo.Point{}, &1.bins["loc"]))
    assert Enum.all?(contains_records, &match?(%Geo.Polygon{}, &1.bins["region"]))

    assert "portland" in contains_names
  end

  defp seed_query_paginate_records(conn) do
    suffix = System.unique_integer([:positive])
    set_name = "#{@query_set}_#{suffix}"
    index_name = "#{@query_index}_#{suffix}"

    keys =
      for i <- 1..20 do
        key = Aerospike.key(@namespace, set_name, "qp_#{i}")
        :ok = Aerospike.put!(conn, key, %{"name" => "item_#{i}", "qval" => i * 5})
        key
      end

    {keys, set_name, index_name}
  end

  defp geo_box(center_lng, center_lat, half_deg) do
    Geo.polygon([
      [
        {center_lng - half_deg, center_lat - half_deg},
        {center_lng + half_deg, center_lat - half_deg},
        {center_lng + half_deg, center_lat + half_deg},
        {center_lng - half_deg, center_lat + half_deg},
        {center_lng - half_deg, center_lat - half_deg}
      ]
    ])
  end
end
