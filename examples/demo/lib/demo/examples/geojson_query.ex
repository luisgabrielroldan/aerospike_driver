defmodule Demo.Examples.GeojsonQuery do
  @moduledoc """
  Demonstrates GeoJSON queries using secondary indexes.

  Creates a geo2dsphere index on a bin containing GeoJSON points, then uses
  `Filter.geo_within/2` to find points inside a bounding region. Also
  demonstrates `Filter.geo_contains/2` to find stored polygons that contain
  a given point.
  """

  require Logger

  alias Aerospike.Filter
  alias Aerospike.Query

  @conn :aero
  @namespace "test"
  @set "demo_geo"
  @point_index "demo_geo_loc_idx"
  @region_index "demo_geo_region_idx"

  @locations [
    {"portland", -122.6765, 45.5231},
    {"seattle", -122.3321, 47.6062},
    {"san_francisco", -122.4194, 37.7749},
    {"los_angeles", -118.2437, 34.0522},
    {"denver", -104.9903, 39.7392}
  ]

  def run do
    write_records()
    create_indexes()
    query_points_within_region()
    query_regions_containing_point()
    cleanup()
  end

  defp write_records do
    Logger.info("  Writing #{length(@locations)} location records...")

    for {name, lng, lat} <- @locations do
      key = Aerospike.key(@namespace, @set, name)

      point = {:geojson, Jason.encode!(%{"type" => "Point", "coordinates" => [lng, lat]})}
      region = {:geojson, bounding_box(lng, lat, 2.0)}

      bins = %{"name" => name, "loc" => point, "region" => region}
      :ok = Aerospike.put!(@conn, key, bins)
    end
  end

  defp create_indexes do
    Logger.info("  Creating geo2dsphere indexes...")

    {:ok, task1} =
      Aerospike.create_index(@conn, @namespace, @set,
        bin: "loc",
        name: @point_index,
        type: :geo2dsphere
      )

    {:ok, task2} =
      Aerospike.create_index(@conn, @namespace, @set,
        bin: "region",
        name: @region_index,
        type: :geo2dsphere
      )

    :ok = Aerospike.IndexTask.wait(task1, timeout: 15_000)
    :ok = Aerospike.IndexTask.wait(task2, timeout: 15_000)

    Logger.info("  Indexes ready.")
  end

  defp query_points_within_region do
    Logger.info("  Querying: points within Pacific Northwest bounding box...")

    pnw_region =
      Jason.encode!(%{
        "type" => "Polygon",
        "coordinates" => [
          [
            [-125.0, 44.0],
            [-120.0, 44.0],
            [-120.0, 49.0],
            [-125.0, 49.0],
            [-125.0, 44.0]
          ]
        ]
      })

    query =
      Query.new(@namespace, @set)
      |> Query.where(Filter.geo_within("loc", pnw_region))
      |> Query.max_records(20)

    {:ok, records} = Aerospike.all(@conn, query)

    names = Enum.map(records, fn r -> r.bins["name"] end) |> Enum.sort()
    Logger.info("    Found #{length(records)} cities in PNW: #{Enum.join(names, ", ")}")

    for name <- ["portland", "seattle"] do
      unless name in names do
        raise "Expected #{name} in PNW results"
      end
    end

    for name <- ["los_angeles", "denver"] do
      if name in names do
        raise "Did not expect #{name} in PNW results"
      end
    end
  end

  defp query_regions_containing_point do
    Logger.info("  Querying: regions containing a point near Portland...")

    portland_point =
      Jason.encode!(%{
        "type" => "Point",
        "coordinates" => [-122.68, 45.52]
      })

    query =
      Query.new(@namespace, @set)
      |> Query.where(Filter.geo_contains("region", portland_point))
      |> Query.max_records(20)

    {:ok, records} = Aerospike.all(@conn, query)

    names = Enum.map(records, fn r -> r.bins["name"] end) |> Enum.sort()
    Logger.info("    #{length(records)} regions contain point: #{Enum.join(names, ", ")}")

    unless "portland" in names do
      raise "Expected portland's region to contain point near Portland"
    end
  end

  defp cleanup do
    Aerospike.drop_index(@conn, @namespace, @point_index)
    Aerospike.drop_index(@conn, @namespace, @region_index)

    for {name, _, _} <- @locations do
      Aerospike.delete(@conn, Aerospike.key(@namespace, @set, name))
    end
  end

  defp bounding_box(center_lng, center_lat, half_deg) do
    Jason.encode!(%{
      "type" => "Polygon",
      "coordinates" => [
        [
          [center_lng - half_deg, center_lat - half_deg],
          [center_lng + half_deg, center_lat - half_deg],
          [center_lng + half_deg, center_lat + half_deg],
          [center_lng - half_deg, center_lat + half_deg],
          [center_lng - half_deg, center_lat - half_deg]
        ]
      ]
    })
  end
end
