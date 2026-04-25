defmodule Aerospike.GeoTest do
  use ExUnit.Case, async: true

  alias Aerospike.Geo

  describe "point/2" do
    test "builds a point with float fields" do
      assert %Geo.Point{lng: -122.0, lat: 45.0} = Geo.point(-122, 45)
    end

    test "rejects non-numeric coordinates" do
      assert_raise FunctionClauseError, fn -> Geo.point("bad", 45.0) end
      assert_raise FunctionClauseError, fn -> Geo.point(-122.0, nil) end
    end

    test "requires explicit point fields" do
      assert_raise ArgumentError, ~r/the following keys must also be given/, fn ->
        Code.eval_string("%Aerospike.Geo.Point{}")
      end
    end
  end

  describe "polygon/1" do
    test "builds and normalizes polygon coordinates" do
      polygon = Geo.polygon([[{-122, 45}, {-122.5, 45.5}], [{-122.25, 45.25}]])

      assert %Geo.Polygon{
               coordinates: [[{-122.0, 45.0}, {-122.5, 45.5}], [{-122.25, 45.25}]]
             } = polygon
    end

    test "preserves empty polygon coordinates" do
      assert %Geo.Polygon{coordinates: []} = Geo.polygon([])
    end

    test "rejects invalid coordinate shapes" do
      assert_raise FunctionClauseError, fn ->
        Geo.polygon([[{-122.0}]])
      end
    end
  end

  describe "circle/3" do
    test "builds a circle with numeric radius" do
      assert %Geo.Circle{lng: -122.0, lat: 45.0, radius: 5_000.0} =
               Geo.circle(-122, 45, 5_000)
    end

    test "supports zero radius and negative coordinates" do
      circle = Geo.circle(-122.5, -45.5, 0)
      assert %Geo.Circle{lng: -122.5, lat: -45.5} = circle
      assert circle.radius == 0.0
    end

    test "rejects non-numeric radius" do
      assert_raise FunctionClauseError, fn -> Geo.circle(-122.0, 45.0, "500") end
    end
  end

  describe "to_json/1" do
    test "encodes point to GeoJSON" do
      decoded =
        -122.5
        |> Geo.point(45.5)
        |> Geo.to_json()
        |> Jason.decode!()

      assert decoded == %{"type" => "Point", "coordinates" => [-122.5, 45.5]}
    end

    test "encodes polygon to GeoJSON" do
      decoded =
        [[{-122.0, 45.0}, {-122.5, 45.5}], [{-122.25, 45.25}]]
        |> Geo.polygon()
        |> Geo.to_json()
        |> Jason.decode!()

      assert decoded == %{
               "type" => "Polygon",
               "coordinates" => [[[-122.0, 45.0], [-122.5, 45.5]], [[-122.25, 45.25]]]
             }
    end

    test "encodes circle to AeroCircle GeoJSON" do
      decoded =
        -122.0
        |> Geo.circle(45.0, 5_000)
        |> Geo.to_json()
        |> Jason.decode!()

      assert decoded == %{"type" => "AeroCircle", "coordinates" => [[-122.0, 45.0], 5_000.0]}
    end
  end

  describe "from_json/1" do
    test "decodes point GeoJSON to struct" do
      json = ~s({"type":"Point","coordinates":[-122.5,45.5]})

      assert %Geo.Point{lng: -122.5, lat: 45.5} = Geo.from_json(json)
    end

    test "decodes polygon GeoJSON to struct" do
      json = ~s({"type":"Polygon","coordinates":[[[-122,45],[-122.5,45.5]],[[-122.25,45.25]]]})

      assert %Geo.Polygon{
               coordinates: [[{-122.0, 45.0}, {-122.5, 45.5}], [{-122.25, 45.25}]]
             } = Geo.from_json(json)
    end

    test "decodes AeroCircle GeoJSON to struct" do
      json = ~s({"type":"AeroCircle","coordinates":[[-122,45],5000]})

      assert %Geo.Circle{lng: -122.0, lat: 45.0, radius: 5_000.0} = Geo.from_json(json)
    end

    test "falls back to raw tuple for unknown geometry type" do
      json = ~s({"type":"LineString","coordinates":[[0,0],[1,1]]})

      assert {:geojson, ^json} = Geo.from_json(json)
    end

    test "falls back to raw tuple when JSON parsing fails" do
      json = ~s({"type":"Point","coordinates":[-122.5,45.5])

      assert {:geojson, ^json} = Geo.from_json(json)
    end
  end

  describe "round-trip" do
    test "point round-trip preserves shape and values" do
      point = Geo.point(-122.5, 45.5)
      assert ^point = point |> Geo.to_json() |> Geo.from_json()
    end

    test "polygon round-trip preserves shape and values" do
      polygon = Geo.polygon([[{-122.0, 45.0}, {-122.5, 45.5}], [{-122.25, 45.25}]])
      assert ^polygon = polygon |> Geo.to_json() |> Geo.from_json()
    end

    test "circle round-trip preserves shape and values" do
      circle = Geo.circle(-122.0, 45.0, 5_000)
      assert ^circle = circle |> Geo.to_json() |> Geo.from_json()
    end
  end
end
