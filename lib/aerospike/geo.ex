defmodule Aerospike.Geo do
  @moduledoc """
  Geospatial geometry builders used by public APIs.

  `Aerospike.Geo` replaces ad-hoc GeoJSON string handling with typed Elixir
  structs for the geometry shapes supported by this client:

  - `Point`
  - `Polygon`
  - `AeroCircle` (modeled as `Circle`)

  Build geometry values with `point/2`, `polygon/1`, and `circle/3`, then pass
  them directly to APIs like `Aerospike.Filter.geo_within/2` and
  `Aerospike.Filter.geo_contains/2`.

  ## Examples

      alias Aerospike.{Filter, Geo}

      point = Geo.point(-122.5, 45.5)

      region =
        Geo.polygon([
          [{-122.6, 45.4}, {-122.3, 45.4}, {-122.3, 45.7}, {-122.6, 45.7}, {-122.6, 45.4}]
        ])

      Filter.geo_contains("shape", point)
      Filter.geo_within("shape", region)

  ## JSON helpers

  `to_json/1` and `from_json/1` convert between Geo structs and GeoJSON strings.
  `from_json/1` returns `{:geojson, json}` for unsupported geometry types or
  malformed JSON so callers can preserve raw values.
  """

  defmodule Point do
    @moduledoc """
    GeoJSON Point geometry (`[lng, lat]`).
    """

    @enforce_keys [:lng, :lat]
    defstruct [:lng, :lat]

    @type t :: %__MODULE__{
            lng: float(),
            lat: float()
          }
  end

  defmodule Polygon do
    @moduledoc """
    GeoJSON Polygon geometry (`[[[lng, lat], ...], ...]`).
    """

    @enforce_keys [:coordinates]
    defstruct [:coordinates]

    @type coordinate_pair :: {float(), float()}
    @type ring :: [coordinate_pair()]
    @type t :: %__MODULE__{
            coordinates: [ring()]
          }
  end

  defmodule Circle do
    @moduledoc """
    Aerospike geo circle (`AeroCircle`) geometry.
    """

    @enforce_keys [:lng, :lat, :radius]
    defstruct [:lng, :lat, :radius]

    @type t :: %__MODULE__{
            lng: float(),
            lat: float(),
            radius: float()
          }
  end

  @doc """
  Builds a point geometry from longitude and latitude.

  ## Example

      iex> Aerospike.Geo.point(-122, 45)
      %Aerospike.Geo.Point{lng: -122.0, lat: 45.0}
  """
  @spec point(number(), number()) :: Point.t()
  def point(lng, lat) when is_number(lng) and is_number(lat) do
    %Point{lng: to_float(lng), lat: to_float(lat)}
  end

  @doc """
  Builds a polygon geometry from rings of coordinate tuples.

  Each ring is a list of `{lng, lat}` tuples.

  ## Example

      iex> Aerospike.Geo.polygon([[{-122, 45}, {-122.5, 45.5}]])
      %Aerospike.Geo.Polygon{coordinates: [[{-122.0, 45.0}, {-122.5, 45.5}]]}
  """
  @spec polygon([[{number(), number()}]]) :: Polygon.t()
  def polygon(coordinates) when is_list(coordinates) do
    %Polygon{coordinates: normalize_rings(coordinates)}
  end

  @doc """
  Builds an Aerospike circle geometry from center and radius in meters.

  ## Example

      iex> Aerospike.Geo.circle(-122, 45, 5000)
      %Aerospike.Geo.Circle{lng: -122.0, lat: 45.0, radius: 5000.0}
  """
  @spec circle(number(), number(), number()) :: Circle.t()
  def circle(lng, lat, radius) when is_number(lng) and is_number(lat) and is_number(radius) do
    %Circle{lng: to_float(lng), lat: to_float(lat), radius: to_float(radius)}
  end

  @doc """
  Encodes a Geo struct into a GeoJSON string.

  ## Example

      iex> Aerospike.Geo.point(-122.5, 45.5) |> Aerospike.Geo.to_json() |> Jason.decode!()
      %{"type" => "Point", "coordinates" => [-122.5, 45.5]}
  """
  @spec to_json(Point.t() | Polygon.t() | Circle.t()) :: String.t()
  def to_json(%Point{lng: lng, lat: lat}) do
    coordinates_json = Jason.encode!([lng, lat])
    "{\"type\":\"Point\",\"coordinates\":#{coordinates_json}}"
  end

  def to_json(%Polygon{coordinates: coordinates}) do
    polygon_coordinates =
      Enum.map(coordinates, fn ring ->
        Enum.map(ring, fn {lng, lat} -> [lng, lat] end)
      end)

    coordinates_json = Jason.encode!(polygon_coordinates)
    "{\"type\":\"Polygon\",\"coordinates\":#{coordinates_json}}"
  end

  def to_json(%Circle{lng: lng, lat: lat, radius: radius}) do
    coordinates_json = Jason.encode!([[lng, lat], radius])
    "{\"type\":\"AeroCircle\",\"coordinates\":#{coordinates_json}}"
  end

  @doc """
  Decodes a GeoJSON string into a supported geometry struct.

  Returns `{:geojson, json}` when parsing fails or the geometry type is unsupported.

  ## Examples

      iex> Aerospike.Geo.from_json(~s({"type":"Point","coordinates":[-122.5,45.5]}))
      %Aerospike.Geo.Point{lng: -122.5, lat: 45.5}

      iex> Aerospike.Geo.from_json(~s({"type":"LineString","coordinates":[[0,0],[1,1]]}))
      {:geojson, ~s({"type":"LineString","coordinates":[[0,0],[1,1]]})}
  """
  @spec from_json(String.t()) :: Point.t() | Polygon.t() | Circle.t() | {:geojson, String.t()}
  def from_json(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, decoded} -> from_decoded_geojson(decoded, json)
      {:error, _reason} -> {:geojson, json}
    end
  end

  defp normalize_rings(rings) do
    Enum.map(rings, &normalize_ring/1)
  end

  defp normalize_ring(ring) do
    Enum.map(ring, &normalize_point/1)
  end

  defp normalize_point({lng, lat}) when is_number(lng) and is_number(lat) do
    {to_float(lng), to_float(lat)}
  end

  defp decode_polygon_rings(rings) when is_list(rings) do
    decode_polygon_rings(rings, [])
  end

  defp decode_polygon_rings([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_polygon_rings([ring | rest], acc) when is_list(ring) do
    case decode_polygon_ring(ring, []) do
      {:ok, normalized_ring} -> decode_polygon_rings(rest, [normalized_ring | acc])
      :error -> :error
    end
  end

  defp decode_polygon_rings(_, _acc), do: :error

  defp decode_polygon_ring([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_polygon_ring([[lng, lat] | rest], acc) when is_number(lng) and is_number(lat) do
    decode_polygon_ring(rest, [{lng, lat} | acc])
  end

  defp decode_polygon_ring(_, _acc), do: :error

  defp from_decoded_geojson(%{"type" => "Point", "coordinates" => [lng, lat]}, _json)
       when is_number(lng) and is_number(lat) do
    point(lng, lat)
  end

  defp from_decoded_geojson(%{"type" => "Polygon", "coordinates" => rings}, json)
       when is_list(rings) do
    case decode_polygon_rings(rings) do
      {:ok, polygon_rings} -> polygon(polygon_rings)
      :error -> {:geojson, json}
    end
  end

  defp from_decoded_geojson(
         %{"type" => "AeroCircle", "coordinates" => [[lng, lat], radius]},
         _json
       )
       when is_number(lng) and is_number(lat) and is_number(radius) do
    circle(lng, lat, radius)
  end

  defp from_decoded_geojson(_decoded, json), do: {:geojson, json}

  defp to_float(number) when is_float(number), do: number
  defp to_float(number) when is_integer(number), do: number * 1.0
end
