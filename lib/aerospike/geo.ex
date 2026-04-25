defmodule Aerospike.Geo do
  @moduledoc """
  Typed geospatial values for Aerospike GeoJSON bins.

  Use `point/2`, `polygon/1`, and `circle/3` to build values that can be
  written directly to geo bins. `to_json/1` and `from_json/1` convert between
  those structs and GeoJSON strings.

  Unsupported or malformed GeoJSON is preserved as `{:geojson, json}` so
  callers can keep round-tripping values the client does not model directly.
  """

  defmodule Point do
    @moduledoc """
    GeoJSON Point geometry using `[longitude, latitude]` coordinates.
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
    GeoJSON Polygon geometry using rings of `[longitude, latitude]` coordinates.
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
    Aerospike `AeroCircle` geometry using center longitude, latitude, and radius.
    """

    @enforce_keys [:lng, :lat, :radius]
    defstruct [:lng, :lat, :radius]

    @type t :: %__MODULE__{
            lng: float(),
            lat: float(),
            radius: float()
          }
  end

  @typedoc "Supported typed geospatial values."
  @type t :: Point.t() | Polygon.t() | Circle.t()

  @doc """
  Builds a point from longitude and latitude.
  """
  @spec point(number(), number()) :: Point.t()
  def point(lng, lat) when is_number(lng) and is_number(lat) do
    %Point{lng: to_float(lng), lat: to_float(lat)}
  end

  @doc """
  Builds a polygon from rings of `{longitude, latitude}` coordinate tuples.
  """
  @spec polygon([[{number(), number()}]]) :: Polygon.t()
  def polygon(coordinates) when is_list(coordinates) do
    %Polygon{coordinates: normalize_rings(coordinates)}
  end

  @doc """
  Builds an Aerospike circle from center longitude, latitude, and radius.
  """
  @spec circle(number(), number(), number()) :: Circle.t()
  def circle(lng, lat, radius) when is_number(lng) and is_number(lat) and is_number(radius) do
    %Circle{lng: to_float(lng), lat: to_float(lat), radius: to_float(radius)}
  end

  @doc """
  Encodes a typed geo value as a GeoJSON string.
  """
  @spec to_json(t()) :: String.t()
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
  Decodes a GeoJSON string into a typed value when the shape is supported.

  Unknown geometry types, unsupported coordinate shapes, and invalid JSON return
  `{:geojson, json}` with the original string.
  """
  @spec from_json(String.t()) :: t() | {:geojson, String.t()}
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

  defp decode_polygon_rings(_rings, _acc), do: :error

  defp decode_polygon_ring([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_polygon_ring([[lng, lat] | rest], acc) when is_number(lng) and is_number(lat) do
    decode_polygon_ring(rest, [{to_float(lng), to_float(lat)} | acc])
  end

  defp decode_polygon_ring(_ring, _acc), do: :error

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
