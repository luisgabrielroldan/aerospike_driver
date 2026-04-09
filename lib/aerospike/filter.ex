defmodule Aerospike.Filter do
  @moduledoc """
  Secondary-index filter values for queries (pure data for wire encoding).

  Use `range/3`, `equal/2`, `contains/3`, `geo_within/2`, or `geo_contains/2` to build
  a predicate, then pass it to `Aerospike.Query.where/2`.
  """

  alias Aerospike.Geo
  alias Aerospike.Key

  @enforce_keys [:bin_name, :index_type, :particle_type, :begin, :end]
  defstruct [:bin_name, :index_type, :particle_type, :begin, :end, :index_name, :ctx]

  @typedoc """
  Secondary-index source type for query predicates.

  - `:default` — scalar bin index
  - `:list` — list element index
  - `:mapkeys` — map-key index
  - `:mapvalues` — map-value index
  - `:geo_within` — points-within-region geospatial query
  - `:geo_contains` — region-contains-point geospatial query
  """
  @type index_type :: :default | :list | :mapkeys | :mapvalues | :geo_within | :geo_contains

  @typedoc """
  Indexed value family used in filter predicate encoding.

  This client currently supports signed int64 (`:integer`) and UTF-8 string (`:string`)
  filter values.
  """
  @type particle_type :: :integer | :string

  @type t :: %__MODULE__{
          bin_name: String.t(),
          index_type: index_type(),
          particle_type: particle_type(),
          begin: term(),
          end: term(),
          index_name: String.t() | nil,
          ctx: term() | nil
        }

  @type geo_geometry :: String.t() | Geo.Point.t() | Geo.Polygon.t() | Geo.Circle.t()

  @doc """
  Numeric range on a bin (signed 64-bit endpoints, inclusive).
  """
  @spec range(String.t(), integer(), integer()) :: t()
  def range(bin_name, begin_val, end_val)
      when is_binary(bin_name) and is_integer(begin_val) and is_integer(end_val) do
    validate_bin_name!(bin_name)
    Key.validate_int64!(begin_val, "range begin")
    Key.validate_int64!(end_val, "range end")

    if begin_val > end_val do
      raise ArgumentError, "range begin must be <= end, got #{begin_val}..#{end_val}"
    end

    %__MODULE__{
      bin_name: bin_name,
      index_type: :default,
      particle_type: :integer,
      begin: begin_val,
      end: end_val
    }
  end

  @doc """
  Equality on a bin. Particle type is inferred from the value (`integer` or UTF-8 string).
  """
  @spec equal(String.t(), integer() | String.t()) :: t()
  def equal(bin_name, value) when is_binary(bin_name) do
    validate_bin_name!(bin_name)

    {particle_type, begin_val, end_val} =
      cond do
        is_integer(value) ->
          Key.validate_int64!(value, "equal value")
          {:integer, value, value}

        is_binary(value) ->
          {:string, value, value}

        true ->
          raise ArgumentError,
                "equal/2 value must be integer or string, got: #{inspect(value)}"
      end

    %__MODULE__{
      bin_name: bin_name,
      index_type: :default,
      particle_type: particle_type,
      begin: begin_val,
      end: end_val
    }
  end

  @doc """
  CDT index membership filter.

  `index_type` must be `:list`, `:mapkeys`, or `:mapvalues`. The value must be an integer
  (64-bit) or a string.
  """
  @spec contains(String.t(), :list | :mapkeys | :mapvalues, integer() | String.t()) :: t()
  def contains(bin_name, index_type, value) when is_binary(bin_name) do
    validate_bin_name!(bin_name)

    unless index_type in [:list, :mapkeys, :mapvalues] do
      raise ArgumentError,
            "contains/3 index_type must be :list, :mapkeys, or :mapvalues, got: #{inspect(index_type)}"
    end

    {particle_type, begin_val, end_val} =
      cond do
        is_integer(value) ->
          Key.validate_int64!(value, "contains value")
          {:integer, value, value}

        is_binary(value) ->
          {:string, value, value}

        true ->
          raise ArgumentError,
                "contains/3 value must be integer or string, got: #{inspect(value)}"
      end

    %__MODULE__{
      bin_name: bin_name,
      index_type: index_type,
      particle_type: particle_type,
      begin: begin_val,
      end: end_val
    }
  end

  @doc """
  Geo region query: bins whose geo index falls within `region` (GeoJSON string).
  """
  @spec geo_within(String.t(), geo_geometry()) :: t()
  def geo_within(bin_name, %Geo.Point{} = region) when is_binary(bin_name) do
    geo_within(bin_name, Geo.to_json(region))
  end

  def geo_within(bin_name, %Geo.Polygon{} = region) when is_binary(bin_name) do
    geo_within(bin_name, Geo.to_json(region))
  end

  def geo_within(bin_name, %Geo.Circle{} = region) when is_binary(bin_name) do
    geo_within(bin_name, Geo.to_json(region))
  end

  def geo_within(bin_name, region) when is_binary(bin_name) and is_binary(region) do
    validate_bin_name!(bin_name)

    if region == "" do
      raise ArgumentError, "geo_within/2 region must be a non-empty GeoJSON string"
    end

    %__MODULE__{
      bin_name: bin_name,
      index_type: :geo_within,
      particle_type: :string,
      begin: region,
      end: region
    }
  end

  @doc """
  Geo point lookup: bins whose region contains `point` (GeoJSON string).
  """
  @spec geo_contains(String.t(), geo_geometry()) :: t()
  def geo_contains(bin_name, %Geo.Point{} = point) when is_binary(bin_name) do
    geo_contains(bin_name, Geo.to_json(point))
  end

  def geo_contains(bin_name, %Geo.Polygon{} = point) when is_binary(bin_name) do
    geo_contains(bin_name, Geo.to_json(point))
  end

  def geo_contains(bin_name, %Geo.Circle{} = point) when is_binary(bin_name) do
    geo_contains(bin_name, Geo.to_json(point))
  end

  def geo_contains(bin_name, point) when is_binary(bin_name) and is_binary(point) do
    validate_bin_name!(bin_name)

    if point == "" do
      raise ArgumentError, "geo_contains/2 point must be a non-empty GeoJSON string"
    end

    %__MODULE__{
      bin_name: bin_name,
      index_type: :geo_contains,
      particle_type: :string,
      begin: point,
      end: point
    }
  end

  @doc """
  Convenience helper for a `geo_within/2` circle query.
  """
  @spec geo_within_radius(String.t(), number(), number(), number()) :: t()
  def geo_within_radius(bin_name, lng, lat, radius)
      when is_binary(bin_name) and is_number(lng) and is_number(lat) and is_number(radius) do
    geo_within(bin_name, Geo.circle(lng, lat, radius))
  end

  @doc """
  Convenience helper for a `geo_contains/2` point query.
  """
  @spec geo_contains_point(String.t(), number(), number()) :: t()
  def geo_contains_point(bin_name, lng, lat)
      when is_binary(bin_name) and is_number(lng) and is_number(lat) do
    geo_contains(bin_name, Geo.point(lng, lat))
  end

  defp validate_bin_name!(name) do
    if name == "" do
      raise ArgumentError, "bin name must be a non-empty string"
    end
  end
end
