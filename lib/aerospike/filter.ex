defmodule Aerospike.Filter do
  @moduledoc """
  Secondary-index predicate values for query builders.

  Use `range/3`, `equal/2`, `contains/3`, `geo_within/2`, or
  `geo_contains/2` to build a predicate, then pass it to
  `Aerospike.Query.where/2`.

  `using_index/2` targets a named index, and `with_ctx/2` carries nested
  CDT context for context-aware predicates.
  """

  alias Aerospike.Ctx
  alias Aerospike.Geo
  alias Aerospike.Key

  @enforce_keys [:bin_name, :index_type, :particle_type, :begin, :end]
  defstruct [:bin_name, :index_type, :particle_type, :begin, :end, :index_name, :ctx]

  @typedoc """
  Secondary-index filter kind encoded for the query predicate.

  `:list`, `:mapkeys`, and `:mapvalues` target CDT collection indexes.
  `:geo_within` and `:geo_contains` target geospatial predicates.
  """
  @type index_type :: :default | :list | :mapkeys | :mapvalues | :geo_within | :geo_contains

  @typedoc "Indexed scalar particle type inferred for integer and string predicates."
  @type particle_type :: :integer | :string

  @typedoc "Scalar value accepted by equality and CDT membership filters."
  @type scalar_value :: integer() | String.t()

  @typedoc "Collection secondary-index kind accepted by `contains/3`."
  @type collection_index_type :: :list | :mapkeys | :mapvalues

  @typedoc """
  GeoJSON input accepted by geospatial query filters.

  Binary inputs are expected to be non-empty GeoJSON strings. Typed
  `Aerospike.Geo` values are converted with `Aerospike.Geo.to_json/1`.
  """
  @type geo_geometry :: String.t() | Geo.Point.t() | Geo.Polygon.t() | Geo.Circle.t()

  @typedoc """
  Secondary-index query filter consumed by `Aerospike.Query.where/2`.

  A query can carry one secondary-index filter. `index_name` is optional and is
  set by `using_index/2`; `ctx` is optional nested-CDT context set by
  `with_ctx/2`.
  """
  @type t :: %__MODULE__{
          bin_name: String.t(),
          index_type: index_type(),
          particle_type: particle_type(),
          begin: term(),
          end: term(),
          index_name: String.t() | nil,
          ctx: [Ctx.step()] | nil
        }

  @doc """
  Numeric range on a bin, inclusive.

  `begin_val` and `end_val` must fit in Aerospike's signed int64 range.
  Raises `ArgumentError` when the bin name is empty, a value is out of range,
  or the range is inverted.
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
  Equality on a bin. The particle type is inferred from the value.

  Supports integer and string values. Integer values must fit in Aerospike's
  signed int64 range.
  """
  @spec equal(String.t(), scalar_value()) :: t()
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
          raise ArgumentError, "equal/2 value must be integer or string, got: #{inspect(value)}"
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
  CDT membership filter for list or map indexes.

  `index_type` must match the collection index created for the bin:
  `:list`, `:mapkeys`, or `:mapvalues`.
  """
  @spec contains(String.t(), collection_index_type(), scalar_value()) :: t()
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
  Geo region query for points within a GeoJSON region.

  Use this with a `:geo2dsphere` index on point data. The region may be a
  GeoJSON string or a typed `Aerospike.Geo` value.
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
    validate_geojson!(region, "geo_within/2 region")

    %__MODULE__{
      bin_name: bin_name,
      index_type: :geo_within,
      particle_type: :string,
      begin: region,
      end: region
    }
  end

  @doc """
  Geo point query for regions containing a GeoJSON point.

  Use this with a `:geo2dsphere` index on region data. The point may be a
  GeoJSON string or a typed `Aerospike.Geo` value.
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
    validate_geojson!(point, "geo_contains/2 point")

    %__MODULE__{
      bin_name: bin_name,
      index_type: :geo_contains,
      particle_type: :string,
      begin: point,
      end: point
    }
  end

  @doc """
  Builds a `geo_within/2` circle query from center longitude, latitude, and radius.
  """
  @spec geo_within_radius(String.t(), number(), number(), number()) :: t()
  def geo_within_radius(bin_name, lng, lat, radius)
      when is_binary(bin_name) and is_number(lng) and is_number(lat) and is_number(radius) do
    geo_within(bin_name, Geo.circle(lng, lat, radius))
  end

  @doc """
  Builds a `geo_contains/2` point query from longitude and latitude.
  """
  @spec geo_contains_point(String.t(), number(), number()) :: t()
  def geo_contains_point(bin_name, lng, lat)
      when is_binary(bin_name) and is_number(lng) and is_number(lat) do
    geo_contains(bin_name, Geo.point(lng, lat))
  end

  @doc """
  Targets a named secondary index.

  Use this when the server has multiple compatible indexes and the query
  should address one index explicitly.
  """
  @spec using_index(t(), String.t()) :: t()
  def using_index(%__MODULE__{} = filter, index_name) when is_binary(index_name) do
    validate_index_name!(index_name)
    %{filter | index_name: index_name}
  end

  @doc """
  Attaches nested CDT context to the filter.

  `ctx` must be a non-empty list of `Aerospike.Ctx` steps that points at the
  nested collection value indexed by the server.
  """
  @spec with_ctx(t(), [Ctx.step()]) :: t()
  def with_ctx(%__MODULE__{} = filter, ctx) when is_list(ctx) do
    validate_ctx!(ctx)
    %{filter | ctx: ctx}
  end

  defp validate_bin_name!(bin_name) do
    if bin_name == "" do
      raise ArgumentError, "bin_name must be a non-empty string"
    end
  end

  defp validate_index_name!(index_name) do
    if index_name == "" do
      raise ArgumentError, "index_name must be a non-empty string"
    end
  end

  defp validate_geojson!(json, subject) do
    if json == "" do
      raise ArgumentError, "#{subject} must be a non-empty GeoJSON string"
    end
  end

  defp validate_ctx!([]), do: raise(ArgumentError, "ctx must be a non-empty list")
  defp validate_ctx!(_ctx), do: :ok
end
