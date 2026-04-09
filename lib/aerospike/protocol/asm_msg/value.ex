defmodule Aerospike.Protocol.AsmMsg.Value do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Geo
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  @particle_null 0
  @particle_integer 1
  @particle_float 2
  @particle_string 3
  @particle_blob 4
  @particle_bool 17
  @particle_map 19
  @particle_list 20
  @particle_geojson 23
  @ordered_collection_sentinel_ext_types [1, 3, 8]

  @doc """
  Decodes wire particle bytes to an Elixir term.

  Returns `{:ok, term}` on success or `{:error, Error.t()}` on failure.
  Unsupported particle types return `{:ok, {:raw, particle_type, data}}`.
  """
  @spec decode_value(non_neg_integer(), binary()) :: {:ok, term()} | {:error, Error.t()}
  def decode_value(@particle_null, <<>>), do: {:ok, nil}

  def decode_value(@particle_integer, <<n::64-signed-big>>), do: {:ok, n}

  def decode_value(@particle_float, <<f::64-float-big>>), do: {:ok, f}

  def decode_value(@particle_string, data) when is_binary(data), do: {:ok, data}

  def decode_value(@particle_blob, data) when is_binary(data), do: {:ok, {:blob, data}}

  def decode_value(@particle_bool, <<0>>), do: {:ok, false}
  def decode_value(@particle_bool, <<1>>), do: {:ok, true}

  def decode_value(@particle_map, data) when is_binary(data) do
    case MessagePack.unpack(data) do
      {:ok, {packed, <<>>}} ->
        {:ok, normalize_msgpack_particle_strings(packed)}

      {:ok, {_, rest}} ->
        {:error,
         Error.from_result_code(:parse_error,
           message: "MessagePack map: trailing bytes (#{byte_size(rest)})"
         )}

      {:error, :invalid_msgpack} ->
        {:error, Error.from_result_code(:parse_error, message: "invalid MessagePack map")}
    end
  end

  def decode_value(@particle_list, data) when is_binary(data) do
    case MessagePack.unpack(data) do
      {:ok, {packed, <<>>}} ->
        {:ok, normalize_msgpack_particle_strings(packed)}

      {:ok, {_, rest}} ->
        {:error,
         Error.from_result_code(:parse_error,
           message: "MessagePack list: trailing bytes (#{byte_size(rest)})"
         )}

      {:error, :invalid_msgpack} ->
        {:error, Error.from_result_code(:parse_error, message: "invalid MessagePack list")}
    end
  end

  # GeoJSON wire layout: 1 byte flags + 2 bytes ncells + (ncells * 8) cell bytes + JSON string.
  def decode_value(@particle_geojson, <<_flags::8, ncells::16-big, rest::binary>>)
      when is_binary(rest) do
    cell_bytes = ncells * 8

    case rest do
      <<_cells::binary-size(cell_bytes), json::binary>> -> {:ok, Geo.from_json(json)}
      _ -> {:ok, {:geojson, rest}}
    end
  end

  def decode_value(@particle_geojson, data) when is_binary(data) do
    {:ok, Geo.from_json(data)}
  end

  def decode_value(particle_type, data) when is_integer(particle_type) and is_binary(data) do
    {:ok, {:raw, particle_type, data}}
  end

  # MessagePack strings inside MAP/LIST particle bins often carry Aerospike's
  # STRING particle type (0x03) as the first payload byte; strip it so callers
  # see plain UTF-8 binaries.
  defp normalize_msgpack_particle_strings(term)

  defp normalize_msgpack_particle_strings(nil), do: nil
  defp normalize_msgpack_particle_strings(true), do: true
  defp normalize_msgpack_particle_strings(false), do: false
  defp normalize_msgpack_particle_strings(n) when is_integer(n), do: n
  defp normalize_msgpack_particle_strings(f) when is_float(f), do: f
  defp normalize_msgpack_particle_strings({:ext, _, _} = ext), do: ext
  defp normalize_msgpack_particle_strings({:blob, b}) when is_binary(b), do: {:blob, b}

  defp normalize_msgpack_particle_strings(bin) when is_binary(bin) do
    case bin do
      <<@particle_string, rest::binary>> -> rest
      _ -> bin
    end
  end

  defp normalize_msgpack_particle_strings(list) when is_list(list) do
    list
    |> Enum.map(&normalize_msgpack_particle_strings/1)
    |> drop_ordered_list_sentinel()
  end

  defp normalize_msgpack_particle_strings(map) when is_map(map) do
    map
    |> Map.new(fn {k, v} ->
      {normalize_msgpack_particle_strings(k), normalize_msgpack_particle_strings(v)}
    end)
    |> drop_ordered_map_sentinel()
  end

  defp drop_ordered_map_sentinel(map) when is_map(map) do
    map
    |> Enum.reject(fn
      {key, nil} -> ordered_collection_sentinel_key?(key)
      _ -> false
    end)
    |> Map.new()
  end

  defp drop_ordered_list_sentinel([head | tail]) do
    if ordered_collection_sentinel_key?(head), do: tail, else: [head | tail]
  end

  defp drop_ordered_list_sentinel([]), do: []

  defp ordered_collection_sentinel_key?({:ext, type, payload})
       when is_integer(type) and type in @ordered_collection_sentinel_ext_types and
              payload == <<>> do
    true
  end

  defp ordered_collection_sentinel_key?(_other), do: false

  @doc """
  Encodes an Elixir term to `{particle_type, data}` for wire use.

  Supported: integers (int64 range), floats, binaries (UTF-8 strings), booleans, nil.

  Raises `ArgumentError` for unsupported terms.
  """
  @spec encode_value(term()) :: {non_neg_integer(), binary()}
  def encode_value(nil), do: {@particle_null, <<>>}

  def encode_value({:bytes, bin}) when is_binary(bin) do
    {@particle_blob, bin}
  end

  def encode_value(%Geo.Point{} = point), do: encode_value({:geojson, Geo.to_json(point)})
  def encode_value(%Geo.Polygon{} = polygon), do: encode_value({:geojson, Geo.to_json(polygon)})
  def encode_value(%Geo.Circle{} = circle), do: encode_value({:geojson, Geo.to_json(circle)})

  def encode_value({:geojson, json}) when is_binary(json) do
    # Wire layout: 1 byte flags + 2 bytes ncells (0) + JSON string.
    {@particle_geojson, <<0::8, 0::16-big, json::binary>>}
  end

  def encode_value(n) when is_integer(n) do
    Key.validate_int64!(n, "integer bin value")
    {@particle_integer, <<n::64-signed-big>>}
  end

  def encode_value(f) when is_float(f), do: {@particle_float, <<f::64-float-big>>}

  def encode_value(s) when is_binary(s), do: {@particle_string, s}

  def encode_value(true), do: {@particle_bool, <<1>>}
  def encode_value(false), do: {@particle_bool, <<0>>}

  def encode_value(list) when is_list(list) do
    {@particle_list, MessagePack.pack!(Enum.map(list, &pack_cdt_term/1))}
  end

  def encode_value(map) when is_map(map) do
    mp =
      map
      |> Map.new(fn {k, v} -> {pack_cdt_map_key(k), pack_cdt_term(v)} end)
      |> MessagePack.pack!()

    {@particle_map, mp}
  end

  def encode_value(other) do
    raise ArgumentError, "unsupported bin value type: #{inspect(other)}"
  end

  defp pack_cdt_term(list) when is_list(list), do: Enum.map(list, &pack_cdt_term/1)

  defp pack_cdt_term(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {pack_cdt_map_key(k), pack_cdt_term(v)} end)
  end

  defp pack_cdt_term(nil), do: nil
  defp pack_cdt_term(true), do: true
  defp pack_cdt_term(false), do: false

  defp pack_cdt_term(n) when is_integer(n) do
    Key.validate_int64!(n, "CDT element")
    n
  end

  defp pack_cdt_term(f) when is_float(f), do: f

  # Stored MAP/LIST MessagePack uses Aerospike STRING particle wrapping for UTF-8
  # text (same as CDT operation args); raw octets use `{:bytes, binary}`.
  defp pack_cdt_term(s) when is_binary(s), do: {:particle_string, s}

  defp pack_cdt_term({:bytes, b}) when is_binary(b), do: {:bytes, b}

  defp pack_cdt_term(other) do
    raise ArgumentError, "unsupported CDT/map/list element: #{inspect(other)}"
  end

  defp pack_cdt_map_key(k) when is_binary(k), do: {:particle_string, k}
  defp pack_cdt_map_key(k) when is_atom(k), do: {:particle_string, Atom.to_string(k)}

  defp pack_cdt_map_key(k) do
    raise ArgumentError, "map key must be string or atom, got: #{inspect(k)}"
  end

  @doc """
  Builds write operations from a bin map.

  Keys may be atoms or strings; atom keys are converted with `Atom.to_string/1`.
  """
  @spec encode_bin_operations(%{optional(atom() | String.t()) => term()}) :: [Operation.t()]
  def encode_bin_operations(bins) when is_map(bins) do
    encode_bin_operations(bins, Operation.op_write())
  end

  @doc """
  Builds operations from a bin map with the specified operation type.

  - `:write` (2) — standard put
  - `:add` (5) — atomic integer increment (values must be integers)
  - `:append` (9) — atomic string append (values must be strings)
  - `:prepend` (10) — atomic string prepend (values must be strings)
  """
  @spec encode_bin_operations(%{optional(atom() | String.t()) => term()}, non_neg_integer()) ::
          [Operation.t()]
  def encode_bin_operations(bins, op_type) when is_map(bins) and is_integer(op_type) do
    bins
    |> Enum.map(fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} when is_binary(k) -> {k, v}
      {k, _v} -> raise ArgumentError, "bin name must be a string or atom, got: #{inspect(k)}"
    end)
    |> Enum.sort_by(fn {name, _} -> name end)
    |> Enum.map(fn {name, value} ->
      {pt, data} = encode_value_for_op(op_type, name, value)
      %Operation{op_type: op_type, particle_type: pt, bin_name: name, data: data}
    end)
  end

  defp encode_value_for_op(op_type, _name, value) when op_type == 2 do
    encode_value(value)
  end

  defp encode_value_for_op(op_type, name, value) when op_type == 5 do
    unless is_integer(value) do
      raise ArgumentError, "add requires integer values, got #{inspect(value)} for bin #{name}"
    end

    Key.validate_int64!(value, "add bin value")
    {@particle_integer, <<value::64-signed-big>>}
  end

  defp encode_value_for_op(op_type, name, value) when op_type in [9, 10] do
    unless is_binary(value) do
      op_name = if op_type == 9, do: "append", else: "prepend"

      raise ArgumentError,
            "#{op_name} requires string values, got #{inspect(value)} for bin #{name}"
    end

    {@particle_string, value}
  end
end
