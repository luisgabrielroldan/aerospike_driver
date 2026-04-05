defmodule Aerospike.Protocol.AsmMsg.Value do
  @moduledoc false

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

  @doc """
  Decodes wire particle bytes to an Elixir term.

  Unsupported particle types return `{:raw, particle_type, data}`.
  """
  @spec decode_value(non_neg_integer(), binary()) :: term()
  def decode_value(@particle_null, <<>>), do: nil

  def decode_value(@particle_integer, <<n::64-signed-big>>), do: n

  def decode_value(@particle_float, <<f::64-float-big>>), do: f

  def decode_value(@particle_string, data) when is_binary(data), do: data

  def decode_value(@particle_blob, data) when is_binary(data), do: {:blob, data}

  def decode_value(@particle_bool, <<0>>), do: false
  def decode_value(@particle_bool, <<1>>), do: true

  def decode_value(@particle_map, data) when is_binary(data) do
    data |> MessagePack.unpack!() |> normalize_msgpack_particle_strings()
  end

  def decode_value(@particle_list, data) when is_binary(data) do
    data |> MessagePack.unpack!() |> normalize_msgpack_particle_strings()
  end

  # GeoJSON is UTF-8 text on the wire (same layout as STRING).
  def decode_value(@particle_geojson, data) when is_binary(data) do
    data
  end

  def decode_value(particle_type, data) when is_integer(particle_type) and is_binary(data) do
    {:raw, particle_type, data}
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
    Enum.map(list, &normalize_msgpack_particle_strings/1)
  end

  defp normalize_msgpack_particle_strings(map) when is_map(map) do
    Map.new(map, fn {k, v} ->
      {normalize_msgpack_particle_strings(k), normalize_msgpack_particle_strings(v)}
    end)
  end

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
    bins
    |> Enum.map(fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} when is_binary(k) -> {k, v}
      {k, _v} -> raise ArgumentError, "bin name must be a string or atom, got: #{inspect(k)}"
    end)
    |> Enum.sort_by(fn {name, _} -> name end)
    |> Enum.map(fn {name, value} ->
      {pt, data} = encode_value(value)
      %Operation{op_type: Operation.op_write(), particle_type: pt, bin_name: name, data: data}
    end)
  end
end
