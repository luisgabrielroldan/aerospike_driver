defmodule Aerospike.Protocol.AsmMsg.Value do
  @moduledoc false

  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg.Operation

  @particle_null 0
  @particle_integer 1
  @particle_float 2
  @particle_string 3
  @particle_blob 4
  @particle_bool 17

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

  def decode_value(particle_type, data) when is_integer(particle_type) and is_binary(data) do
    {:raw, particle_type, data}
  end

  @doc """
  Encodes an Elixir term to `{particle_type, data}` for wire use.

  Supported: integers (int64 range), floats, binaries (UTF-8 strings), booleans, nil.

  Raises `ArgumentError` for unsupported terms.
  """
  @spec encode_value(term()) :: {non_neg_integer(), binary()}
  def encode_value(nil), do: {@particle_null, <<>>}

  def encode_value(n) when is_integer(n) do
    Key.validate_int64!(n, "integer bin value")
    {@particle_integer, <<n::64-signed-big>>}
  end

  def encode_value(f) when is_float(f), do: {@particle_float, <<f::64-float-big>>}

  def encode_value(s) when is_binary(s), do: {@particle_string, s}

  def encode_value(true), do: {@particle_bool, <<1>>}
  def encode_value(false), do: {@particle_bool, <<0>>}

  def encode_value(other) do
    raise ArgumentError, "unsupported bin value type: #{inspect(other)}"
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
