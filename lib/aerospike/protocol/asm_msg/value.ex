defmodule Aerospike.Protocol.AsmMsg.Value do
  @moduledoc false

  alias Aerospike.Error

  @particle_null 0
  @particle_integer 1
  @particle_float 2
  @particle_string 3
  @particle_blob 4
  @particle_bool 17

  @doc """
  Encodes an Elixir value into `{particle_type, data}` for a simple write op.

  Supported values are the narrow subset this spike phase needs:
  `nil`, integers, floats, strings, booleans, and explicit blobs via
  `{:blob, binary}`.
  """
  @spec encode_value(term()) ::
          {:ok, {non_neg_integer(), binary()}} | {:error, Error.t()}
  def encode_value(nil), do: {:ok, {@particle_null, <<>>}}

  def encode_value(value) when is_integer(value) do
    {:ok, {@particle_integer, <<value::64-signed-big>>}}
  end

  def encode_value(value) when is_float(value) do
    {:ok, {@particle_float, <<value::64-float-big>>}}
  end

  def encode_value(value) when is_binary(value) do
    {:ok, {@particle_string, value}}
  end

  def encode_value(true), do: {:ok, {@particle_bool, <<1>>}}
  def encode_value(false), do: {:ok, {@particle_bool, <<0>>}}

  def encode_value({:blob, value}) when is_binary(value) do
    {:ok, {@particle_blob, value}}
  end

  def encode_value(value) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message:
         "unsupported write particle #{inspect(value)}; supported values: nil, integer, float, binary, boolean, {:blob, binary}"
     )}
  end

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

  def decode_value(particle_type, data) when is_integer(particle_type) and is_binary(data) do
    {:ok, {:raw, particle_type, data}}
  end
end
