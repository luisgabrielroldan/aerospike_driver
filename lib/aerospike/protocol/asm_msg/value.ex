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
