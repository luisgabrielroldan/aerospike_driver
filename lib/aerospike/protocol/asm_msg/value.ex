defmodule Aerospike.Protocol.AsmMsg.Value do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Protocol.AsmMsg.Operation

  @particle_null 0
  @particle_integer 1
  @particle_float 2
  @particle_string 3
  @particle_blob 4
  @particle_bool 17
  @op_write Operation.op_write()
  @op_add Operation.op_add()
  @op_append Operation.op_append()
  @op_prepend Operation.op_prepend()

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
  Builds write operations from a bin map.

  Keys may be atoms or strings; atom keys are converted with `Atom.to_string/1`.
  """
  @spec encode_bin_operations(%{optional(atom() | String.t()) => term()}) :: [Operation.t()]
  def encode_bin_operations(bins) when is_map(bins) do
    encode_bin_operations(bins, Operation.op_write())
  end

  @doc """
  Builds operations from a bin map with the specified operation type.
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
      {:ok, {particle_type, data}} = encode_value_for_op(op_type, name, value)

      %Operation{op_type: op_type, particle_type: particle_type, bin_name: name, data: data}
    end)
  end

  defp encode_value_for_op(op_type, _name, value) when op_type == @op_write do
    encode_value(value)
  end

  defp encode_value_for_op(op_type, name, value) when op_type == @op_add do
    unless is_integer(value) do
      raise ArgumentError, "add requires integer values, got #{inspect(value)} for bin #{name}"
    end

    encode_value(value)
  end

  defp encode_value_for_op(op_type, name, value)
       when op_type in [@op_append, @op_prepend] do
    unless is_binary(value) do
      op_name = if op_type == @op_append, do: "append", else: "prepend"

      raise ArgumentError,
            "#{op_name} requires string values, got #{inspect(value)} for bin #{name}"
    end

    encode_value(value)
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
