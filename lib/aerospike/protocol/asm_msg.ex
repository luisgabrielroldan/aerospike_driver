defmodule Aerospike.Protocol.AsmMsg do
  @moduledoc false

  import Bitwise

  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation

  @msg_remaining_header_size 22

  # Info1 flags
  @info1_read 0x01
  @info1_get_all 0x02

  defstruct info1: 0,
            info2: 0,
            info3: 0,
            info4: 0,
            result_code: 0,
            generation: 0,
            expiration: 0,
            timeout: 0,
            fields: [],
            operations: []

  @type t :: %__MODULE__{
          info1: non_neg_integer(),
          info2: non_neg_integer(),
          info3: non_neg_integer(),
          info4: non_neg_integer(),
          result_code: non_neg_integer(),
          generation: non_neg_integer(),
          expiration: non_neg_integer(),
          timeout: integer(),
          fields: [Field.t()],
          operations: [Operation.t()]
        }

  @doc "Returns the INFO1_READ flag value."
  @spec info1_read() :: 0x01
  def info1_read, do: @info1_read

  @doc "Returns the INFO1_GET_ALL flag value."
  @spec info1_get_all() :: 0x02
  def info1_get_all, do: @info1_get_all

  @doc """
  Encodes an AsmMsg struct into binary format.

  The encoding produces:
  - 22-byte AS_MSG header
  - Encoded fields (field_count fields)
  - Encoded operations (operation_count operations)

  Note: This does NOT include the 8-byte protocol header.

  ## Examples

      iex> msg = %Aerospike.Protocol.AsmMsg{info1: 1, info2: 0, info3: 0, info4: 0}
      iex> encoded = Aerospike.Protocol.AsmMsg.encode(msg)
      iex> IO.iodata_length(encoded)
      22

  """
  @spec encode(t()) :: iodata()
  def encode(%__MODULE__{} = msg) do
    field_count = length(msg.fields)
    op_count = length(msg.operations)

    header =
      <<@msg_remaining_header_size::8, msg.info1::8, msg.info2::8, msg.info3::8, msg.info4::8,
        msg.result_code::8, msg.generation::32-big, msg.expiration::32-big,
        msg.timeout::32-big-signed, field_count::16-big, op_count::16-big>>

    [
      header
      | Enum.map(msg.fields, &Field.encode/1) ++ Enum.map(msg.operations, &Operation.encode/1)
    ]
  end

  @doc """
  Decodes a binary AS_MSG (without the 8-byte protocol header) into an AsmMsg struct.

  Returns `{:ok, msg}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> binary = <<22, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>
      iex> {:ok, msg} = Aerospike.Protocol.AsmMsg.decode(binary)
      iex> msg.info1
      1

  """
  @spec decode(binary()) :: {:ok, t()} | {:error, atom()}
  def decode(<<
        @msg_remaining_header_size::8,
        info1::8,
        info2::8,
        info3::8,
        info4::8,
        result_code::8,
        generation::32-big,
        expiration::32-big,
        timeout::32-big-signed,
        field_count::16-big,
        op_count::16-big,
        rest::binary
      >>) do
    with {:ok, fields, rest} <- decode_fields(rest, field_count),
         {:ok, operations, _rest} <- decode_operations(rest, op_count) do
      {:ok,
       %__MODULE__{
         info1: info1,
         info2: info2,
         info3: info3,
         info4: info4,
         result_code: result_code,
         generation: generation,
         expiration: expiration,
         timeout: timeout,
         fields: fields,
         operations: operations
       }}
    end
  end

  def decode(<<header_size::8, _rest::binary>>) when header_size != @msg_remaining_header_size do
    {:error, :invalid_header_size}
  end

  def decode(_), do: {:error, :incomplete_header}

  defp decode_fields(binary, 0), do: {:ok, [], binary}

  defp decode_fields(binary, count) do
    decode_fields(binary, count, [])
  end

  defp decode_fields(binary, 0, acc), do: {:ok, Enum.reverse(acc), binary}

  defp decode_fields(binary, count, acc) do
    case Field.decode(binary) do
      {:ok, field, rest} -> decode_fields(rest, count - 1, [field | acc])
      {:error, _} = error -> error
    end
  end

  defp decode_operations(binary, 0), do: {:ok, [], binary}

  defp decode_operations(binary, count) do
    decode_operations(binary, count, [])
  end

  defp decode_operations(binary, 0, acc), do: {:ok, Enum.reverse(acc), binary}

  defp decode_operations(binary, count, acc) do
    case Operation.decode(binary) do
      {:ok, op, rest} -> decode_operations(rest, count - 1, [op | acc])
      {:error, _} = error -> error
    end
  end

  @doc """
  Creates a simple read command (GET).
  """
  @spec read_command(binary(), binary(), binary()) :: t()
  def read_command(namespace, set, digest) do
    %__MODULE__{
      info1: @info1_read ||| @info1_get_all,
      fields: [
        Field.namespace(namespace),
        Field.set(set),
        Field.digest(digest)
      ]
    }
  end
end
