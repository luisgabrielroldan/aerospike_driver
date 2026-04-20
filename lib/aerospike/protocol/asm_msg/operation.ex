defmodule Aerospike.Protocol.AsmMsg.Operation do
  @moduledoc false

  # `read_header`: when true, this is a header-only read (generation/TTL, no bins).
  # Wire `op_type` matches `READ` (1); flagging is used only for operate header flags.
  #
  # `map_cdt`: true for map collection op types; same wire op bytes as list CDT but
  # operate-args treats them like map for RESPOND_ALL_OPS.
  defstruct op_type: 0,
            particle_type: 0,
            bin_name: "",
            data: <<>>,
            read_header: false,
            map_cdt: false

  @type t :: %__MODULE__{
          op_type: non_neg_integer(),
          particle_type: non_neg_integer(),
          bin_name: String.t(),
          data: binary(),
          read_header: boolean(),
          map_cdt: boolean()
        }

  @doc """
  Encodes an operation into binary format.

  Operation header format (8 bytes + name + data):
  - 4 bytes: size (4 + name_length + data_length)
  - 1 byte: operation type
  - 1 byte: particle type
  - 1 byte: reserved (always 0)
  - 1 byte: bin name length
  - N bytes: bin name
  - M bytes: data

  ## Examples

      iex> op = %Aerospike.Protocol.AsmMsg.Operation{op_type: 2, particle_type: 3, bin_name: "bin", data: "value"}
      iex> encoded = Aerospike.Protocol.AsmMsg.Operation.encode(op)
      iex> byte_size(encoded)
      16

  """
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{
        op_type: op_type,
        particle_type: particle_type,
        bin_name: bin_name,
        data: data
      }) do
    name_len = byte_size(bin_name)
    size = 4 + name_len + byte_size(data)

    <<size::32-big, op_type::8, particle_type::8, 0::8, name_len::8, bin_name::binary,
      data::binary>>
  end

  @doc """
  Decodes an operation from binary format.

  Returns `{:ok, operation, rest}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> binary = <<0, 0, 0, 12, 2, 3, 0, 3, "bin", "value", "rest">>
      iex> {:ok, op, remaining} = Aerospike.Protocol.AsmMsg.Operation.decode(binary)
      iex> op.op_type
      2
      iex> op.bin_name
      "bin"
      iex> op.data
      "value"
      iex> remaining
      "rest"

  """
  @spec decode(binary()) :: {:ok, t(), binary()} | {:error, atom()}
  def decode(
        <<size::32-big, op_type::8, particle_type::8, _reserved::8, name_len::8, rest::binary>>
      )
      when size >= 4 do
    data_len = size - 4 - name_len

    if byte_size(rest) >= name_len + data_len do
      <<bin_name::binary-size(name_len), data::binary-size(data_len), remaining::binary>> = rest

      {:ok,
       %__MODULE__{
         op_type: op_type,
         particle_type: particle_type,
         bin_name: bin_name,
         data: data
       }, remaining}
    else
      {:error, :incomplete_operation}
    end
  end

  def decode(<<_size::32-big, _rest::binary>>), do: {:error, :invalid_operation_size}
  def decode(_), do: {:error, :incomplete_operation_header}
end
