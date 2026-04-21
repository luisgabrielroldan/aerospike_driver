defmodule Aerospike.Protocol.AsmMsg.Operation do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Protocol.AsmMsg.Value

  @op_read 1
  @op_write 2
  @op_touch 11
  @op_delete 14

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

  @doc "Returns the READ operation type."
  @spec op_read() :: 1
  def op_read, do: @op_read

  @doc "Returns the WRITE operation type."
  @spec op_write() :: 2
  def op_write, do: @op_write

  @doc "Returns the TOUCH operation type."
  @spec op_touch() :: 11
  def op_touch, do: @op_touch

  @doc "Returns the DELETE operation type."
  @spec op_delete() :: 14
  def op_delete, do: @op_delete

  @doc """
  Builds a named-bin read operation.
  """
  @spec read(String.t()) :: t()
  def read(bin_name) when is_binary(bin_name) and byte_size(bin_name) > 0 do
    %__MODULE__{op_type: @op_read, bin_name: bin_name}
  end

  @doc """
  Builds one supported simple operation from a narrow internal tuple shape.
  """
  @spec from_simple(term()) :: {:ok, t()} | {:error, Error.t()}
  def from_simple({:read, bin_name}) when is_binary(bin_name) and byte_size(bin_name) > 0 do
    {:ok, read(bin_name)}
  end

  def from_simple({:write, bin_name, value}) do
    write(bin_name, value)
  end

  def from_simple(:touch), do: {:ok, touch()}
  def from_simple(:delete), do: {:ok, delete()}

  def from_simple(other) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message:
         "unsupported simple operation #{inspect(other)}; supported shapes: {:read, bin}, {:write, bin, value}, :touch, :delete"
     )}
  end

  @doc """
  Builds a simple write operation for the supported spike value subset.
  """
  @spec write(String.t(), term()) :: {:ok, t()} | {:error, Error.t()}
  def write(bin_name, value) when is_binary(bin_name) and byte_size(bin_name) > 0 do
    with {:ok, {particle_type, data}} <- Value.encode_value(value) do
      {:ok,
       %__MODULE__{
         op_type: @op_write,
         particle_type: particle_type,
         bin_name: bin_name,
         data: data
       }}
    end
  end

  def write(bin_name, _value) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "write bin name must be a non-empty binary, got: #{inspect(bin_name)}"
     )}
  end

  @doc """
  Builds a touch operation.
  """
  @spec touch() :: t()
  def touch do
    %__MODULE__{op_type: @op_touch}
  end

  @doc """
  Builds an operate delete operation.
  """
  @spec delete() :: t()
  def delete do
    %__MODULE__{op_type: @op_delete}
  end

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
