defmodule Aerospike.Protocol.AsmMsg.Operation do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Exp
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.MessagePack

  @op_read 1
  @op_write 2
  @op_cdt_read 3
  @op_cdt_modify 4
  @op_add 5
  @op_exp_read 7
  @op_exp_modify 8
  @op_append 9
  @op_prepend 10
  @op_touch 11
  @op_bit_read 12
  @op_bit_modify 13
  @op_delete 14
  @op_hll_read 15
  @op_hll_modify 16
  @particle_null 0
  @particle_blob 4

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

  @doc "Returns the CDT READ operation type."
  @spec op_cdt_read() :: 3
  def op_cdt_read, do: @op_cdt_read

  @doc "Returns the CDT MODIFY operation type."
  @spec op_cdt_modify() :: 4
  def op_cdt_modify, do: @op_cdt_modify

  @doc "Returns the WRITE operation type."
  @spec op_write() :: 2
  def op_write, do: @op_write

  @doc "Returns the ADD operation type."
  @spec op_add() :: 5
  def op_add, do: @op_add

  @doc "Returns the EXP_READ operation type."
  @spec op_exp_read() :: 7
  def op_exp_read, do: @op_exp_read

  @doc "Returns the EXP_MODIFY operation type."
  @spec op_exp_modify() :: 8
  def op_exp_modify, do: @op_exp_modify

  @doc "Returns the APPEND operation type."
  @spec op_append() :: 9
  def op_append, do: @op_append

  @doc "Returns the PREPEND operation type."
  @spec op_prepend() :: 10
  def op_prepend, do: @op_prepend

  @doc "Returns the TOUCH operation type."
  @spec op_touch() :: 11
  def op_touch, do: @op_touch

  @doc "Returns the BIT_READ operation type."
  @spec op_bit_read() :: 12
  def op_bit_read, do: @op_bit_read

  @doc "Returns the BIT_MODIFY operation type."
  @spec op_bit_modify() :: 13
  def op_bit_modify, do: @op_bit_modify

  @doc "Returns the DELETE operation type."
  @spec op_delete() :: 14
  def op_delete, do: @op_delete

  @doc "Returns the HLL_READ operation type."
  @spec op_hll_read() :: 15
  def op_hll_read, do: @op_hll_read

  @doc "Returns the HLL_MODIFY operation type."
  @spec op_hll_modify() :: 16
  def op_hll_modify, do: @op_hll_modify

  @doc "Returns the null particle type."
  @spec particle_null() :: 0
  def particle_null, do: @particle_null

  @doc "Returns the blob particle type."
  @spec particle_blob() :: 4
  def particle_blob, do: @particle_blob

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

  def from_simple({:add, bin_name, value}) do
    add(bin_name, value)
  end

  def from_simple({:append, bin_name, value}) do
    append(bin_name, value)
  end

  def from_simple({:prepend, bin_name, value}) do
    prepend(bin_name, value)
  end

  def from_simple(:touch), do: {:ok, touch()}
  def from_simple(:delete), do: {:ok, delete()}

  def from_simple(other) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message:
         "unsupported simple operation #{inspect(other)}; supported shapes: {:read, bin}, {:write, bin, value}, {:add, bin, delta}, {:append, bin, suffix}, {:prepend, bin, prefix}, :touch, :delete"
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
  Builds an add operation.
  """
  @spec add(String.t(), integer() | float()) :: {:ok, t()} | {:error, Error.t()}
  def add(bin_name, value) when is_binary(bin_name) and byte_size(bin_name) > 0 do
    build_numeric_mutation(bin_name, value, @op_add)
  end

  def add(bin_name, _value) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "add bin name must be a non-empty binary, got: #{inspect(bin_name)}"
     )}
  end

  @doc """
  Builds an append operation.
  """
  @spec append(String.t(), String.t()) :: {:ok, t()} | {:error, Error.t()}
  def append(bin_name, value) when is_binary(bin_name) and byte_size(bin_name) > 0 do
    build_binary_mutation(bin_name, value, @op_append, "append")
  end

  def append(bin_name, _value) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "append bin name must be a non-empty binary, got: #{inspect(bin_name)}"
     )}
  end

  @doc """
  Builds a prepend operation.
  """
  @spec prepend(String.t(), String.t()) :: {:ok, t()} | {:error, Error.t()}
  def prepend(bin_name, value) when is_binary(bin_name) and byte_size(bin_name) > 0 do
    build_binary_mutation(bin_name, value, @op_prepend, "prepend")
  end

  def prepend(bin_name, _value) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "prepend bin name must be a non-empty binary, got: #{inspect(bin_name)}"
     )}
  end

  @doc """
  Builds an expression read operation.
  """
  @spec exp_read(String.t(), Exp.t(), non_neg_integer()) :: {:ok, t()} | {:error, Error.t()}
  def exp_read(bin_name, %Exp{} = expression, flags \\ 0) do
    build_exp_operation(bin_name, expression, flags, @op_exp_read, "expression read")
  end

  @doc """
  Builds an expression write operation.
  """
  @spec exp_modify(String.t(), Exp.t(), non_neg_integer()) :: {:ok, t()} | {:error, Error.t()}
  def exp_modify(bin_name, %Exp{} = expression, flags \\ 0) do
    build_exp_operation(bin_name, expression, flags, @op_exp_modify, "expression write")
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

  defp build_numeric_mutation(bin_name, value, op_type) do
    case Value.encode_value(value) do
      {:ok, {particle_type, data}} when particle_type in [1, 2] ->
        {:ok,
         %__MODULE__{
           op_type: op_type,
           particle_type: particle_type,
           bin_name: bin_name,
           data: data
         }}

      {:ok, _} ->
        {:error,
         Error.from_result_code(:invalid_argument,
           message: "add requires an integer or float value, got: #{inspect(value)}"
         )}

      {:error, %Error{}} = err ->
        err
    end
  end

  defp build_binary_mutation(bin_name, value, op_type, name) do
    case Value.encode_value(value) do
      {:ok, {particle_type, data}} when particle_type == 3 ->
        {:ok,
         %__MODULE__{
           op_type: op_type,
           particle_type: particle_type,
           bin_name: bin_name,
           data: data
         }}

      {:ok, _} ->
        {:error,
         Error.from_result_code(:invalid_argument,
           message: "#{name} requires a binary value, got: #{inspect(value)}"
         )}

      {:error, %Error{}} = err ->
        err
    end
  end

  defp build_exp_operation(bin_name, %Exp{wire: wire}, flags, op_type, _name)
       when is_binary(bin_name) and byte_size(bin_name) > 0 and is_binary(wire) and
              byte_size(wire) > 0 and is_integer(flags) and flags >= 0 do
    {:ok,
     %__MODULE__{
       op_type: op_type,
       particle_type: @particle_blob,
       bin_name: bin_name,
       data: encode_exp_op(wire, flags)
     }}
  end

  defp build_exp_operation(_bin_name, %Exp{wire: ""}, _flags, _op_type, name) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "#{name} expression wire must be non-empty"
     )}
  end

  defp build_exp_operation(bin_name, %Exp{}, _flags, _op_type, name)
       when not (is_binary(bin_name) and byte_size(bin_name) > 0) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "#{name} bin name must be a non-empty binary, got: #{inspect(bin_name)}"
     )}
  end

  defp build_exp_operation(_bin_name, %Exp{}, flags, _op_type, name) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "#{name} flags must be a non-negative integer, got: #{inspect(flags)}"
     )}
  end

  defp encode_exp_op(wire, flags), do: <<0x92>> <> wire <> MessagePack.pack!(flags)

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
