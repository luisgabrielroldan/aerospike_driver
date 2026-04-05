defmodule Aerospike.Protocol.AsmMsg.Operation do
  @moduledoc false

  # Operation type constants from operation.go
  @read 1
  @write 2
  @cdt_read 3
  @cdt_modify 4
  @add 5
  @exp_read 7
  @exp_modify 8
  @append 9
  @prepend 10
  @touch 11
  @bit_read 12
  @bit_modify 13
  @delete 14
  @hll_read 15
  @hll_modify 16

  # Particle type constants from particle_type.go
  @particle_null 0
  @particle_integer 1
  @particle_float 2
  @particle_string 3
  @particle_blob 4
  @particle_digest 6
  @particle_bool 17
  @particle_hll 18
  @particle_map 19
  @particle_list 20
  @particle_geojson 23

  # `read_header`: when true, this is a header-only read (generation/TTL, no bins).
  # Wire `op_type` matches `READ` (1); flagging is used only for operate header flags.
  #
  # `map_cdt`: true for map collection op types (Go `_MAP_READ` / `_MAP_MODIFY`); same wire
  # op bytes as list CDT but operate-args treats them like map for RESPOND_ALL_OPS.
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

  # Operation type accessor functions
  @doc "Returns the READ operation type constant."
  @spec op_read() :: 1
  def op_read, do: @read

  @doc "Returns the WRITE operation type constant."
  @spec op_write() :: 2
  def op_write, do: @write

  @doc "Returns the CDT_READ operation type constant."
  @spec op_cdt_read() :: 3
  def op_cdt_read, do: @cdt_read

  @doc "Returns the CDT_MODIFY operation type constant."
  @spec op_cdt_modify() :: 4
  def op_cdt_modify, do: @cdt_modify

  @doc "Returns the ADD operation type constant."
  @spec op_add() :: 5
  def op_add, do: @add

  @doc "Returns the EXP_READ operation type constant."
  @spec op_exp_read() :: 7
  def op_exp_read, do: @exp_read

  @doc "Returns the EXP_MODIFY operation type constant."
  @spec op_exp_modify() :: 8
  def op_exp_modify, do: @exp_modify

  @doc "Returns the APPEND operation type constant."
  @spec op_append() :: 9
  def op_append, do: @append

  @doc "Returns the PREPEND operation type constant."
  @spec op_prepend() :: 10
  def op_prepend, do: @prepend

  @doc "Returns the TOUCH operation type constant."
  @spec op_touch() :: 11
  def op_touch, do: @touch

  @doc "Returns the BIT_READ operation type constant."
  @spec op_bit_read() :: 12
  def op_bit_read, do: @bit_read

  @doc "Returns the BIT_MODIFY operation type constant."
  @spec op_bit_modify() :: 13
  def op_bit_modify, do: @bit_modify

  @doc "Returns the DELETE operation type constant."
  @spec op_delete() :: 14
  def op_delete, do: @delete

  @doc "Returns the HLL_READ operation type constant."
  @spec op_hll_read() :: 15
  def op_hll_read, do: @hll_read

  @doc "Returns the HLL_MODIFY operation type constant."
  @spec op_hll_modify() :: 16
  def op_hll_modify, do: @hll_modify

  # Particle type accessor functions
  @doc "Returns the NULL particle type constant."
  @spec particle_null() :: 0
  def particle_null, do: @particle_null

  @doc "Returns the INTEGER particle type constant."
  @spec particle_integer() :: 1
  def particle_integer, do: @particle_integer

  @doc "Returns the FLOAT particle type constant."
  @spec particle_float() :: 2
  def particle_float, do: @particle_float

  @doc "Returns the STRING particle type constant."
  @spec particle_string() :: 3
  def particle_string, do: @particle_string

  @doc "Returns the BLOB particle type constant."
  @spec particle_blob() :: 4
  def particle_blob, do: @particle_blob

  @doc "Returns the DIGEST particle type constant."
  @spec particle_digest() :: 6
  def particle_digest, do: @particle_digest

  @doc "Returns the BOOL particle type constant."
  @spec particle_bool() :: 17
  def particle_bool, do: @particle_bool

  @doc "Returns the HLL particle type constant."
  @spec particle_hll() :: 18
  def particle_hll, do: @particle_hll

  @doc "Returns the MAP particle type constant."
  @spec particle_map() :: 19
  def particle_map, do: @particle_map

  @doc "Returns the LIST particle type constant."
  @spec particle_list() :: 20
  def particle_list, do: @particle_list

  @doc "Returns the GEOJSON particle type constant."
  @spec particle_geojson() :: 23
  def particle_geojson, do: @particle_geojson

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

  @doc """
  Creates a read operation for a specific bin.
  """
  @spec read(String.t()) :: t()
  def read(bin_name) when is_binary(bin_name) do
    %__MODULE__{op_type: @read, particle_type: @particle_null, bin_name: bin_name, data: <<>>}
  end

  @doc """
  Creates a write operation with an integer value.
  """
  @spec write_integer(String.t(), integer()) :: t()
  def write_integer(bin_name, value) when is_binary(bin_name) and is_integer(value) do
    %__MODULE__{
      op_type: @write,
      particle_type: @particle_integer,
      bin_name: bin_name,
      data: <<value::64-big-signed>>
    }
  end

  @doc """
  Creates a write operation with a string value.
  """
  @spec write_string(String.t(), String.t()) :: t()
  def write_string(bin_name, value) when is_binary(bin_name) and is_binary(value) do
    %__MODULE__{op_type: @write, particle_type: @particle_string, bin_name: bin_name, data: value}
  end

  @doc """
  Creates a write operation with a blob (binary) value.
  """
  @spec write_blob(String.t(), binary()) :: t()
  def write_blob(bin_name, value) when is_binary(bin_name) and is_binary(value) do
    %__MODULE__{op_type: @write, particle_type: @particle_blob, bin_name: bin_name, data: value}
  end

  @doc """
  Creates a write operation with a float value.
  """
  @spec write_float(String.t(), float()) :: t()
  def write_float(bin_name, value) when is_binary(bin_name) and is_float(value) do
    %__MODULE__{
      op_type: @write,
      particle_type: @particle_float,
      bin_name: bin_name,
      data: <<value::64-float-big>>
    }
  end

  @doc """
  Creates an add (increment) operation.
  """
  @spec add(String.t(), integer()) :: t()
  def add(bin_name, value) when is_binary(bin_name) and is_integer(value) do
    %__MODULE__{
      op_type: @add,
      particle_type: @particle_integer,
      bin_name: bin_name,
      data: <<value::64-big-signed>>
    }
  end

  @doc """
  Creates an append operation (string concat to an existing string bin).
  """
  @spec append(String.t(), String.t()) :: t()
  def append(bin_name, value) when is_binary(bin_name) and is_binary(value) do
    %__MODULE__{
      op_type: @append,
      particle_type: @particle_string,
      bin_name: bin_name,
      data: value
    }
  end

  @doc """
  Creates a prepend operation (prefix to an existing string bin).
  """
  @spec prepend(String.t(), String.t()) :: t()
  def prepend(bin_name, value) when is_binary(bin_name) and is_binary(value) do
    %__MODULE__{
      op_type: @prepend,
      particle_type: @particle_string,
      bin_name: bin_name,
      data: value
    }
  end

  @doc """
  Creates a touch operation (updates record TTL without modifying data).
  """
  @spec touch() :: t()
  def touch do
    %__MODULE__{op_type: @touch, particle_type: @particle_null, bin_name: "", data: <<>>}
  end
end
