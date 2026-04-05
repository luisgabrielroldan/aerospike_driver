defmodule Aerospike.Protocol.AsmMsg do
  @moduledoc false

  import Bitwise

  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation

  @msg_remaining_header_size 22

  # Info1 flags
  @info1_read 0x01
  @info1_get_all 0x02
  @info1_short_query 0x04
  @info1_batch 0x08
  @info1_nobindata 0x20
  @info1_read_mode_ap_all 0x40
  @info1_compress_response 0x80

  # Info2 flags
  @info2_write 0x01
  @info2_delete 0x02
  @info2_generation 0x04
  @info2_generation_gt 0x08
  @info2_durable_delete 0x10
  @info2_create_only 0x20
  @info2_relax_ap_long_query 0x40
  @info2_respond_all_ops 0x80

  # Info3 flags
  @info3_last 0x01
  @info3_commit_master 0x02
  @info3_partition_done 0x04
  @info3_update_only 0x08
  @info3_create_or_replace 0x10
  @info3_replace_only 0x20
  @info3_sc_read_type 0x40
  @info3_sc_read_relax 0x80

  # Info4/MRT flags
  @info4_mrt_verify_read 0x01
  @info4_mrt_roll_forward 0x02
  @info4_mrt_roll_back 0x04
  @info4_mrt_on_locking_only 0x10

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

  # Flag accessor functions
  @doc "Returns the INFO1_READ flag value."
  @spec info1_read() :: 0x01
  def info1_read, do: @info1_read

  @doc "Returns the INFO1_GET_ALL flag value."
  @spec info1_get_all() :: 0x02
  def info1_get_all, do: @info1_get_all

  @doc "Returns the INFO1_SHORT_QUERY flag value."
  @spec info1_short_query() :: 0x04
  def info1_short_query, do: @info1_short_query

  @doc "Returns the INFO1_BATCH flag value."
  @spec info1_batch() :: 0x08
  def info1_batch, do: @info1_batch

  @doc "Returns the INFO1_NOBINDATA flag value."
  @spec info1_nobindata() :: 0x20
  def info1_nobindata, do: @info1_nobindata

  @doc "Returns the INFO1_READ_MODE_AP_ALL flag value."
  @spec info1_read_mode_ap_all() :: 0x40
  def info1_read_mode_ap_all, do: @info1_read_mode_ap_all

  @doc "Returns the INFO1_COMPRESS_RESPONSE flag value."
  @spec info1_compress_response() :: 0x80
  def info1_compress_response, do: @info1_compress_response

  @doc "Returns the INFO2_WRITE flag value."
  @spec info2_write() :: 0x01
  def info2_write, do: @info2_write

  @doc "Returns the INFO2_DELETE flag value."
  @spec info2_delete() :: 0x02
  def info2_delete, do: @info2_delete

  @doc "Returns the INFO2_GENERATION flag value."
  @spec info2_generation() :: 0x04
  def info2_generation, do: @info2_generation

  @doc "Returns the INFO2_GENERATION_GT flag value."
  @spec info2_generation_gt() :: 0x08
  def info2_generation_gt, do: @info2_generation_gt

  @doc "Returns the INFO2_DURABLE_DELETE flag value."
  @spec info2_durable_delete() :: 0x10
  def info2_durable_delete, do: @info2_durable_delete

  @doc "Returns the INFO2_CREATE_ONLY flag value."
  @spec info2_create_only() :: 0x20
  def info2_create_only, do: @info2_create_only

  @doc "Returns the INFO2_RELAX_AP_LONG_QUERY flag value."
  @spec info2_relax_ap_long_query() :: 0x40
  def info2_relax_ap_long_query, do: @info2_relax_ap_long_query

  @doc "Returns the INFO2_RESPOND_ALL_OPS flag value."
  @spec info2_respond_all_ops() :: 0x80
  def info2_respond_all_ops, do: @info2_respond_all_ops

  @doc "Returns the INFO3_LAST flag value."
  @spec info3_last() :: 0x01
  def info3_last, do: @info3_last

  @doc "Returns the INFO3_COMMIT_MASTER flag value."
  @spec info3_commit_master() :: 0x02
  def info3_commit_master, do: @info3_commit_master

  @doc "Returns the INFO3_PARTITION_DONE flag value."
  @spec info3_partition_done() :: 0x04
  def info3_partition_done, do: @info3_partition_done

  @doc "Returns the INFO3_UPDATE_ONLY flag value."
  @spec info3_update_only() :: 0x08
  def info3_update_only, do: @info3_update_only

  @doc "Returns the INFO3_CREATE_OR_REPLACE flag value."
  @spec info3_create_or_replace() :: 0x10
  def info3_create_or_replace, do: @info3_create_or_replace

  @doc "Returns the INFO3_REPLACE_ONLY flag value."
  @spec info3_replace_only() :: 0x20
  def info3_replace_only, do: @info3_replace_only

  @doc "Returns the INFO3_SC_READ_TYPE flag value."
  @spec info3_sc_read_type() :: 0x40
  def info3_sc_read_type, do: @info3_sc_read_type

  @doc "Returns the INFO3_SC_READ_RELAX flag value."
  @spec info3_sc_read_relax() :: 0x80
  def info3_sc_read_relax, do: @info3_sc_read_relax

  @doc "Returns the INFO4_MRT_VERIFY_READ flag value."
  @spec info4_mrt_verify_read() :: 0x01
  def info4_mrt_verify_read, do: @info4_mrt_verify_read

  @doc "Returns the INFO4_MRT_ROLL_FORWARD flag value."
  @spec info4_mrt_roll_forward() :: 0x02
  def info4_mrt_roll_forward, do: @info4_mrt_roll_forward

  @doc "Returns the INFO4_MRT_ROLL_BACK flag value."
  @spec info4_mrt_roll_back() :: 0x04
  def info4_mrt_roll_back, do: @info4_mrt_roll_back

  @doc "Returns the INFO4_MRT_ON_LOCKING_ONLY flag value."
  @spec info4_mrt_on_locking_only() :: 0x10
  def info4_mrt_on_locking_only, do: @info4_mrt_on_locking_only

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
      iex> byte_size(encoded)
      22

  """
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{} = msg) do
    field_count = length(msg.fields)
    op_count = length(msg.operations)

    header =
      <<@msg_remaining_header_size::8, msg.info1::8, msg.info2::8, msg.info3::8, msg.info4::8,
        msg.result_code::8, msg.generation::32-big, msg.expiration::32-big,
        msg.timeout::32-big-signed, field_count::16-big, op_count::16-big>>

    fields_binary = Enum.map_join(msg.fields, &Field.encode/1)
    ops_binary = Enum.map_join(msg.operations, &Operation.encode/1)

    header <> fields_binary <> ops_binary
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

  @doc """
  Creates a simple write command (PUT).
  """
  @spec write_command(binary(), binary(), binary(), [Operation.t()]) :: t()
  def write_command(namespace, set, digest, operations) do
    %__MODULE__{
      info2: @info2_write,
      fields: [
        Field.namespace(namespace),
        Field.set(set),
        Field.digest(digest)
      ],
      operations: operations
    }
  end

  @doc """
  Creates a delete command.
  """
  @spec delete_command(binary(), binary(), binary()) :: t()
  def delete_command(namespace, set, digest) do
    %__MODULE__{
      info2: @info2_write ||| @info2_delete,
      fields: [
        Field.namespace(namespace),
        Field.set(set),
        Field.digest(digest)
      ]
    }
  end

  @doc """
  Creates an exists command (read metadata only, no bin data).

  Uses `INFO1_READ | INFO1_NOBINDATA` to check record existence without fetching bins.
  """
  @spec exists_command(binary(), binary(), binary()) :: t()
  def exists_command(namespace, set, digest) do
    %__MODULE__{
      info1: @info1_read ||| @info1_nobindata,
      fields: [
        Field.namespace(namespace),
        Field.set(set),
        Field.digest(digest)
      ]
    }
  end

  @doc """
  Creates a touch command (refresh TTL without changing bins).

  Uses `INFO2_WRITE` with a single touch operation to refresh the record's TTL.
  """
  @spec touch_command(binary(), binary(), binary()) :: t()
  def touch_command(namespace, set, digest) do
    %__MODULE__{
      info2: @info2_write,
      fields: [
        Field.namespace(namespace),
        Field.set(set),
        Field.digest(digest)
      ],
      operations: [Operation.touch()]
    }
  end
end
