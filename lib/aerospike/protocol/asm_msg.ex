defmodule Aerospike.Protocol.AsmMsg do
  @moduledoc false

  import Bitwise

  alias Aerospike.Exp
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation

  @msg_remaining_header_size 22

  # Info1 flags
  @info1_read 0x01
  @info1_get_all 0x02
  @info1_batch 0x08
  @info1_nobindata 0x20

  # Info2 flags
  @info2_write 0x01
  @info2_delete 0x02
  @info2_generation 0x04
  @info2_durable_delete 0x10
  @info2_create_only 0x20
  @info2_respond_all_ops 0x80

  # Info3 flags
  @info3_last 0x01
  @info3_update_only 0x08
  @info3_create_or_replace 0x10
  @info3_replace_only 0x20
  @info3_sc_read_type 0x40

  # Info4 flags
  @info4_mrt_verify_read 0x01
  @info4_mrt_roll_forward 0x02
  @info4_mrt_roll_back 0x04

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

  @doc "Returns the INFO1_BATCH flag value."
  @spec info1_batch() :: 0x08
  def info1_batch, do: @info1_batch

  @doc "Returns the INFO1_NOBINDATA flag value."
  @spec info1_nobindata() :: 0x20
  def info1_nobindata, do: @info1_nobindata

  @doc "Returns the INFO2_WRITE flag value."
  @spec info2_write() :: 0x01
  def info2_write, do: @info2_write

  @doc "Returns the INFO2_DELETE flag value."
  @spec info2_delete() :: 0x02
  def info2_delete, do: @info2_delete

  @doc "Returns the INFO2_GENERATION flag value."
  @spec info2_generation() :: 0x04
  def info2_generation, do: @info2_generation

  @doc "Returns the INFO2_DURABLE_DELETE flag value."
  @spec info2_durable_delete() :: 0x10
  def info2_durable_delete, do: @info2_durable_delete

  @doc "Returns the INFO2_CREATE_ONLY flag value."
  @spec info2_create_only() :: 0x20
  def info2_create_only, do: @info2_create_only

  @doc "Returns the INFO2_RESPOND_ALL_OPS flag value."
  @spec info2_respond_all_ops() :: 0x80
  def info2_respond_all_ops, do: @info2_respond_all_ops

  @doc "Returns the INFO3_LAST flag value."
  @spec info3_last() :: 0x01
  def info3_last, do: @info3_last

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

  @doc "Returns the INFO4_MRT_VERIFY_READ flag value."
  @spec info4_mrt_verify_read() :: 0x01
  def info4_mrt_verify_read, do: @info4_mrt_verify_read

  @doc "Returns the INFO4_MRT_ROLL_FORWARD flag value."
  @spec info4_mrt_roll_forward() :: 0x02
  def info4_mrt_roll_forward, do: @info4_mrt_roll_forward

  @doc "Returns the INFO4_MRT_ROLL_BACK flag value."
  @spec info4_mrt_roll_back() :: 0x04
  def info4_mrt_roll_back, do: @info4_mrt_roll_back

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

    [header | encode_fields(msg.fields, encode_operations(msg.operations))]
  end

  defp encode_fields([], tail), do: tail
  defp encode_fields([field | rest], tail), do: [Field.encode(field) | encode_fields(rest, tail)]

  defp encode_operations([]), do: []

  defp encode_operations([operation | rest]) do
    [Operation.encode(operation) | encode_operations(rest)]
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
    case Field.decode(binary) do
      {:ok, field, rest} ->
        with {:ok, fields, rest} <- decode_fields(rest, count - 1) do
          {:ok, [field | fields], rest}
        end

      {:error, _} = error ->
        error
    end
  end

  defp decode_operations(binary, 0), do: {:ok, [], binary}

  defp decode_operations(binary, count) do
    case Operation.decode(binary) do
      {:ok, op, rest} ->
        with {:ok, operations, rest} <- decode_operations(rest, count - 1) do
          {:ok, [op | operations], rest}
        end

      {:error, _} = error ->
        error
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
  Builds a homogeneous batch read-style request around a pre-encoded batch field.
  """
  @spec batch_read_command(binary()) :: t()
  def batch_read_command(batch_field_data) when is_binary(batch_field_data) do
    %__MODULE__{
      info1: @info1_read ||| @info1_batch,
      fields: [
        %Field{type: Field.type_batch_index_with_set(), data: batch_field_data}
      ]
    }
  end

  @doc """
  Builds a key-scoped request for unary writes, header reads, and simple operate.

  The caller chooses the request attributes explicitly through `opts` so
  command modules do not need to hardcode wire flags inline.
  """
  @spec key_command(Key.t(), [Operation.t()], keyword()) :: t()
  def key_command(%Key{} = key, operations, opts \\ []) when is_list(operations) do
    %__MODULE__{
      info1: info1_from_opts(opts),
      info2: info2_from_opts(opts),
      info3: info3_from_opts(opts),
      generation: generation_from_opts(opts),
      expiration: Keyword.get(opts, :ttl, 0),
      timeout: Keyword.get(opts, :timeout, 0),
      fields: key_fields(key, opts),
      operations: operations
    }
  end

  @spec key_fields(Key.t(), keyword()) :: [Field.t()]
  def key_fields(%Key{} = key, opts \\ []) do
    send_key? = Keyword.get(opts, :send_key, false)

    [
      Field.namespace(key.namespace),
      Field.set(key.set),
      Field.digest(key.digest)
      | maybe_key_field(key, send_key?)
    ]
  end

  defp maybe_key_field(_key, false), do: []

  defp maybe_key_field(%Key{} = key, true) do
    case Field.key_from_user_key(%{user_key: key.user_key}) do
      nil -> []
      field -> [field]
    end
  end

  @doc false
  @spec maybe_add_filter_exp(t(), Exp.t() | nil) :: t()
  def maybe_add_filter_exp(%__MODULE__{} = msg, nil), do: msg

  def maybe_add_filter_exp(%__MODULE__{} = msg, %Exp{wire: wire}) when is_binary(wire) do
    %{msg | fields: msg.fields ++ [Field.filter_exp(wire)]}
  end

  defp info1_from_opts(opts) do
    0
    |> maybe_flag(Keyword.get(opts, :read, false), @info1_read)
    |> maybe_flag(Keyword.get(opts, :read_all, false), @info1_get_all)
    |> maybe_flag(Keyword.get(opts, :read_header, false), @info1_nobindata)
  end

  defp info2_from_opts(opts) do
    0
    |> maybe_flag(Keyword.get(opts, :write, false), @info2_write)
    |> maybe_flag(Keyword.get(opts, :delete, false), @info2_delete)
    |> maybe_flag(generation_flag?(Keyword.get(opts, :generation)), @info2_generation)
    |> maybe_flag(Keyword.get(opts, :durable_delete, false), @info2_durable_delete)
    |> maybe_flag(Keyword.get(opts, :exists) == :create_only, @info2_create_only)
    |> maybe_flag(Keyword.get(opts, :respond_all_ops, false), @info2_respond_all_ops)
  end

  defp info3_from_opts(opts) do
    0
    |> maybe_flag(Keyword.get(opts, :exists) == :update_only, @info3_update_only)
    |> maybe_flag(Keyword.get(opts, :exists) == :create_or_replace, @info3_create_or_replace)
    |> maybe_flag(Keyword.get(opts, :exists) == :replace_only, @info3_replace_only)
  end

  defp generation_flag?(generation) when is_integer(generation) and generation > 0, do: true
  defp generation_flag?(_generation), do: false

  defp generation_from_opts(opts) do
    case Keyword.get(opts, :generation) do
      generation when is_integer(generation) and generation >= 0 -> generation
      _ -> 0
    end
  end

  defp maybe_flag(flags, false, _flag), do: flags
  defp maybe_flag(flags, true, flag), do: flags ||| flag
end
