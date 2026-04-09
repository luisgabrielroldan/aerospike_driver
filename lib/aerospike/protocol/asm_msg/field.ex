defmodule Aerospike.Protocol.AsmMsg.Field do
  @moduledoc false

  # Field type constants from field_type.go
  @namespace 0
  @table 1
  @key 2
  @record_version 3
  @digest_ripe 4
  @mrt_id 5
  @mrt_deadline 6
  @query_id 7
  @socket_timeout 9
  @records_per_second 10
  @pid_array 11
  @digest_array 12
  @max_records 13
  @bval_array 15
  @index_name 21
  @index_range 22
  @index_context 23
  @index_expression 24
  @index_type 26
  @udf_package_name 30
  @udf_function 31
  @udf_arglist 32
  @udf_op 33
  @query_binlist 40
  @batch_index 41
  @batch_index_with_set 42
  @filter_exp 43

  defstruct type: 0, data: <<>>

  @type t :: %__MODULE__{
          type: non_neg_integer(),
          data: binary()
        }

  # Field type accessor functions
  @doc "Returns the NAMESPACE field type constant."
  @spec type_namespace() :: 0
  def type_namespace, do: @namespace

  @doc "Returns the TABLE (SET) field type constant."
  @spec type_table() :: 1
  def type_table, do: @table

  @doc "Returns the KEY field type constant."
  @spec type_key() :: 2
  def type_key, do: @key

  @doc "Returns the RECORD_VERSION field type constant."
  @spec type_record_version() :: 3
  def type_record_version, do: @record_version

  @doc "Returns the DIGEST_RIPE field type constant."
  @spec type_digest() :: 4
  def type_digest, do: @digest_ripe

  @doc "Returns the MRT_ID field type constant."
  @spec type_mrt_id() :: 5
  def type_mrt_id, do: @mrt_id

  @doc "Returns the MRT_DEADLINE field type constant."
  @spec type_mrt_deadline() :: 6
  def type_mrt_deadline, do: @mrt_deadline

  @doc "Returns the QUERY_ID field type constant."
  @spec type_query_id() :: 7
  def type_query_id, do: @query_id

  @doc "Returns the SOCKET_TIMEOUT field type constant."
  @spec type_socket_timeout() :: 9
  def type_socket_timeout, do: @socket_timeout

  @doc "Returns the RECORDS_PER_SECOND field type constant."
  @spec type_records_per_second() :: 10
  def type_records_per_second, do: @records_per_second

  @doc "Returns the PID_ARRAY field type constant."
  @spec type_pid_array() :: 11
  def type_pid_array, do: @pid_array

  @doc "Returns the DIGEST_ARRAY field type constant."
  @spec type_digest_array() :: 12
  def type_digest_array, do: @digest_array

  @doc "Returns the MAX_RECORDS field type constant."
  @spec type_max_records() :: 13
  def type_max_records, do: @max_records

  @doc "Returns the BVAL_ARRAY field type constant."
  @spec type_bval_array() :: 15
  def type_bval_array, do: @bval_array

  @doc "Returns the INDEX_NAME field type constant."
  @spec type_index_name() :: 21
  def type_index_name, do: @index_name

  @doc "Returns the INDEX_RANGE field type constant."
  @spec type_index_range() :: 22
  def type_index_range, do: @index_range

  @doc "Returns the INDEX_CONTEXT field type constant."
  @spec type_index_context() :: 23
  def type_index_context, do: @index_context

  @doc "Returns the INDEX_EXPRESSION field type constant."
  @spec type_index_expression() :: 24
  def type_index_expression, do: @index_expression

  @doc "Returns the INDEX_TYPE field type constant."
  @spec type_index_type() :: 26
  def type_index_type, do: @index_type

  @doc "Returns the UDF_PACKAGE_NAME field type constant."
  @spec type_udf_package_name() :: 30
  def type_udf_package_name, do: @udf_package_name

  @doc "Returns the UDF_FUNCTION field type constant."
  @spec type_udf_function() :: 31
  def type_udf_function, do: @udf_function

  @doc "Returns the UDF_ARGLIST field type constant."
  @spec type_udf_arglist() :: 32
  def type_udf_arglist, do: @udf_arglist

  @doc "Returns the UDF_OP field type constant."
  @spec type_udf_op() :: 33
  def type_udf_op, do: @udf_op

  @doc "Returns the QUERY_BINLIST field type constant."
  @spec type_query_binlist() :: 40
  def type_query_binlist, do: @query_binlist

  @doc "Returns the BATCH_INDEX field type constant."
  @spec type_batch_index() :: 41
  def type_batch_index, do: @batch_index

  @doc "Returns the BATCH_INDEX_WITH_SET field type constant."
  @spec type_batch_index_with_set() :: 42
  def type_batch_index_with_set, do: @batch_index_with_set

  @doc "Returns the FILTER_EXP field type constant."
  @spec type_filter_exp() :: 43
  def type_filter_exp, do: @filter_exp

  @doc """
  Encodes a field into binary format.

  Field header format:
  - 4 bytes: size (data_length + 1 for the type byte)
  - 1 byte: field type
  - N bytes: data

  ## Examples

      iex> field = %Aerospike.Protocol.AsmMsg.Field{type: 0, data: "test"}
      iex> Aerospike.Protocol.AsmMsg.Field.encode(field)
      <<0, 0, 0, 5, 0, "test">>

  """
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{type: type, data: data}) do
    size = byte_size(data) + 1
    <<size::32-big, type::8, data::binary>>
  end

  @doc """
  Decodes a field from binary format.

  Returns `{:ok, field, rest}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> binary = <<0, 0, 0, 5, 0, "test", "remaining">>
      iex> {:ok, field, rest} = Aerospike.Protocol.AsmMsg.Field.decode(binary)
      iex> field.type
      0
      iex> field.data
      "test"
      iex> rest
      "remaining"

  """
  @spec decode(binary()) :: {:ok, t(), binary()} | {:error, atom()}
  def decode(<<size::32-big, type::8, rest::binary>>) when size > 0 do
    data_size = size - 1

    if byte_size(rest) >= data_size do
      <<data::binary-size(data_size), remaining::binary>> = rest
      {:ok, %__MODULE__{type: type, data: data}, remaining}
    else
      {:error, :incomplete_field}
    end
  end

  def decode(<<_size::32-big, _rest::binary>>), do: {:error, :invalid_field_size}
  def decode(_), do: {:error, :incomplete_field_header}

  @doc """
  Creates a namespace field.
  """
  @spec namespace(binary()) :: t()
  def namespace(name) when is_binary(name) do
    %__MODULE__{type: @namespace, data: name}
  end

  @doc """
  Creates a set (table) field.
  """
  @spec set(binary()) :: t()
  def set(name) when is_binary(name) do
    %__MODULE__{type: @table, data: name}
  end

  @doc """
  Creates a key field with the given particle type and value.
  """
  @spec key(non_neg_integer(), binary()) :: t()
  def key(particle_type, value) when is_integer(particle_type) and is_binary(value) do
    %__MODULE__{type: @key, data: <<particle_type::8, value::binary>>}
  end

  @doc """
  Creates a digest field (RIPEMD-160 hash).
  """
  @spec digest(binary()) :: t()
  def digest(digest) when is_binary(digest) and byte_size(digest) == 20 do
    %__MODULE__{type: @digest_ripe, data: digest}
  end

  @doc """
  Creates a KEY field from a Key struct's user_key.

  - Integer user_key -> particle type 1, 8 bytes big-endian
  - String user_key -> particle type 3, raw UTF-8
  - nil user_key -> nil (no field, typically digest-only key)
  """
  @spec key_from_user_key(%{user_key: integer() | binary() | nil}) :: t() | nil
  def key_from_user_key(%{user_key: n}) when is_integer(n) do
    key(1, <<n::64-signed-big>>)
  end

  def key_from_user_key(%{user_key: s}) when is_binary(s) do
    key(3, s)
  end

  def key_from_user_key(%{user_key: nil}), do: nil

  @doc """
  Creates a UDF_PACKAGE_NAME field (the Lua module name).
  """
  @spec udf_package_name(binary()) :: t()
  def udf_package_name(name) when is_binary(name) do
    %__MODULE__{type: @udf_package_name, data: name}
  end

  @doc """
  Creates a UDF_FUNCTION field (the Lua function name).
  """
  @spec udf_function(name :: binary()) :: t()
  def udf_function(name) when is_binary(name) do
    %__MODULE__{type: @udf_function, data: name}
  end

  @doc """
  Creates a UDF_ARGLIST field from a MessagePack-encoded binary.
  """
  @spec udf_arglist(binary()) :: t()
  def udf_arglist(encoded) when is_binary(encoded) do
    %__MODULE__{type: @udf_arglist, data: encoded}
  end

  @doc """
  Creates a UDF_OP field. Use `1` for single-record execution.
  """
  @spec udf_op(non_neg_integer()) :: t()
  def udf_op(op) when is_integer(op) and op >= 0 do
    %__MODULE__{type: @udf_op, data: <<op::8>>}
  end

  @doc """
  Creates an MRT_ID field from a transaction ID.

  Encodes the transaction ID as an 8-byte little-endian signed integer.
  """
  @spec mrt_id(integer()) :: t()
  def mrt_id(txn_id) when is_integer(txn_id) do
    %__MODULE__{type: @mrt_id, data: <<txn_id::64-signed-little>>}
  end

  @doc """
  Creates an MRT_DEADLINE field from a server-returned deadline value.

  Encodes the deadline as a 4-byte little-endian signed integer.
  """
  @spec mrt_deadline(integer()) :: t()
  def mrt_deadline(deadline) when is_integer(deadline) do
    %__MODULE__{type: @mrt_deadline, data: <<deadline::32-signed-little>>}
  end

  @doc """
  Creates a RECORD_VERSION field from a version integer.

  Encodes the version as 7 bytes in little-endian order. The upper byte
  of the uint64 is intentionally dropped — the wire format uses 7 bytes.
  """
  @spec record_version(non_neg_integer()) :: t()
  def record_version(version) when is_integer(version) and version >= 0 do
    %__MODULE__{type: @record_version, data: <<version::56-little-unsigned>>}
  end
end
