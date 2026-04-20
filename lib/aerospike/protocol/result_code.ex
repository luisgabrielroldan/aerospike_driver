defmodule Aerospike.Protocol.ResultCode do
  @moduledoc false

  @codes %{
    # Client-side error codes (negative)
    -24 => :txn_already_aborted,
    -23 => :txn_already_committed,
    -22 => :txn_failed,
    -21 => :grpc_error,
    -20 => :batch_failed,
    -19 => :no_response,
    -18 => :network_error,
    -17 => :common_error,
    -16 => :max_retries_exceeded,
    -15 => :max_error_rate,
    -14 => :max_records_required,
    -13 => :rack_not_defined,
    -12 => :invalid_cluster_partition_map,
    -11 => :server_not_available,
    -10 => :cluster_name_mismatch,
    -9 => :recordset_closed,
    -8 => :no_available_connections_to_node,
    -7 => :type_not_supported,
    -6 => :command_rejected,
    -5 => :query_terminated,
    -4 => :scan_terminated,
    -27 => :cluster_not_ready,
    -26 => :pool_timeout,
    -3 => :invalid_node,
    -2 => :parse_error,
    -1 => :serialize_error,

    # Server success
    0 => :ok,

    # Server error codes (positive)
    1 => :server_error,
    2 => :key_not_found,
    3 => :generation_error,
    4 => :parameter_error,
    5 => :key_exists,
    6 => :bin_exists,
    7 => :cluster_key_mismatch,
    8 => :server_mem_error,
    9 => :timeout,
    10 => :always_forbidden,
    11 => :partition_unavailable,
    12 => :bin_type_error,
    13 => :record_too_big,
    14 => :key_busy,
    15 => :scan_abort,
    16 => :unsupported_feature,
    17 => :bin_not_found,
    18 => :device_overload,
    19 => :key_mismatch,
    20 => :invalid_namespace,
    21 => :bin_name_too_long,
    22 => :fail_forbidden,
    23 => :element_not_found,
    24 => :element_exists,
    25 => :enterprise_only,
    26 => :op_not_applicable,
    27 => :filtered_out,
    28 => :lost_conflict,
    32 => :xdr_key_busy,

    # Query codes
    50 => :query_end,

    # Security codes
    51 => :security_not_supported,
    52 => :security_not_enabled,
    53 => :security_scheme_not_supported,
    54 => :invalid_command,
    55 => :invalid_field,
    56 => :illegal_state,
    60 => :invalid_user,
    61 => :user_already_exists,
    62 => :invalid_password,
    63 => :expired_password,
    64 => :forbidden_password,
    65 => :invalid_credential,
    66 => :expired_session,
    70 => :invalid_role,
    71 => :role_already_exists,
    72 => :invalid_privilege,
    73 => :invalid_whitelist,
    74 => :quotas_not_enabled,
    75 => :invalid_quota,
    80 => :not_authenticated,
    81 => :role_violation,
    82 => :not_whitelisted,
    83 => :quota_exceeded,

    # UDF codes
    100 => :udf_bad_response,

    # MRT (Multi-Record Transaction) codes
    120 => :mrt_blocked,
    121 => :mrt_version_mismatch,
    122 => :mrt_expired,
    123 => :mrt_too_many_writes,
    124 => :mrt_committed,
    125 => :mrt_aborted,
    126 => :mrt_already_locked,
    127 => :mrt_monitor_exists,

    # Batch codes
    150 => :batch_disabled,
    151 => :batch_max_requests_exceeded,
    152 => :batch_queues_full,

    # Geo codes
    160 => :geo_invalid_geojson,

    # Index codes
    200 => :index_found,
    201 => :index_not_found,
    202 => :index_oom,
    203 => :index_not_readable,
    204 => :index_generic,
    205 => :index_name_maxlen,
    206 => :index_maxcount,

    # Query error codes
    210 => :query_aborted,
    211 => :query_queue_full,
    212 => :query_timeout,
    213 => :query_generic,
    214 => :query_netio_error,
    215 => :query_duplicate,

    # UDF file codes
    1301 => :udf_not_found,
    1302 => :lua_file_not_found
  }

  @messages %{
    txn_already_aborted:
      "Multi-record transaction commit called, but the transaction was already aborted",
    txn_already_committed:
      "Multi-record transaction abort called, but the transaction was already committed",
    txn_failed: "Multi-record transaction failed",
    grpc_error: "GRPC error",
    batch_failed: "One or more keys failed in a batch",
    no_response: "No response was received from the server",
    network_error: "Network error",
    common_error: "Common error",
    max_retries_exceeded: "Max retries exceeded",
    max_error_rate: "Max errors limit reached for node",
    max_records_required: "max_records must be set to a positive integer for this operation",
    rack_not_defined: "Requested rack for node/namespace was not defined in the cluster",
    invalid_cluster_partition_map:
      "Cluster has an invalid partition map, usually due to bad configuration",
    server_not_available: "Server is not accepting requests",
    cluster_name_mismatch: "Cluster name does not match the configured cluster name",
    recordset_closed: "Recordset has already been closed or cancelled",
    no_available_connections_to_node: "No available connections to the node",
    type_not_supported: "Type cannot be converted to value type",
    command_rejected: "Command rejected",
    query_terminated: "Query terminated",
    scan_terminated: "Scan terminated",
    cluster_not_ready: "Cluster client is not ready (initial tend not complete)",
    pool_timeout: "Connection pool checkout timed out",
    invalid_node: "Invalid node",
    parse_error: "Parse error",
    serialize_error: "Serialize error",
    ok: "Operation succeeded",
    server_error: "Server error",
    key_not_found: "Key not found",
    generation_error: "Generation error",
    parameter_error: "Parameter error",
    key_exists: "Key already exists",
    bin_exists: "Bin already exists",
    cluster_key_mismatch: "Cluster key mismatch",
    server_mem_error: "Server memory error",
    timeout: "Timeout",
    always_forbidden: "Operation not allowed in current configuration",
    partition_unavailable: "Partition not available",
    bin_type_error: "Bin type error",
    record_too_big: "Record too big",
    key_busy: "Hot key",
    scan_abort: "Scan aborted",
    unsupported_feature: "Unsupported server feature",
    bin_not_found: "Bin not found",
    device_overload: "Device overload",
    key_mismatch: "Key mismatch",
    invalid_namespace: "Namespace not found",
    bin_name_too_long:
      "Bin name length greater than 15 characters, or maximum number of unique bin names exceeded",
    fail_forbidden: "Operation not allowed at this time",
    element_not_found: "Element not found",
    element_exists: "Element exists",
    enterprise_only: "Enterprise only feature",
    op_not_applicable: "Operation not applicable",
    filtered_out: "Transaction filtered out",
    lost_conflict: "Write command loses conflict to XDR",
    xdr_key_busy: "Write can't complete until XDR finishes shipping",
    query_end: "Query end",
    security_not_supported: "Security not supported",
    security_not_enabled: "Security not enabled",
    security_scheme_not_supported: "Security scheme not supported",
    invalid_command: "Invalid command",
    invalid_field: "Invalid field",
    illegal_state: "Illegal state",
    invalid_user: "Invalid user",
    user_already_exists: "User already exists",
    invalid_password: "Invalid password",
    expired_password: "Expired password",
    forbidden_password: "Forbidden password",
    invalid_credential: "Invalid credential",
    expired_session: "Login session expired",
    invalid_role: "Invalid role",
    role_already_exists: "Role already exists",
    invalid_privilege: "Invalid privilege",
    invalid_whitelist: "Invalid whitelist",
    quotas_not_enabled: "Quotas not enabled",
    invalid_quota: "Invalid quota",
    not_authenticated: "Not authenticated",
    role_violation: "Role violation",
    not_whitelisted: "Command not whitelisted",
    quota_exceeded: "Quota exceeded",
    udf_bad_response: "UDF returned error",
    mrt_blocked: "Transaction record blocked by a different transaction",
    mrt_version_mismatch: "Transaction read version mismatch identified during commit",
    mrt_expired: "Transaction deadline reached without a successful commit or abort",
    mrt_too_many_writes: "Transaction write command limit exceeded",
    mrt_committed: "Transaction was already committed",
    mrt_aborted: "Transaction was already aborted",
    mrt_already_locked: "This record has been locked by a previous update in this transaction",
    mrt_monitor_exists: "This transaction has already started",
    batch_disabled: "Batch functionality has been disabled",
    batch_max_requests_exceeded: "Batch max requests have been exceeded",
    batch_queues_full: "All batch queues are full",
    geo_invalid_geojson: "Invalid GeoJSON on insert/update",
    index_found: "Index already exists",
    index_not_found: "Index not found",
    index_oom: "Index out of memory",
    index_not_readable: "Index not readable",
    index_generic: "Index error",
    index_name_maxlen: "Index name max length exceeded",
    index_maxcount: "Index count exceeds max",
    query_aborted: "Query aborted",
    query_queue_full: "Query queue full",
    query_timeout: "Query timeout",
    query_generic: "Query error",
    query_netio_error: "Query NetIO error on server",
    query_duplicate: "Duplicate TaskId sent for the statement",
    udf_not_found: "UDF does not exist",
    lua_file_not_found: "LUA package/file does not exist"
  }

  @reverse_codes Map.new(@codes, fn {k, v} -> {v, k} end)

  @doc """
  Converts an integer result code to its atom representation.

  Returns `{:ok, atom}` for known codes, `{:error, code}` for unknown codes.

  ## Examples

      iex> Aerospike.Protocol.ResultCode.from_integer(0)
      {:ok, :ok}

      iex> Aerospike.Protocol.ResultCode.from_integer(2)
      {:ok, :key_not_found}

      iex> Aerospike.Protocol.ResultCode.from_integer(-18)
      {:ok, :network_error}

      iex> Aerospike.Protocol.ResultCode.from_integer(99999)
      {:error, 99999}

  """
  @spec from_integer(integer()) :: {:ok, atom()} | {:error, integer()}
  def from_integer(code) when is_integer(code) do
    case Map.fetch(@codes, code) do
      {:ok, atom} -> {:ok, atom}
      :error -> {:error, code}
    end
  end

  @doc """
  Converts an atom result code to its integer representation.

  Returns `{:ok, integer}` for known atoms, `{:error, atom}` for unknown atoms.

  ## Examples

      iex> Aerospike.Protocol.ResultCode.to_integer(:ok)
      {:ok, 0}

      iex> Aerospike.Protocol.ResultCode.to_integer(:key_not_found)
      {:ok, 2}

      iex> Aerospike.Protocol.ResultCode.to_integer(:unknown_code)
      {:error, :unknown_code}

  """
  @spec to_integer(atom()) :: {:ok, integer()} | {:error, atom()}
  def to_integer(atom) when is_atom(atom) do
    case Map.fetch(@reverse_codes, atom) do
      {:ok, code} -> {:ok, code}
      :error -> {:error, atom}
    end
  end

  @doc """
  Returns a human-readable message for a result code atom.

  ## Examples

      iex> Aerospike.Protocol.ResultCode.message(:ok)
      "Operation succeeded"

      iex> Aerospike.Protocol.ResultCode.message(:key_not_found)
      "Key not found"

      iex> Aerospike.Protocol.ResultCode.message(:unknown)
      "Unknown error code"

  """
  @spec message(atom()) :: String.t()
  def message(atom) when is_atom(atom) do
    Map.get(@messages, atom, "Unknown error code")
  end

  @doc """
  Returns true if the result code indicates success.

  ## Examples

      iex> Aerospike.Protocol.ResultCode.success?(:ok)
      true

      iex> Aerospike.Protocol.ResultCode.success?(:key_not_found)
      false

  """
  @spec success?(atom()) :: boolean()
  def success?(:ok), do: true
  def success?(_), do: false

  @doc """
  Returns the list of all known result code atoms.
  """
  @spec all_codes() :: [atom()]
  def all_codes, do: Map.values(@codes)

  @doc """
  Returns the mapping of integer codes to atoms.
  """
  @spec codes_map() :: %{integer() => atom()}
  def codes_map, do: @codes
end
