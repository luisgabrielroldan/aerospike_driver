defmodule Aerospike.CRUD do
  @moduledoc false

  import Bitwise

  # Implements the core single-record commands: put, get, delete, exists, touch, operate.
  #
  # Each command follows the same pipeline:
  # 1. Merge per-command defaults from ETS with caller-supplied opts.
  # 2. If a transaction is active, run pre-flight checks (verify state, register keys).
  # 3. Encode the request as an AsmMsg with policy flags and MRT fields applied.
  # 4. Route to the correct node via the partition map and send the wire bytes.
  # 5. Decode the response, track transaction state, and emit telemetry.

  alias Aerospike.CircuitBreaker
  alias Aerospike.Error
  alias Aerospike.Exp
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.MessagePack
  alias Aerospike.Protocol.OperateFlags
  alias Aerospike.Protocol.Response
  alias Aerospike.Router
  alias Aerospike.Txn
  alias Aerospike.TxnMonitor
  alias Aerospike.TxnOps

  # Coerces atom bin names to strings; the wire protocol uses string bin names.
  @doc false
  @spec normalize_bins(Aerospike.Record.bins_input()) :: Aerospike.Record.bins()
  def normalize_bins(bins) when is_map(bins) do
    Map.new(bins, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} when is_binary(k) -> {k, v}
      {k, _} -> raise ArgumentError, "bin name must be a string or atom, got: #{inspect(k)}"
    end)
  end

  @doc """
  Writes bins for `key` using merged connection defaults and `opts`.
  """
  @spec put(atom(), Key.t(), Aerospike.Record.bins_input(), keyword()) ::
          :ok | {:error, Error.t()}
  def put(conn, %Key{} = key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    bins = normalize_bins(bins)
    merged = Policy.merge_defaults(conn, :write, opts)

    with_telemetry(:put, key, conn, fn ->
      run_write(conn, key, bins, merged, Operation.op_write())
    end)
  end

  @doc """
  Atomically adds integer deltas to bins for `key`.
  """
  @spec add(atom(), Key.t(), Aerospike.Record.bins_input(), keyword()) ::
          :ok | {:error, Error.t()}
  def add(conn, %Key{} = key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    bins = normalize_bins(bins)
    merged = Policy.merge_defaults(conn, :write, opts)

    with_telemetry(:add, key, conn, fn ->
      run_write(conn, key, bins, merged, Operation.op_add())
    end)
  end

  @doc """
  Atomically appends string suffixes to bins for `key`.
  """
  @spec append(atom(), Key.t(), Aerospike.Record.bins_input(), keyword()) ::
          :ok | {:error, Error.t()}
  def append(conn, %Key{} = key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    bins = normalize_bins(bins)
    merged = Policy.merge_defaults(conn, :write, opts)

    with_telemetry(:append, key, conn, fn ->
      run_write(conn, key, bins, merged, Operation.op_append())
    end)
  end

  @doc """
  Atomically prepends string prefixes to bins for `key`.
  """
  @spec prepend(atom(), Key.t(), Aerospike.Record.bins_input(), keyword()) ::
          :ok | {:error, Error.t()}
  def prepend(conn, %Key{} = key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    bins = normalize_bins(bins)
    merged = Policy.merge_defaults(conn, :write, opts)

    with_telemetry(:prepend, key, conn, fn ->
      run_write(conn, key, bins, merged, Operation.op_prepend())
    end)
  end

  defp run_write(conn, key, bins, merged, op_type) do
    txn = Keyword.get(merged, :txn)

    case prepare_txn_write(conn, txn, key, merged) do
      :ok ->
        wire = encode_write(conn, key, bins, merged, op_type)
        txn_track = txn_track_info(conn, txn, key, :write)
        router_then_decode(conn, key, wire, merged, &finish_write/2, txn_track)

      {:error, _} = err ->
        {err, nil}
    end
  end

  defp finish_write(msg, node) do
    case Response.parse_write_response(msg) do
      :ok -> {:ok, node}
      {:error, _} = err -> {err, node}
    end
  end

  @doc """
  Reads a record for `key`.
  """
  @spec get(atom(), Key.t(), keyword()) :: {:ok, Aerospike.Record.t()} | {:error, Error.t()}
  def get(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    merged = Policy.merge_defaults(conn, :read, opts)

    with_telemetry(:get, key, conn, fn -> run_get(conn, key, merged) end)
  end

  defp run_get(conn, key, merged) do
    txn = Keyword.get(merged, :txn)

    case prepare_txn_read(conn, txn, key) do
      :ok ->
        wire = encode_get(conn, key, merged)
        txn_track = txn_track_info(conn, txn, key, :read)
        router_then_decode(conn, key, wire, merged, &finish_get(&1, &2, key), txn_track)

      {:error, _} = err ->
        {err, nil}
    end
  end

  defp finish_get(msg, node, key) do
    case Response.parse_record_response(msg, key) do
      {:ok, _} = ok -> {ok, node}
      {:error, _} = err -> {err, node}
    end
  end

  @doc """
  Deletes the record for `key`. Returns whether a record existed and was removed.
  """
  @spec delete(atom(), Key.t(), keyword()) :: {:ok, boolean()} | {:error, Error.t()}
  def delete(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    merged = Policy.merge_defaults(conn, :delete, opts)

    with_telemetry(:delete, key, conn, fn -> run_delete(conn, key, merged) end)
  end

  defp run_delete(conn, key, merged) do
    txn = Keyword.get(merged, :txn)

    case prepare_txn_write(conn, txn, key, merged) do
      :ok ->
        wire = encode_delete(conn, key, merged)
        txn_track = txn_track_info(conn, txn, key, :write)
        router_then_decode(conn, key, wire, merged, &finish_delete/2, txn_track)

      {:error, _} = err ->
        {err, nil}
    end
  end

  defp finish_delete(msg, node) do
    case Response.parse_delete_response(msg) do
      {:ok, _} = ok -> {ok, node}
      {:error, _} = err -> {err, node}
    end
  end

  @doc """
  Returns whether a record exists for `key`.
  """
  @spec exists(atom(), Key.t(), keyword()) :: {:ok, boolean()} | {:error, Error.t()}
  def exists(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    merged = Policy.merge_defaults(conn, :exists, opts)

    with_telemetry(:exists, key, conn, fn -> run_exists(conn, key, merged) end)
  end

  defp run_exists(conn, key, merged) do
    txn = Keyword.get(merged, :txn)

    case prepare_txn_read(conn, txn, key) do
      :ok ->
        wire = encode_exists(conn, key, merged)
        txn_track = txn_track_info(conn, txn, key, :read)
        router_then_decode(conn, key, wire, merged, &finish_exists/2, txn_track)

      {:error, _} = err ->
        {err, nil}
    end
  end

  defp finish_exists(msg, node) do
    case Response.parse_exists_response(msg) do
      {:ok, _} = ok -> {ok, node}
      {:error, _} = err -> {err, node}
    end
  end

  @doc """
  Refreshes TTL for `key` without changing bins.
  """
  @spec touch(atom(), Key.t(), keyword()) :: :ok | {:error, Error.t()}
  def touch(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    merged = Policy.merge_defaults(conn, :touch, opts)

    with_telemetry(:touch, key, conn, fn -> run_touch(conn, key, merged) end)
  end

  defp run_touch(conn, key, merged) do
    txn = Keyword.get(merged, :txn)

    case prepare_txn_write(conn, txn, key, merged) do
      :ok ->
        wire = encode_touch(conn, key, merged)
        txn_track = txn_track_info(conn, txn, key, :write)
        router_then_decode(conn, key, wire, merged, &finish_write/2, txn_track)

      {:error, _} = err ->
        {err, nil}
    end
  end

  @doc """
  Runs an atomic multi-operation command on a single record.
  """
  @spec operate(atom(), Key.t(), [Operation.t()], keyword()) ::
          {:ok, Aerospike.Record.t()} | {:error, Error.t()}
  def operate(conn, %Key{} = key, ops, opts \\ [])
      when is_atom(conn) and is_list(ops) and is_list(opts) do
    merged = Policy.merge_defaults(conn, :operate, opts)

    with_telemetry(:operate, key, conn, fn -> run_operate(conn, key, ops, merged) end)
  end

  defp run_operate(_conn, _key, [], _merged) do
    {{:error,
      Error.from_result_code(:parameter_error,
        message: "operate requires a non-empty operation list"
      )}, nil}
  end

  defp run_operate(conn, key, ops, merged) do
    txn = Keyword.get(merged, :txn)
    st = OperateFlags.scan_ops(ops)

    case prepare_txn_for_operate(conn, txn, key, st.has_write?, merged) do
      :ok ->
        wire = encode_operate(conn, key, ops, merged, st)
        kind = if st.has_write?, do: :write, else: :read
        txn_track = txn_track_info(conn, txn, key, kind)
        router_then_decode(conn, key, wire, merged, &finish_operate(&1, &2, key), txn_track)

      {:error, _} = err ->
        {err, nil}
    end
  end

  defp finish_operate(msg, node, key) do
    case Response.parse_record_response(msg, key) do
      {:ok, _} = ok -> {ok, node}
      {:error, _} = err -> {err, node}
    end
  end

  @doc """
  Executes a UDF (User Defined Function) on a single record.

  `package` is the Lua module name as registered on the server (without the `.lua` extension).
  `function` is the Lua function name within that module.
  `args` is the list of arguments passed to the function.
  """
  @spec apply_udf(atom(), Key.t(), String.t(), String.t(), list(), keyword()) ::
          {:ok, term()} | {:error, Error.t()}
  def apply_udf(conn, %Key{} = key, package, function, args, opts \\ [])
      when is_atom(conn) and is_binary(package) and is_binary(function) and is_list(args) and
             is_list(opts) do
    merged = Policy.merge_defaults(conn, :write, opts)

    with_telemetry(:apply_udf, key, conn, fn ->
      wire = encode_udf(key, package, function, args, merged)
      router_then_decode(conn, key, wire, merged, &finish_udf/2)
    end)
  end

  defp finish_udf(msg, node) do
    case Response.parse_udf_response(msg) do
      {:ok, _} = ok -> {ok, node}
      {:error, _} = err -> {err, node}
    end
  end

  defp encode_udf(key, package, function, args, merged) do
    arglist = MessagePack.pack!(Enum.map(args, &pack_udf_arg/1))

    %AsmMsg{
      info2: AsmMsg.info2_write(),
      fields: [
        Field.namespace(key.namespace),
        Field.set(key.set),
        Field.digest(key.digest),
        Field.udf_package_name(package),
        Field.udf_function(function),
        Field.udf_arglist(arglist),
        Field.udf_op(1)
      ]
    }
    |> Policy.apply_write_policy(merged)
    |> apply_filter_exp(merged)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  # UDF args use Aerospike-aware MessagePack encoding: strings carry the particle
  # type byte prefix (0x03) inside the msgpack str payload, matching how official
  # clients encode ValueArray elements for the wire protocol.
  defp pack_udf_arg(s) when is_binary(s), do: {:particle_string, s}
  defp pack_udf_arg({:bytes, b}) when is_binary(b), do: {:bytes, b}
  defp pack_udf_arg(nil), do: nil
  defp pack_udf_arg(true), do: true
  defp pack_udf_arg(false), do: false
  defp pack_udf_arg(n) when is_integer(n), do: n
  defp pack_udf_arg(f) when is_float(f), do: f
  defp pack_udf_arg(list) when is_list(list), do: Enum.map(list, &pack_udf_arg/1)

  defp pack_udf_arg(%{} = map) do
    Map.new(map, fn {k, v} -> {pack_udf_arg(k), pack_udf_arg(v)} end)
  end

  defp encode_operate(conn, key, ops, merged, st) do
    info1 =
      if st.header_only? do
        st.info1 ||| AsmMsg.info1_nobindata()
      else
        st.info1
      end

    info2 = if st.has_write?, do: st.info2 ||| AsmMsg.info2_write(), else: st.info2

    info2 =
      if respond_all_ops?(merged, st, info1) do
        info2 ||| AsmMsg.info2_respond_all_ops()
      else
        info2
      end

    base = %AsmMsg{
      info1: info1,
      info2: info2,
      info3: st.info3,
      fields: operate_fields(key),
      operations: ops
    }

    base
    |> Policy.apply_operate_policy(merged, st.has_write?)
    |> Policy.apply_send_key(key, merged)
    |> maybe_add_mrt_fields(conn, key, merged, st.has_write?)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp operate_fields(%Key{} = key) do
    [
      Field.namespace(key.namespace),
      Field.set(key.set),
      Field.digest(key.digest)
    ]
  end

  defp respond_all_ops?(merged, st, info1) do
    want? = st.respond_all? or Keyword.get(merged, :respond_per_each_op, false)
    get_all? = (info1 &&& AsmMsg.info1_get_all()) != 0
    want? and not get_all?
  end

  defp router_then_decode(conn, key, wire, merged, on_msg, txn_track \\ nil) do
    case Router.run(conn, key, wire, merged) do
      {:ok, body, node} ->
        case AsmMsg.decode(body) do
          {:ok, msg} ->
            result = on_msg.(msg, node)
            maybe_record_device_overload(conn, node, result)
            track_txn_response(txn_track, msg, result)
            result

          {:error, reason} ->
            {{:error,
              Error.from_result_code(:parse_error,
                message: "decode failed: #{inspect(reason)}"
              )}, node}
        end

      {:error, %Error{} = e} ->
        track_txn_in_doubt(txn_track, e)
        {{:error, e}, nil}
    end
  end

  defp maybe_record_device_overload(conn, node, {{:error, %Error{code: :device_overload}}, _}) do
    CircuitBreaker.record_error(conn, node, :device_overload)
  end

  defp maybe_record_device_overload(conn, node, {:error, %Error{code: :device_overload}}) do
    CircuitBreaker.record_error(conn, node, :device_overload)
  end

  defp maybe_record_device_overload(_conn, _node, _result), do: :ok

  # Wraps the command in a `:telemetry.span` so callers can observe latency,
  # success/failure, and which node handled the request.
  defp with_telemetry(command, key, conn, fun) when is_atom(command) and is_atom(conn) do
    meta = %{command: command, namespace: key.namespace, set: key.set, conn: conn}

    :telemetry.span([:aerospike, :command], meta, fn ->
      {result, node} = fun.()
      stop = %{result: telemetry_result(result)}
      stop = if node, do: Map.put(stop, :node, node), else: stop
      {result, stop}
    end)
  end

  defp telemetry_result(:ok), do: :ok
  defp telemetry_result({:ok, _}), do: :ok
  defp telemetry_result({:error, %Error{code: code}}), do: {:error, code}

  defp encode_write(conn, key, bins, merged, op_type) do
    ops = Value.encode_bin_operations(bins, op_type)

    key
    |> base_write_msg(ops)
    |> Policy.apply_write_policy(merged)
    |> Policy.apply_send_key(key, merged)
    |> apply_filter_exp(merged)
    |> maybe_add_mrt_fields(conn, key, merged, true)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp base_write_msg(%Key{} = key, ops) do
    AsmMsg.write_command(key.namespace, key.set, key.digest, ops)
  end

  defp encode_get(conn, key, merged) do
    key
    |> Policy.read_message_for_opts(merged)
    |> Policy.apply_read_policy(merged)
    |> apply_filter_exp(merged)
    |> maybe_add_mrt_fields(conn, key, merged, false)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp encode_delete(conn, key, merged) do
    key
    |> base_delete_msg()
    |> Policy.apply_delete_policy(merged)
    |> apply_filter_exp(merged)
    |> maybe_add_mrt_fields(conn, key, merged, true)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp base_delete_msg(%Key{} = key) do
    AsmMsg.delete_command(key.namespace, key.set, key.digest)
  end

  defp encode_exists(conn, key, merged) do
    key
    |> base_exists_msg()
    |> Policy.apply_read_policy(merged)
    |> apply_filter_exp(merged)
    |> maybe_add_mrt_fields(conn, key, merged, false)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp base_exists_msg(%Key{} = key) do
    AsmMsg.exists_command(key.namespace, key.set, key.digest)
  end

  defp encode_touch(conn, key, merged) do
    key
    |> base_touch_msg()
    |> Policy.apply_touch_policy(merged)
    |> apply_filter_exp(merged)
    |> maybe_add_mrt_fields(conn, key, merged, true)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  @doc false
  def apply_filter_exp(%AsmMsg{} = msg, opts) do
    case Keyword.get(opts, :filter) do
      %Exp{wire: w} ->
        %{msg | fields: msg.fields ++ [%Field{type: Field.type_filter_exp(), data: w}]}

      _ ->
        msg
    end
  end

  defp base_touch_msg(%Key{} = key) do
    AsmMsg.touch_command(key.namespace, key.set, key.digest)
  end

  # -- Transaction integration helpers -----------------------------------------

  @doc false
  @spec maybe_add_mrt_fields(AsmMsg.t(), atom(), Key.t(), keyword(), boolean()) :: AsmMsg.t()
  def maybe_add_mrt_fields(%AsmMsg{} = msg, conn, %Key{} = key, opts, has_write) do
    case Keyword.get(opts, :txn) do
      nil -> msg
      %Txn{} = txn -> do_add_mrt_fields(msg, conn, txn, key, has_write)
    end
  end

  defp do_add_mrt_fields(msg, conn, %Txn{} = txn, %Key{} = key, has_write) do
    deadline = TxnOps.get_deadline(conn, txn)
    version = TxnOps.get_read_version(conn, txn, key)

    mrt_fields = [Field.mrt_id(txn.id)]

    mrt_fields =
      case version do
        nil -> mrt_fields
        v -> mrt_fields ++ [Field.record_version(v)]
      end

    mrt_fields =
      if has_write and deadline != 0 do
        mrt_fields ++ [Field.mrt_deadline(deadline)]
      else
        mrt_fields
      end

    %{msg | fields: msg.fields ++ mrt_fields}
  end

  defp prepare_txn_read(_conn, nil, _key), do: :ok

  defp prepare_txn_read(conn, %Txn{} = txn, %Key{} = key) do
    with :ok <- TxnOps.verify_command(conn, txn) do
      TxnOps.set_namespace(conn, txn, key.namespace)
    end
  end

  defp prepare_txn_write(_conn, nil, _key, _opts), do: :ok

  defp prepare_txn_write(conn, %Txn{} = txn, %Key{} = key, opts) do
    TxnMonitor.register_key(conn, txn, key, opts)
  end

  defp prepare_txn_for_operate(conn, txn, key, true = _has_write, opts) do
    prepare_txn_write(conn, txn, key, opts)
  end

  defp prepare_txn_for_operate(conn, txn, key, false = _has_write, _opts) do
    prepare_txn_read(conn, txn, key)
  end

  defp txn_track_info(_conn, nil, _key, _kind), do: nil
  defp txn_track_info(conn, %Txn{} = txn, key, kind), do: {conn, txn, key, kind}

  defp track_txn_response(nil, _msg, _result), do: :ok

  defp track_txn_response({conn, txn, key, :write}, msg, result) do
    version = extract_response_version(msg)
    result_code = result_code_from_pair(result)
    TxnOps.track_write(conn, txn, key, version, result_code)
  end

  defp track_txn_response({conn, txn, key, :read}, msg, _result) do
    version = extract_response_version(msg)
    TxnOps.track_read(conn, txn, key, version)
  end

  defp track_txn_in_doubt(nil, _error), do: :ok

  defp track_txn_in_doubt({conn, txn, key, :write}, %Error{code: code})
       when code in [:timeout, :network_error] do
    TxnOps.track_write_in_doubt(conn, txn, key)
  end

  defp track_txn_in_doubt(_txn_track, _error), do: :ok

  defp extract_response_version(%AsmMsg{} = msg) do
    case Response.extract_record_version(msg) do
      {:ok, v} -> v
      :none -> nil
    end
  end

  defp result_code_from_pair({:ok, _node}), do: :ok
  defp result_code_from_pair({{:ok, _}, _node}), do: :ok
  defp result_code_from_pair({{:error, %Error{code: code}}, _node}), do: code
end
