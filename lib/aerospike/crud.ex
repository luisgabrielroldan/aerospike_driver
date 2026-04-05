defmodule Aerospike.CRUD do
  @moduledoc false

  import Bitwise

  # Implements the core single-record commands: put, get, delete, exists, touch, operate.
  #
  # Each command follows the same pipeline:
  # 1. Merge per-command defaults from ETS with caller-supplied opts.
  # 2. Encode the request as an AsmMsg with policy flags applied.
  # 3. Route to the correct node via the partition map and send the wire bytes.
  # 4. Decode the response and emit telemetry.

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.OperateFlags
  alias Aerospike.Protocol.Response
  alias Aerospike.Router

  # Coerces atom bin names to strings; the wire protocol uses string bin names.
  @doc false
  @spec normalize_bins(map()) :: %{String.t() => term()}
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
  @spec put(atom(), Key.t(), map(), keyword()) :: :ok | {:error, Error.t()}
  def put(conn, %Key{} = key, bins, opts \\ []) when is_atom(conn) and is_list(opts) do
    bins = normalize_bins(bins)
    merged = Policy.merge_defaults(conn, :write, opts)

    with_telemetry(:put, key, conn, fn ->
      run_write(conn, key, bins, merged, Operation.op_write())
    end)
  end

  @doc """
  Atomically adds integer deltas to bins for `key`.
  """
  @spec add(atom(), Key.t(), map(), keyword()) :: :ok | {:error, Error.t()}
  def add(conn, %Key{} = key, bins, opts \\ []) when is_atom(conn) and is_list(opts) do
    bins = normalize_bins(bins)
    merged = Policy.merge_defaults(conn, :write, opts)

    with_telemetry(:add, key, conn, fn ->
      run_write(conn, key, bins, merged, Operation.op_add())
    end)
  end

  @doc """
  Atomically appends string suffixes to bins for `key`.
  """
  @spec append(atom(), Key.t(), map(), keyword()) :: :ok | {:error, Error.t()}
  def append(conn, %Key{} = key, bins, opts \\ []) when is_atom(conn) and is_list(opts) do
    bins = normalize_bins(bins)
    merged = Policy.merge_defaults(conn, :write, opts)

    with_telemetry(:append, key, conn, fn ->
      run_write(conn, key, bins, merged, Operation.op_append())
    end)
  end

  @doc """
  Atomically prepends string prefixes to bins for `key`.
  """
  @spec prepend(atom(), Key.t(), map(), keyword()) :: :ok | {:error, Error.t()}
  def prepend(conn, %Key{} = key, bins, opts \\ []) when is_atom(conn) and is_list(opts) do
    bins = normalize_bins(bins)
    merged = Policy.merge_defaults(conn, :write, opts)

    with_telemetry(:prepend, key, conn, fn ->
      run_write(conn, key, bins, merged, Operation.op_prepend())
    end)
  end

  defp run_write(conn, key, bins, merged, op_type) do
    wire = encode_write(key, bins, merged, op_type)
    router_then_decode(conn, key, wire, merged, &finish_write/2)
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
    wire = encode_get(key, merged)
    router_then_decode(conn, key, wire, merged, &finish_get(&1, &2, key))
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
    wire = encode_delete(key, merged)
    router_then_decode(conn, key, wire, merged, &finish_delete/2)
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
    wire = encode_exists(key, merged)
    router_then_decode(conn, key, wire, merged, &finish_exists/2)
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
    wire = encode_touch(key, merged)
    router_then_decode(conn, key, wire, merged, &finish_write/2)
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
    wire = encode_operate(key, ops, merged)
    router_then_decode(conn, key, wire, merged, &finish_operate(&1, &2, key))
  end

  defp finish_operate(msg, node, key) do
    case Response.parse_record_response(msg, key) do
      {:ok, _} = ok -> {ok, node}
      {:error, _} = err -> {err, node}
    end
  end

  defp encode_operate(key, ops, merged) do
    st = OperateFlags.scan_ops(ops)

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
    |> AsmMsg.encode()
    |> Message.encode_as_msg()
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

  # Returns `{result, node_name}` so telemetry can tag the responding node.
  defp router_then_decode(conn, key, wire, merged, on_msg) do
    case Router.run(conn, key, wire, merged) do
      {:ok, body, node} -> with_decoded_msg(body, node, on_msg)
      {:error, %Error{} = e} -> {{:error, e}, nil}
    end
  end

  defp with_decoded_msg(body, node, on_msg) do
    case AsmMsg.decode(body) do
      {:ok, msg} ->
        on_msg.(msg, node)

      {:error, reason} ->
        {{:error,
          Error.from_result_code(:parse_error,
            message: "decode failed: #{inspect(reason)}"
          )}, node}
    end
  end

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

  defp encode_write(key, bins, merged, op_type) do
    ops = Value.encode_bin_operations(bins, op_type)

    key
    |> base_write_msg(ops)
    |> Policy.apply_write_policy(merged)
    |> Policy.apply_send_key(key, merged)
    |> AsmMsg.encode()
    |> Message.encode_as_msg()
  end

  defp base_write_msg(%Key{} = key, ops) do
    AsmMsg.write_command(key.namespace, key.set, key.digest, ops)
  end

  defp encode_get(key, merged) do
    key
    |> Policy.read_message_for_opts(merged)
    |> Policy.apply_read_policy(merged)
    |> AsmMsg.encode()
    |> Message.encode_as_msg()
  end

  defp encode_delete(key, merged) do
    key
    |> base_delete_msg()
    |> Policy.apply_delete_policy(merged)
    |> AsmMsg.encode()
    |> Message.encode_as_msg()
  end

  defp base_delete_msg(%Key{} = key) do
    AsmMsg.delete_command(key.namespace, key.set, key.digest)
  end

  defp encode_exists(key, merged) do
    key
    |> base_exists_msg()
    |> Policy.apply_read_policy(merged)
    |> AsmMsg.encode()
    |> Message.encode_as_msg()
  end

  defp base_exists_msg(%Key{} = key) do
    AsmMsg.exists_command(key.namespace, key.set, key.digest)
  end

  defp encode_touch(key, merged) do
    key
    |> base_touch_msg()
    |> Policy.apply_touch_policy(merged)
    |> AsmMsg.encode()
    |> Message.encode_as_msg()
  end

  defp base_touch_msg(%Key{} = key) do
    AsmMsg.touch_command(key.namespace, key.set, key.digest)
  end
end
