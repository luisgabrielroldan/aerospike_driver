defmodule Aerospike.Protocol.ScanResponse do
  @moduledoc false

  import Bitwise

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.ResultCode
  alias Aerospike.Record

  @typedoc """
  Partition progress from a scan/query AS_MSG frame (`partition_done` field).

  Same keys and meaning as `t:Aerospike.PartitionFilter.partition_entry/0` (`id`, `digest`, `bval`);
  kept as a separate type because values are produced by the wire decoder, not user input.
  """
  @type partition_done_info :: %{
          id: integer(),
          digest: binary() | nil,
          bval: integer() | nil
        }

  @type frame_result :: %{
          required(:records) => [Record.t()],
          required(:partition_done) => [partition_done_info()] | nil,
          required(:last?) => boolean()
        }

  @doc """
  Parses a concatenated scan/query response body (multiple AS_MSG bodies, no 8-byte proto framing).

  Returns records in order and accumulated partition-done markers for cursor tracking.
  The buffer must end on a terminal `INFO3_LAST` frame (complete server response).
  """
  @spec parse(binary(), String.t(), String.t() | nil) ::
          {:ok, [Record.t()], [partition_done_info()]} | {:error, Error.t()}
  def parse(body, namespace, set_name \\ nil)
      when is_binary(body) and is_binary(namespace) and
             (is_binary(set_name) or is_nil(set_name)) do
    set = if is_nil(set_name), do: "", else: set_name

    case do_parse(body, namespace, set, [], []) do
      {:ok, records, parts, true} ->
        {:ok, records, parts}

      {:ok, _records, _parts, false} ->
        {:error,
         Error.from_result_code(:parse_error,
           message: "incomplete scan response: expected LAST frame"
         )}

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Parses AS_MSG bodies from one received protocol chunk while streaming.

  `done?` is `true` only when a LAST frame ended this chunk with no trailing bytes.
  """
  @spec parse_stream_chunk(binary(), String.t(), String.t() | nil) ::
          {:ok, [Record.t()], [partition_done_info()], boolean()} | {:error, Error.t()}
  def parse_stream_chunk(body, namespace, set_name \\ nil)
      when is_binary(body) and is_binary(namespace) and
             (is_binary(set_name) or is_nil(set_name)) do
    set = if is_nil(set_name), do: "", else: set_name

    case do_parse(body, namespace, set, [], []) do
      {:ok, records, parts, done?} -> {:ok, records, parts, done?}
      {:error, _} = err -> err
    end
  end

  @doc """
  Parses exactly one AS_MSG body (for lazy single-frame reads).

  Fails if trailing bytes remain after one message.
  """
  @spec parse_frame(binary(), String.t(), String.t() | nil) ::
          {:ok, frame_result()} | {:error, Error.t()}
  def parse_frame(body, namespace, set_name \\ nil)
      when is_binary(body) and is_binary(namespace) and
             (is_binary(set_name) or is_nil(set_name)) do
    set = if is_nil(set_name), do: "", else: set_name

    case read_header(body) do
      {:error, reason} ->
        {:error, Error.from_result_code(:parse_error, message: inspect(reason))}

      {:ok, meta, rest} ->
        finalize_parse_frame(process_one_frame(meta, rest, namespace, set))
    end
  end

  defp finalize_parse_frame({:ok, result, <<>>}), do: {:ok, result}

  defp finalize_parse_frame({:ok, _result, _extra}) do
    {:error, Error.from_result_code(:parse_error, message: "trailing bytes after AS_MSG frame")}
  end

  defp finalize_parse_frame({:error, _} = err), do: err

  defp do_parse(<<>>, _namespace, _set, records, parts) do
    {:ok, Enum.reverse(records), Enum.reverse(parts), false}
  end

  defp do_parse(data, namespace, set, records, parts) do
    case read_header(data) do
      {:error, reason} ->
        {:error, Error.from_result_code(:parse_error, message: inspect(reason))}

      {:ok, meta, rest} ->
        case process_one_frame(meta, rest, namespace, set) do
          {:ok, result, tail} -> continue_parse(result, tail, namespace, set, records, parts)
          {:error, _} = err -> err
        end
    end
  end

  defp continue_parse(result, tail, namespace, set, records, parts) do
    records2 = prepend_records(records, result.records)
    parts2 = prepend_parts(parts, result.partition_done)

    if result.last? do
      finish_scan(records2, parts2, tail)
    else
      do_parse(tail, namespace, set, records2, parts2)
    end
  end

  defp finish_scan(records, parts, <<>>) do
    {:ok, Enum.reverse(records), Enum.reverse(parts), true}
  end

  defp finish_scan(_records, _parts, _tail) do
    {:error,
     Error.from_result_code(:parse_error, message: "trailing bytes after LAST scan frame")}
  end

  defp prepend_records(acc, []), do: acc
  defp prepend_records(acc, [r | more]), do: prepend_records([r | acc], more)

  defp prepend_parts(acc, nil), do: acc
  defp prepend_parts(acc, []), do: acc
  defp prepend_parts(acc, [p | more]), do: prepend_parts([p | acc], more)

  defp process_one_frame(meta, rest, namespace, default_set) do
    case decode_fields(rest, meta.field_count) do
      {:error, reason} ->
        {:error, Error.from_result_code(:parse_error, message: inspect(reason))}

      {:ok, fields, after_fields} ->
        dispatch_frame(meta, fields, after_fields, namespace, default_set)
    end
  end

  defp dispatch_frame(meta, fields, after_fields, ns, default_set) do
    last? = meta.last?
    partition_done? = partition_done_flag?(meta)

    cond do
      partition_done? ->
        handle_partition_done(meta, fields, after_fields, ns, default_set)

      last? and fatal_error_rc?(meta.rc) ->
        skip_ops_then(after_fields, meta.op_count, fn _tail ->
          {:fatal, code} = last_frame_rc(meta.rc)
          {:error, Error.from_result_code(code)}
        end)

      last? ->
        skip_ops_then(after_fields, meta.op_count, fn tail ->
          {:ok, %{records: [], partition_done: nil, last?: true}, tail}
        end)

      stream_terminal_rc?(meta.rc) ->
        skip_ops_then(after_fields, meta.op_count, fn tail ->
          {:ok, %{records: [], partition_done: nil, last?: true}, tail}
        end)

      partition_skip_rc?(meta.rc) ->
        skip_ops_then(after_fields, meta.op_count, fn tail ->
          {:ok, %{records: [], partition_done: nil, last?: false}, tail}
        end)

      true ->
        decode_record_frame(meta, fields, after_fields, ns, default_set)
    end
  end

  defp partition_done_flag?(meta),
    do: (meta.info3 &&& AsmMsg.info3_partition_done()) != 0

  defp fatal_error_rc?(rc), do: match?({:fatal, _}, last_frame_rc(rc))

  defp handle_partition_done(meta, fields, after_fields, _ns, _default_set) do
    last? = meta.last?
    pinfo = partition_done_from_fields(meta.generation, fields)

    skip_ops_then(after_fields, meta.op_count, fn tail ->
      {:ok, %{records: [], partition_done: [pinfo], last?: last?}, tail}
    end)
  end

  defp decode_record_frame(meta, fields, after_fields, ns, default_set) do
    case decode_operations_to_bins(after_fields, meta.op_count) do
      {:ok, bins, tail} ->
        case build_record(meta, fields, bins, ns, default_set) do
          {:ok, record} ->
            {:ok, %{records: [record], partition_done: nil, last?: false}, tail}

          {:error, _} = err ->
            err
        end

      {:error, %Error{} = e} ->
        {:error, e}

      {:error, r} ->
        {:error, Error.from_result_code(:parse_error, message: inspect(r))}
    end
  end

  defp skip_ops_then(data, n, continuation) do
    case skip_operations(data, n) do
      {:ok, tail} -> continuation.(tail)
      {:error, r} -> {:error, Error.from_result_code(:parse_error, message: inspect(r))}
    end
  end

  defp read_header(
         <<22::8, _info1::8, _info2::8, info3::8, _info4::8, rc::8, generation::32-big,
           expiration::32-big, _timeout::32-signed-big, field_count::16-big, op_count::16-big,
           rest::binary>>
       ) do
    {:ok,
     %{
       info3: info3,
       rc: rc,
       generation: generation,
       expiration: expiration,
       field_count: field_count,
       op_count: op_count,
       last?: (info3 &&& AsmMsg.info3_last()) != 0
     }, rest}
  end

  defp read_header(_), do: {:error, :short_header}

  defp last_frame_rc(rc_int) do
    case ResultCode.from_integer(rc_int) do
      {:ok, :ok} -> :ok
      {:ok, :key_not_found} -> :ok
      {:ok, :filtered_out} -> :ok
      {:ok, other} -> {:fatal, other}
      {:error, _} -> {:fatal, :server_error}
    end
  end

  # Non-zero RCs that signal end-of-stream (matching Go client behavior).
  # The server uses these instead of (or in addition to) INFO3_LAST in some
  # modes — notably SC-namespace query responses.
  #
  # Per-record codes like :key_not_found and :filtered_out are NOT terminal —
  # they appear in batch/scan/query responses for individual records that are
  # missing or filtered. The Go client treats them as skip-and-continue
  # (multi_command.go: parseResult).
  defp stream_terminal_rc?(rc_int) do
    case ResultCode.from_integer(rc_int) do
      {:ok, :ok} -> false
      {:ok, :partition_unavailable} -> false
      {:ok, :key_not_found} -> false
      {:ok, :filtered_out} -> false
      _ -> true
    end
  end

  # Non-zero RCs that appear mid-stream as per-record statuses. The frame
  # carries no record data — skip it and continue reading the next frame.
  defp partition_skip_rc?(rc_int) do
    case ResultCode.from_integer(rc_int) do
      {:ok, :partition_unavailable} -> true
      {:ok, :key_not_found} -> true
      {:ok, :filtered_out} -> true
      _ -> false
    end
  end

  defp decode_fields(data, 0), do: {:ok, [], data}

  defp decode_fields(data, n) when n > 0 do
    case Field.decode(data) do
      {:ok, field, rest} ->
        case decode_fields(rest, n - 1) do
          {:ok, acc, tail} -> {:ok, [field | acc], tail}
          {:error, _} = err -> err
        end

      {:error, _} = err ->
        err
    end
  end

  # Same layout as Field.decode/1: <<size::32-big>> then `size` bytes (type + data).
  defp skip_fields(data, 0), do: {:ok, data}

  defp skip_fields(<<size::32-big, _::binary-size(size), rest::binary>>, n)
       when n > 0 and size > 0 do
    skip_fields(rest, n - 1)
  end

  defp skip_fields(<<0::32-big, _::binary>>, _n), do: {:error, :invalid_field_size}

  defp skip_fields(_, _), do: {:error, :incomplete_field}

  defp partition_done_from_fields(partition_id, fields) do
    digest =
      case Enum.find(fields, &(&1.type == Field.type_digest())) do
        %Field{data: d} when is_binary(d) and byte_size(d) == 20 -> d
        _ -> nil
      end

    bval = bval_from_fields(fields)
    %{id: partition_id, digest: digest, bval: bval}
  end

  defp bval_from_fields(fields) do
    case Enum.find(fields, &(&1.type == Field.type_bval_array())) do
      %Field{data: <<n::64-signed-little>>} -> n
      %Field{data: _} -> nil
      _ -> nil
    end
  end

  defp extract_record_fields(fields) do
    Enum.reduce(fields, %{}, fn %Field{type: t, data: d}, acc ->
      cond do
        t == Field.type_namespace() -> Map.put_new(acc, :ns, d)
        t == Field.type_table() -> Map.put_new(acc, :set, d)
        t == Field.type_digest() -> Map.put_new(acc, :digest, d)
        t == Field.type_key() -> Map.put_new(acc, :key_data, d)
        true -> acc
      end
    end)
  end

  defp build_record(meta, fields, bins, default_ns, default_set) do
    extracted = extract_record_fields(fields)
    ns = Map.get(extracted, :ns, default_ns)
    set = Map.get(extracted, :set, default_set)

    case Map.get(extracted, :digest) do
      d when is_binary(d) and byte_size(d) == 20 ->
        base = Key.from_digest(ns, set, d)
        key = apply_user_key(base, Map.get(extracted, :key_data))

        {:ok,
         %Record{
           key: key,
           bins: bins,
           generation: meta.generation,
           ttl: meta.expiration
         }}

      _ ->
        {:error, Error.from_result_code(:parse_error, message: "scan record missing DIGEST_RIPE")}
    end
  end

  defp apply_user_key(key, <<1::8, n::64-signed-big>>), do: %{key | user_key: n}
  defp apply_user_key(key, <<3::8, s::binary>>), do: %{key | user_key: s}
  defp apply_user_key(key, _), do: key

  defp skip_operations(data, 0), do: {:ok, data}

  defp skip_operations(data, n) when n > 0 do
    case Operation.decode(data) do
      {:ok, _op, rest} -> skip_operations(rest, n - 1)
      {:error, _} = err -> err
    end
  end

  defp decode_operations_to_bins(data, op_count) do
    decode_ops(data, op_count, %{})
  end

  defp decode_ops(data, 0, bins), do: {:ok, bins, data}

  defp decode_ops(data, n, bins) when n > 0 do
    case Operation.decode(data) do
      {:ok, %Operation{bin_name: name} = op, rest} when name != "" ->
        case Value.decode_value(op.particle_type, op.data) do
          {:ok, v} -> decode_ops(rest, n - 1, Map.put(bins, name, v))
          {:error, _} = err -> err
        end

      {:ok, _op, rest} ->
        decode_ops(rest, n - 1, bins)

      {:error, _} = err ->
        err
    end
  end

  @doc false
  @spec count_records(binary()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count_records(body) when is_binary(body) do
    case do_count(body, 0) do
      {:ok, count, true} ->
        {:ok, count}

      {:ok, _count, false} ->
        {:error,
         Error.from_result_code(:parse_error,
           message: "incomplete scan response: expected LAST frame"
         )}

      {:error, _} = err ->
        err
    end
  end

  defp do_count(<<>>, count), do: {:ok, count, false}

  defp do_count(data, count) do
    with {:ok, meta, rest} <- read_header(data),
         {:ok, after_fields} <- skip_fields(rest, meta.field_count),
         {:ok, tail} <- skip_operations(after_fields, meta.op_count) do
      count_dispatch(meta, tail, count)
    else
      {:error, reason} -> {:error, Error.from_result_code(:parse_error, message: inspect(reason))}
    end
  end

  defp count_dispatch(meta, tail, count) do
    cond do
      partition_done_flag?(meta) -> count_partition_done(meta, tail, count)
      meta.last? or stream_terminal_rc?(meta.rc) -> count_terminal(meta, tail, count)
      partition_skip_rc?(meta.rc) -> do_count(tail, count)
      true -> do_count(tail, count + 1)
    end
  end

  defp count_partition_done(meta, tail, count) do
    if meta.last?, do: finish_count(count, tail), else: do_count(tail, count)
  end

  defp count_terminal(meta, tail, count) do
    if fatal_error_rc?(meta.rc), do: count_fatal(meta), else: finish_count(count, tail)
  end

  defp count_fatal(meta) do
    {:fatal, code} = last_frame_rc(meta.rc)
    {:error, Error.from_result_code(code)}
  end

  defp finish_count(count, <<>>), do: {:ok, count, true}

  defp finish_count(_count, _trailing) do
    {:error,
     Error.from_result_code(:parse_error, message: "trailing bytes after LAST scan frame")}
  end

  @doc false
  @spec lazy_stream_chunk_terminal?(binary()) :: boolean()
  def lazy_stream_chunk_terminal?(body) when is_binary(body) do
    case scan_concatenated_as_msgs_for_terminal(body) do
      {:ok, :terminal} -> true
      {:ok, :continue} -> false
      {:error, _} -> true
    end
  end

  defp scan_concatenated_as_msgs_for_terminal(data) do
    case skip_one_message(data) do
      {:ok, meta, tail} ->
        cond do
          meta.last? and tail != <<>> ->
            {:error, :trailing_after_last}

          meta.last? ->
            {:ok, :terminal}

          tcp_stop_after_message?(meta) ->
            {:ok, :terminal}

          tail == <<>> ->
            {:ok, :continue}

          true ->
            scan_concatenated_as_msgs_for_terminal(tail)
        end

      {:error, _} ->
        {:ok, :terminal}
    end
  end

  defp skip_one_message(data) do
    with {:ok, meta, rest} <- read_header(data),
         {:ok, after_fields} <- skip_fields(rest, meta.field_count),
         {:ok, tail} <- skip_operations(after_fields, meta.op_count) do
      {:ok, meta, tail}
    end
  end

  defp tcp_stop_after_message?(meta) do
    case ResultCode.from_integer(meta.rc) do
      {:ok, :ok} -> false
      {:ok, :partition_unavailable} -> false
      {:ok, :key_not_found} -> false
      {:ok, :filtered_out} -> false
      _ -> true
    end
  end
end
