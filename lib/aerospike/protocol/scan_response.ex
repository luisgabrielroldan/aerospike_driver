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

  @info3_partition_done 0x04

  @type partition_done_info :: %{
          id: integer(),
          digest: binary() | nil,
          bval: integer() | nil,
          unavailable?: boolean()
        }

  @type frame_result :: %{
          required(:records) => [Record.t()],
          required(:partition_done) => [partition_done_info()] | nil,
          required(:last?) => boolean()
        }

  @type aggregate_frame_result :: %{
          required(:results) => [term()],
          required(:partition_done) => [partition_done_info()] | nil,
          required(:last?) => boolean()
        }

  @spec parse(binary(), String.t(), String.t() | nil) ::
          {:ok, [Record.t()], [partition_done_info()]} | {:error, Error.t()}
  def parse(body, namespace, set_name \\ nil)
      when is_binary(body) and is_binary(namespace) and
             (is_binary(set_name) or is_nil(set_name)) do
    set = if is_nil(set_name), do: "", else: set_name

    case do_parse(body, namespace, set, [], []) do
      {:ok, records, parts, true} -> {:ok, records, parts}
      {:ok, _records, _parts, false} -> incomplete_response_error()
      {:error, _} = err -> err
    end
  end

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

  @spec parse_aggregate_stream_chunk(binary(), String.t(), String.t() | nil) ::
          {:ok, [term()], [partition_done_info()], boolean()} | {:error, Error.t()}
  def parse_aggregate_stream_chunk(body, namespace, set_name \\ nil)
      when is_binary(body) and is_binary(namespace) and
             (is_binary(set_name) or is_nil(set_name)) do
    set = if is_nil(set_name), do: "", else: set_name

    case do_parse_aggregate(body, namespace, set, [], []) do
      {:ok, results, parts, done?} -> {:ok, results, parts, done?}
      {:error, _} = err -> err
    end
  end

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

  @spec count_records(binary()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count_records(body) when is_binary(body) do
    case do_count_records(body, 0) do
      {:ok, count, true} -> {:ok, count}
      {:ok, _count, false} -> incomplete_response_error()
      {:error, _} = err -> err
    end
  end

  @spec lazy_stream_chunk_terminal?(binary()) :: boolean()
  def lazy_stream_chunk_terminal?(body) when is_binary(body) do
    case scan_concatenated_as_msgs_for_terminal(body) do
      {:ok, :terminal} -> true
      {:ok, :continue} -> false
      {:error, _} -> true
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

  defp do_parse_aggregate(<<>>, _namespace, _set, results, parts) do
    {:ok, Enum.reverse(results), Enum.reverse(parts), false}
  end

  defp do_parse_aggregate(data, namespace, set, results, parts) do
    case read_header(data) do
      {:error, reason} ->
        {:error, Error.from_result_code(:parse_error, message: inspect(reason))}

      {:ok, meta, rest} ->
        case process_one_aggregate_frame(meta, rest, namespace, set) do
          {:ok, result, tail} ->
            continue_parse_aggregate(result, tail, namespace, set, results, parts)

          {:error, _} = err ->
            err
        end
    end
  end

  defp continue_parse_aggregate(result, tail, namespace, set, results, parts) do
    results2 = prepend_results(results, result.results)
    parts2 = prepend_parts(parts, result.partition_done)

    if result.last? do
      finish_scan(results2, parts2, tail)
    else
      do_parse_aggregate(tail, namespace, set, results2, parts2)
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
  defp prepend_records(acc, [record | more]), do: prepend_records([record | acc], more)

  defp prepend_results(acc, []), do: acc
  defp prepend_results(acc, [result | more]), do: prepend_results([result | acc], more)

  defp prepend_parts(acc, nil), do: acc
  defp prepend_parts(acc, []), do: acc
  defp prepend_parts(acc, [part | more]), do: prepend_parts([part | acc], more)

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

  defp process_one_aggregate_frame(meta, rest, _namespace, _default_set) do
    case decode_fields(rest, meta.field_count) do
      {:error, reason} ->
        {:error, Error.from_result_code(:parse_error, message: inspect(reason))}

      {:ok, fields, after_fields} ->
        dispatch_aggregate_frame(meta, fields, after_fields)
    end
  end

  defp dispatch_aggregate_frame(meta, fields, after_fields) do
    last? = meta.last?
    partition_done? = partition_done_flag?(meta)

    cond do
      partition_done? ->
        handle_aggregate_partition_done(meta, fields, after_fields)

      last? and fatal_error_rc?(meta.rc) ->
        skip_ops_then(after_fields, meta.op_count, fn _tail ->
          {:fatal, code} = last_frame_rc(meta.rc)
          {:error, Error.from_result_code(code)}
        end)

      last? ->
        skip_ops_then(after_fields, meta.op_count, fn tail ->
          {:ok, %{results: [], partition_done: nil, last?: true}, tail}
        end)

      stream_terminal_rc?(meta.rc) ->
        skip_ops_then(after_fields, meta.op_count, fn tail ->
          {:ok, %{results: [], partition_done: nil, last?: true}, tail}
        end)

      partition_skip_rc?(meta.rc) ->
        skip_ops_then(after_fields, meta.op_count, fn tail ->
          {:ok, %{results: [], partition_done: nil, last?: false}, tail}
        end)

      true ->
        decode_aggregate_frame(meta, after_fields)
    end
  end

  defp partition_done_flag?(meta),
    do: (meta.info3 &&& @info3_partition_done) != 0

  defp fatal_error_rc?(rc), do: match?({:fatal, _}, last_frame_rc(rc))

  defp handle_partition_done(meta, fields, after_fields, _ns, _default_set) do
    pinfo =
      partition_done_from_fields(meta.generation, fields, partition_unavailable_rc?(meta.rc))

    skip_ops_then(after_fields, meta.op_count, fn tail ->
      {:ok, %{records: [], partition_done: [pinfo], last?: meta.last?}, tail}
    end)
  end

  defp handle_aggregate_partition_done(meta, fields, after_fields) do
    pinfo =
      partition_done_from_fields(meta.generation, fields, partition_unavailable_rc?(meta.rc))

    skip_ops_then(after_fields, meta.op_count, fn tail ->
      {:ok, %{results: [], partition_done: [pinfo], last?: meta.last?}, tail}
    end)
  end

  defp partition_unavailable_rc?(rc_int) do
    match?({:ok, :partition_unavailable}, ResultCode.from_integer(rc_int))
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

      {:error, reason} ->
        {:error, Error.from_result_code(:parse_error, message: inspect(reason))}
    end
  end

  defp decode_aggregate_frame(meta, after_fields) do
    with :ok <- ensure_single_aggregate_op(meta.op_count),
         {:ok, bins, tail} <- decode_aggregate_bins(after_fields, meta.op_count),
         {:ok, result} <- extract_aggregate_result(bins) do
      {:ok, %{results: [result], partition_done: nil, last?: false}, tail}
    else
      {:error, %Error{} = err} -> {:error, err}
      {:error, reason} -> {:error, Error.from_result_code(:parse_error, message: inspect(reason))}
    end
  end

  defp skip_ops_then(data, n, continuation) do
    case skip_operations(data, n) do
      {:ok, tail} -> continuation.(tail)
      {:error, reason} -> {:error, Error.from_result_code(:parse_error, message: inspect(reason))}
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
      {:ok, :ok} -> {:ok, :ok}
      {:ok, :key_not_found} -> {:ok, :ok}
      {:ok, :filtered_out} -> {:ok, :ok}
      {:ok, other} -> {:fatal, other}
      {:error, _} -> {:fatal, :server_error}
    end
  end

  defp stream_terminal_rc?(rc_int) do
    case ResultCode.from_integer(rc_int) do
      {:ok, :ok} -> false
      {:ok, :partition_unavailable} -> false
      {:ok, :key_not_found} -> false
      {:ok, :filtered_out} -> false
      _ -> true
    end
  end

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

  defp skip_fields(data, 0), do: {:ok, data}

  defp skip_fields(<<size::32-big, _::binary-size(size), rest::binary>>, n)
       when n > 0 and size > 0 do
    skip_fields(rest, n - 1)
  end

  defp skip_fields(<<0::32-big, _::binary>>, _n), do: {:error, :invalid_field_size}
  defp skip_fields(_, _), do: {:error, :incomplete_field}

  defp partition_done_from_fields(partition_id, fields, unavailable?) do
    digest =
      case Enum.find(fields, &(&1.type == Field.type_digest())) do
        %Field{data: d} when is_binary(d) and byte_size(d) == 20 -> d
        _ -> nil
      end

    bval = bval_from_fields(fields)
    %{id: partition_id, digest: digest, bval: bval, unavailable?: unavailable?}
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
        base = %Key{namespace: ns, set: set, digest: d}
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
      {:ok, _operation, rest} -> skip_operations(rest, n - 1)
      {:error, _} = err -> err
    end
  end

  defp decode_operations_to_bins(data, 0), do: {:ok, %{}, data}

  defp decode_operations_to_bins(data, n) when n > 0 do
    case Operation.decode(data) do
      {:ok, operation, rest} ->
        with {:ok, value} <- Value.decode_value(operation.particle_type, operation.data),
             {:ok, bins, tail} <- decode_operations_to_bins(rest, n - 1) do
          {:ok, Map.put(bins, operation.bin_name, value), tail}
        end

      {:error, _} = err ->
        err
    end
  end

  defp decode_aggregate_bins(data, 0), do: {:ok, [], data}

  defp decode_aggregate_bins(data, n) when n > 0 do
    case Operation.decode(data) do
      {:ok, operation, rest} ->
        with {:ok, value} <- Value.decode_value(operation.particle_type, operation.data),
             {:ok, bins, tail} <- decode_aggregate_bins(rest, n - 1) do
          {:ok, [decode_aggregate_operation(operation, value) | bins], tail}
        end

      {:error, _} = err ->
        err
    end
  end

  defp decode_aggregate_operation(%Operation{bin_name: "SUCCESS"}, value), do: {:ok, value}
  defp decode_aggregate_operation(%Operation{bin_name: "FAILURE"}, value), do: {:error, value}

  defp decode_aggregate_operation(%Operation{bin_name: other}, _value) do
    {:error,
     Error.from_result_code(:parse_error, message: "expected SUCCESS bin, got #{inspect(other)}")}
  end

  defp ensure_single_aggregate_op(1), do: :ok

  defp ensure_single_aggregate_op(op_count) do
    {:error,
     Error.from_result_code(:parse_error,
       message: "query aggregate expects exactly one operation, got #{op_count}"
     )}
  end

  defp extract_aggregate_result([{:ok, value}]), do: {:ok, value}

  defp extract_aggregate_result([{:error, message}]),
    do: {:error, Error.from_result_code(:query_generic, message: message)}

  defp extract_aggregate_result([]),
    do: {:error, Error.from_result_code(:parse_error, message: "aggregate frame missing result")}

  defp extract_aggregate_result([_ | _]) do
    {:error,
     Error.from_result_code(:parse_error,
       message: "aggregate frame must contain exactly one operation"
     )}
  end

  defp incomplete_response_error do
    {:error,
     Error.from_result_code(:parse_error,
       message: "incomplete scan response: expected LAST frame"
     )}
  end

  defp do_count_records(<<>>, count) do
    {:ok, count, false}
  end

  defp do_count_records(data, count) do
    case read_header(data) do
      {:error, reason} ->
        {:error, Error.from_result_code(:parse_error, message: inspect(reason))}

      {:ok, meta, rest} ->
        with {:ok, after_fields} <- skip_fields(rest, meta.field_count),
             {:ok, next_count, tail, done?} <- count_frame(meta, after_fields, count) do
          continue_count_records(next_count, tail, done?)
        end
    end
  end

  defp count_frame(meta, after_fields, count) do
    cond do
      partition_done_flag?(meta) ->
        count_skipped_frame(after_fields, meta.op_count, count, meta.last?)

      meta.last? and fatal_error_rc?(meta.rc) ->
        {:fatal, code} = last_frame_rc(meta.rc)
        {:error, Error.from_result_code(code)}

      meta.last? ->
        count_skipped_frame(after_fields, meta.op_count, count, true)

      partition_skip_rc?(meta.rc) or stream_terminal_rc?(meta.rc) ->
        count_skipped_frame(after_fields, meta.op_count, count, false)

      true ->
        count_record_frame(after_fields, meta.op_count, count)
    end
  end

  defp continue_count_records(count, tail, true), do: finish_count(count, tail)
  defp continue_count_records(count, tail, false), do: do_count_records(tail, count)

  defp count_skipped_frame(data, op_count, count, done?) do
    case skip_operations(data, op_count) do
      {:ok, tail} -> {:ok, count, tail, done?}
      {:error, reason} -> {:error, parse_error(reason)}
    end
  end

  defp count_record_frame(data, op_count, count) do
    case decode_operations_to_bins(data, op_count) do
      {:ok, _bins, tail} -> {:ok, count + 1, tail, false}
      {:error, reason} -> {:error, parse_error(reason)}
    end
  end

  defp finish_count(count, <<>>), do: {:ok, count, true}

  defp finish_count(_count, _trailing) do
    {:error,
     Error.from_result_code(:parse_error, message: "trailing bytes after LAST scan frame")}
  end

  defp parse_error(reason) do
    Error.from_result_code(:parse_error, message: inspect(reason))
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
