defmodule Aerospike.Protocol.BatchResponse do
  @dialyzer :no_match

  @moduledoc false

  import Bitwise

  alias Aerospike.Batch
  alias Aerospike.BatchResult
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.ResultCode
  alias Aerospike.Record

  @doc false
  @spec parse_batch_get(binary(), list(Key.t())) ::
          {:ok, list(Record.t() | nil)} | {:error, Error.t()}
  def parse_batch_get(body, []) when is_binary(body), do: {:ok, []}

  def parse_batch_get(body, keys) when is_binary(body) and is_list(keys) do
    n = length(keys)
    keys_t = List.to_tuple(keys)
    do_parse_get(body, keys_t, n, %{})
  end

  defp do_parse_get(<<>>, _keys_t, n, slots), do: {:ok, slots_to_list(n, slots, nil)}

  defp do_parse_get(data, keys_t, n, slots) do
    case read_header(data) do
      {:last, _} -> {:ok, slots_to_list(n, slots, nil)}
      {:error, reason} -> {:error, Error.from_result_code(:parse_error, message: inspect(reason))}
      {:ok, meta, rest} -> get_after_header(meta, rest, keys_t, n, slots)
    end
  end

  defp slots_to_list(n, slots, default) do
    for i <- 0..(n - 1), do: Map.get(slots, i, default)
  end

  defp get_after_header(meta, rest, keys_t, n, slots) do
    case batch_read_rc(meta.rc) do
      {:ok, :filtered_out} -> get_continue_after_advance(rest, meta, keys_t, n, slots, false)
      {:error, code} -> {:error, Error.from_result_code(code)}
      {:ok, :ok} -> get_found_record(rest, meta, keys_t, n, slots)
      {:ok, :key_not_found} -> get_missing_record(rest, meta, keys_t, n, slots)
    end
  end

  defp get_continue_after_advance(rest, meta, keys_t, n, slots, decode_bins?) do
    case advance_record(rest, meta, decode_bins?) do
      {:ok, {_, tail}} -> do_parse_get(tail, keys_t, n, slots)
      {:error, _} = err -> err
    end
  end

  defp get_found_record(rest, meta, keys_t, n, slots) do
    case record_at(meta.bidx, keys_t, n) do
      {:ok, key, _} ->
        case advance_record(rest, meta, true) do
          {:ok, {bins, tail}} ->
            rec = %Record{key: key, bins: bins, generation: meta.gen, ttl: meta.exp}
            slots2 = Map.put(slots, meta.bidx, rec)
            do_parse_get(tail, keys_t, n, slots2)

          {:error, _} = err ->
            err
        end

      {:error, _} = err ->
        err
    end
  end

  defp get_missing_record(rest, meta, keys_t, n, slots) do
    case record_at(meta.bidx, keys_t, n) do
      {:ok, _, _} ->
        case advance_record(rest, meta, false) do
          {:ok, {_, tail}} ->
            do_parse_get(tail, keys_t, n, Map.put(slots, meta.bidx, nil))

          {:error, _} = err ->
            err
        end

      {:error, _} = err ->
        err
    end
  end

  defp advance_record(rest, meta, decode_bins?) do
    with {:ok, after_fields} <- skip_fields(rest, meta.field_count) do
      advance_after_fields(after_fields, meta, decode_bins?)
    end
  end

  defp advance_after_fields(data, %{rc: 0} = meta, true) do
    case decode_operations_to_bins(data, meta.op_count) do
      {:ok, bins, tail} -> {:ok, {bins, tail}}
      {:error, _} = err -> err
    end
  end

  defp advance_after_fields(data, meta, _) do
    case skip_operations(data, meta.op_count) do
      {:ok, tail} -> {:ok, {%{}, tail}}
      {:error, _} = err -> err
    end
  end

  defp read_header(
         <<22::8, _i1::8, _i2::8, i3::8, _i4::8, rc::8, gen::32-big, exp::32-big, bidx::32-big,
           fc::16-big, oc::16-big, rest::binary>>
       ) do
    if (i3 &&& AsmMsg.info3_last()) != 0 do
      {:last, rest}
    else
      {:ok, %{info3: i3, rc: rc, gen: gen, exp: exp, bidx: bidx, field_count: fc, op_count: oc},
       rest}
    end
  end

  defp read_header(_), do: {:error, :short_header}

  defp batch_read_rc(rc_int) do
    case ResultCode.from_integer(rc_int) do
      {:ok, :ok} -> {:ok, :ok}
      {:ok, :key_not_found} -> {:ok, :key_not_found}
      {:ok, :filtered_out} -> {:ok, :filtered_out}
      {:ok, other} -> {:error, other}
      {:error, _} -> {:error, :server_error}
    end
  end

  defp record_at(bidx, keys_t, n_keys) do
    if bidx >= 0 and bidx < n_keys do
      {:ok, elem(keys_t, bidx), nil}
    else
      {:error, Error.from_result_code(:parse_error, message: "invalid batch index #{bidx}")}
    end
  end

  defp skip_fields(data, 0), do: {:ok, data}

  defp skip_fields(data, n) when n > 0 do
    case Field.decode(data) do
      {:ok, _f, rest} -> skip_fields(rest, n - 1)
      {:error, _} = err -> err
    end
  end

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
  @spec parse_batch_exists(binary(), non_neg_integer()) ::
          {:ok, [boolean()]} | {:error, Error.t()}
  def parse_batch_exists(body, 0) when is_binary(body), do: {:ok, []}

  def parse_batch_exists(body, count) when is_binary(body) and count >= 0 do
    do_parse_exists(body, %{}, count)
  end

  defp do_parse_exists(<<>>, slots, count), do: {:ok, slots_to_list(count, slots, false)}

  defp do_parse_exists(data, slots, count) do
    case read_header(data) do
      {:last, _} -> {:ok, slots_to_list(count, slots, false)}
      {:error, reason} -> {:error, Error.from_result_code(:parse_error, message: inspect(reason))}
      {:ok, meta, rest} -> exists_after_header(meta, rest, slots, count)
    end
  end

  defp exists_after_header(meta, rest, slots, count) do
    exists? = meta.rc == 0

    slots2 =
      if meta.bidx >= 0 and meta.bidx < count do
        Map.put(slots, meta.bidx, exists?)
      else
        slots
      end

    case batch_read_rc(meta.rc) do
      {:error, code} -> {:error, Error.from_result_code(code)}
      {:ok, _} -> exists_continue(rest, meta, slots2, count)
    end
  end

  defp exists_continue(rest, meta, slots2, count) do
    case advance_record(rest, meta, false) do
      {:ok, {_, tail}} -> do_parse_exists(tail, slots2, count)
      {:error, _} = err -> err
    end
  end

  @doc false
  @spec parse_batch_operate(binary(), list(Batch.t())) ::
          {:ok, list(BatchResult.t() | nil)} | {:error, Error.t()}
  def parse_batch_operate(body, []) when is_binary(body), do: {:ok, []}

  def parse_batch_operate(body, batch_ops) when is_binary(body) and is_list(batch_ops) do
    n = length(batch_ops)
    ops_t = List.to_tuple(batch_ops)
    do_parse_operate(body, ops_t, n, %{})
  end

  defp do_parse_operate(<<>>, _ops_t, n, slots), do: {:ok, slots_to_list(n, slots, nil)}

  defp do_parse_operate(data, ops_t, n, slots) do
    case read_header(data) do
      {:last, _} -> {:ok, slots_to_list(n, slots, nil)}
      {:error, reason} -> {:error, Error.from_result_code(:parse_error, message: inspect(reason))}
      {:ok, meta, rest} -> operate_one_entry(ops_t, n, rest, meta, slots)
    end
  end

  defp operate_one_entry(ops_t, n, rest, meta, slots) do
    case handle_operate_entry(ops_t, n, rest, meta, slots) do
      {:ok, tail, slots2} -> do_parse_operate(tail, ops_t, n, slots2)
      {:error, _} = err -> err
    end
  end

  defp handle_operate_entry(ops_t, n, rest, meta, slots) do
    if meta.bidx < 0 or meta.bidx >= n do
      {:error, Error.from_result_code(:parse_error, message: "invalid batch index #{meta.bidx}")}
    else
      op = elem(ops_t, meta.bidx)
      key = Batch.key(op)

      with {:ok, after_fields} <- skip_fields(rest, meta.field_count),
           {:ok, bins, tail} <- decode_or_skip_ops(after_fields, meta.op_count, meta.rc) do
        br = batch_result_for_op(op, key, meta, bins)
        {:ok, tail, Map.put(slots, meta.bidx, br)}
      end
    end
  end

  defp decode_or_skip_ops(data, oc, rc) when rc == 0, do: decode_operations_to_bins(data, oc)
  defp decode_or_skip_ops(data, oc, _rc), do: skip_ops_as_bins(data, oc)

  defp skip_ops_as_bins(data, oc) do
    case skip_operations(data, oc) do
      {:ok, tail} -> {:ok, %{}, tail}
      {:error, _} = err -> err
    end
  end

  # Successful writes/deletes typically carry no bin payload — API uses `record: nil` for those.
  # Reads use `nil` record when the key is missing (same idea as `batch_get/3`).
  defp batch_result_for_op(%Batch.Read{}, key, meta, bins) do
    case ResultCode.from_integer(meta.rc) do
      {:ok, :ok} ->
        BatchResult.ok(%Record{key: key, bins: bins, generation: meta.gen, ttl: meta.exp})

      {:ok, :key_not_found} ->
        BatchResult.ok(nil)

      {:ok, :bin_not_found} ->
        BatchResult.ok(nil)

      {:ok, other} ->
        BatchResult.error(Error.from_result_code(other), false)

      {:error, _} ->
        BatchResult.error(Error.from_result_code(:server_error), false)
    end
  end

  defp batch_result_for_op(%Batch.Put{}, _key, meta, _bins) do
    case ResultCode.from_integer(meta.rc) do
      {:ok, :ok} -> BatchResult.ok(nil)
      {:ok, other} -> BatchResult.error(Error.from_result_code(other), false)
      {:error, _} -> BatchResult.error(Error.from_result_code(:server_error), false)
    end
  end

  defp batch_result_for_op(%Batch.Delete{}, _key, meta, _bins) do
    case ResultCode.from_integer(meta.rc) do
      {:ok, :ok} -> BatchResult.ok(nil)
      {:ok, :key_not_found} -> BatchResult.ok(nil)
      {:ok, other} -> BatchResult.error(Error.from_result_code(other), false)
      {:error, _} -> BatchResult.error(Error.from_result_code(:server_error), false)
    end
  end

  defp batch_result_for_op(%Batch.UDF{}, key, meta, bins) do
    case ResultCode.from_integer(meta.rc) do
      {:ok, :ok} ->
        if bins == %{} do
          BatchResult.ok(nil)
        else
          BatchResult.ok(%Record{key: key, bins: bins, generation: meta.gen, ttl: meta.exp})
        end

      {:ok, other} ->
        BatchResult.error(Error.from_result_code(other), false)

      {:error, _} ->
        BatchResult.error(Error.from_result_code(:server_error), false)
    end
  end

  defp batch_result_for_op(%Batch.Operate{}, key, meta, bins) do
    case ResultCode.from_integer(meta.rc) do
      {:ok, :ok} ->
        BatchResult.ok(%Record{key: key, bins: bins, generation: meta.gen, ttl: meta.exp})

      {:ok, other} ->
        BatchResult.error(Error.from_result_code(other), false)

      {:error, _} ->
        BatchResult.error(Error.from_result_code(:server_error), false)
    end
  end
end
