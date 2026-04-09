defmodule Aerospike.BatchOps do
  @moduledoc false

  # Internal batch orchestration: defaults, per-node grouping, encode, pool checkout, parse.

  alias Aerospike.Batch
  alias Aerospike.CircuitBreaker
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.BatchEncoder
  alias Aerospike.Protocol.BatchResponse
  alias Aerospike.Router
  alias Aerospike.Tables

  @typep batch_groups :: %{String.t() => Router.node_batch_group()}

  @spec batch_get(atom(), [Key.t()], keyword()) ::
          {:ok, [Aerospike.Record.t() | nil]} | {:error, Error.t()}
  def batch_get(conn, keys, opts) when is_atom(conn) and is_list(keys) and is_list(opts) do
    merged = Policy.merge_defaults(conn, :batch, opts)

    with_telemetry(:batch_get, conn, keys, fn ->
      case keys do
        [] -> {:ok, []}
        _ -> batch_get_nonempty(conn, keys, merged)
      end
    end)
  end

  defp batch_get_nonempty(conn, keys, merged) do
    with {:ok, groups} <- Router.group_by_node(conn, keys, merged) do
      run_per_node_get(conn, groups, keys, merged)
    end
  end

  @spec batch_exists(atom(), [Key.t()], keyword()) :: {:ok, [boolean()]} | {:error, Error.t()}
  def batch_exists(conn, keys, opts) when is_atom(conn) and is_list(keys) and is_list(opts) do
    merged = Policy.merge_defaults(conn, :batch, opts)

    with_telemetry(:batch_exists, conn, keys, fn ->
      case keys do
        [] -> {:ok, []}
        _ -> batch_exists_nonempty(conn, keys, merged)
      end
    end)
  end

  defp batch_exists_nonempty(conn, keys, merged) do
    n = length(keys)

    with {:ok, groups} <- Router.group_by_node(conn, keys, merged) do
      run_per_node_exists(conn, groups, n, merged)
    end
  end

  @spec batch_operate(atom(), [Batch.t()], keyword()) ::
          {:ok, [Aerospike.BatchResult.t()]} | {:error, Error.t()}
  def batch_operate(conn, ops, opts) when is_atom(conn) and is_list(ops) and is_list(opts) do
    merged = Policy.merge_defaults(conn, :batch, opts)

    with_telemetry(:batch_operate, conn, ops, fn ->
      case ops do
        [] ->
          {:ok, []}

        _ ->
          batch_operate_nonempty(conn, ops, merged)
      end
    end)
  end

  defp batch_operate_nonempty(conn, ops, merged) do
    keys = Enum.map(ops, &Batch.key/1)

    with {:ok, groups} <- Router.group_by_node(conn, keys, merged),
         {:ok, slots} <- run_per_node_operate(conn, groups, ops, merged) do
      finalize_operate_slots(ops, slots)
    end
  end

  defp finalize_operate_slots(ops, slots) do
    filled =
      Enum.zip(ops, slots)
      |> Enum.map(fn {_op, slot} ->
        slot ||
          Aerospike.BatchResult.error(
            Error.from_result_code(:no_response, message: "missing batch slot for operation"),
            false
          )
      end)

    {:ok, filled}
  end

  @spec run_per_node_get(atom(), batch_groups(), [Key.t()], keyword()) ::
          {:ok, [Aerospike.Record.t() | nil]} | {:error, Error.t()}
  defp run_per_node_get(conn, groups, all_keys, merged) do
    merge_slot_results(
      conn,
      groups,
      fn node, %{pool_pid: pool, entries: entries} ->
        wire = BatchEncoder.encode_batch_get(entries, read_opts_for_batch_get(merged))
        timeout = Keyword.get(merged, :pool_checkout_timeout, 5_000)

        with {:ok, body} <- Router.checkout_and_request_stream(pool, wire, timeout) do
          result = BatchResponse.parse_batch_get(body, all_keys)
          maybe_record_device_overload(conn, node, result)
          result
        end
      end,
      length(all_keys),
      nil,
      merged
    )
  end

  defp read_opts_for_batch_get(merged) do
    merged
    |> Keyword.take([
      :timeout,
      :respond_all_keys,
      :pool_checkout_timeout,
      :replica,
      :bins,
      :header_only,
      :read_touch_ttl_percent,
      :filter
    ])
  end

  @spec run_per_node_exists(atom(), batch_groups(), non_neg_integer(), keyword()) ::
          {:ok, [boolean()]} | {:error, Error.t()}
  defp run_per_node_exists(conn, groups, count, merged) do
    merge_slot_results(
      conn,
      groups,
      fn node, %{pool_pid: pool, entries: entries} ->
        wire = BatchEncoder.encode_batch_exists(entries, merged)
        timeout = Keyword.get(merged, :pool_checkout_timeout, 5_000)

        with {:ok, body} <- Router.checkout_and_request_stream(pool, wire, timeout) do
          result = BatchResponse.parse_batch_exists(body, count)
          maybe_record_device_overload(conn, node, result)
          result
        end
      end,
      count,
      false,
      merged
    )
  end

  @spec run_per_node_operate(atom(), batch_groups(), [Batch.t()], keyword()) ::
          {:ok, [Aerospike.BatchResult.t() | nil]} | {:error, Error.t()}
  defp run_per_node_operate(conn, groups, all_ops, merged) do
    n = length(all_ops)
    ops_tuple = List.to_tuple(all_ops)

    merge_slot_results(
      conn,
      groups,
      fn node, %{pool_pid: pool, entries: entries} ->
        indexed_batch =
          Enum.map(entries, fn {idx, _key} -> {idx, elem(ops_tuple, idx)} end)

        wire = BatchEncoder.encode_batch_operate(indexed_batch, merged)
        timeout = Keyword.get(merged, :pool_checkout_timeout, 5_000)

        with {:ok, body} <- Router.checkout_and_request_stream(pool, wire, timeout) do
          result = BatchResponse.parse_batch_operate(body, all_ops)
          maybe_record_device_overload(conn, node, result)
          result
        end
      end,
      n,
      nil,
      merged
    )
  end

  @spec merge_slot_results(
          atom(),
          batch_groups(),
          (String.t(), Router.node_batch_group() ->
             {:ok, list()} | {:error, Error.t()}),
          non_neg_integer(),
          term(),
          keyword()
        ) :: {:ok, list()} | {:error, Error.t()}
  defp merge_slot_results(conn, groups, fun, slot_count, empty_val, merged) do
    timeout_ms = batch_task_await_timeout_ms(merged)
    task_sup = Tables.task_sup(conn)
    tasks = Enum.map(groups, fn {node, group} -> async_group_task(task_sup, fun, node, group) end)
    pairs = Task.yield_many(tasks, timeout_ms)

    with {:ok, results} <- finalize_yield_pairs(pairs) do
      reduce_merged_slots(results, slot_count, empty_val)
    end
  end

  defp reduce_merged_slots(results, slot_count, empty_val) do
    case Enum.find(results, &match?({:error, _}, &1)) do
      {:error, _} = err ->
        err

      nil ->
        merged_slots =
          Enum.reduce(results, List.duplicate(empty_val, slot_count), fn {:ok, partial}, acc ->
            zip_merge_slots(acc, partial, empty_val)
          end)

        {:ok, merged_slots}
    end
  end

  defp batch_task_await_timeout_ms(merged) do
    case Keyword.get(merged, :timeout) do
      t when is_integer(t) and t > 0 -> t
      _ -> 30_000
    end
  end

  defp finalize_yield_pairs(pairs) do
    bad =
      Enum.find(pairs, fn {_task, out} ->
        out == nil or match?({:exit, _}, out)
      end)

    case bad do
      nil ->
        results =
          Enum.map(pairs, fn {_task, {:ok, res}} -> res end)

        {:ok, results}

      {_task, nil} ->
        shutdown_yield_pairs(pairs)
        {:error, Error.from_result_code(:timeout)}

      {_task, {:exit, reason}} ->
        shutdown_yield_pairs(pairs)
        {:error, Error.from_result_code(:network_error, message: inspect(reason))}
    end
  end

  defp shutdown_yield_pairs(pairs) do
    Enum.each(pairs, fn {task, _out} -> Task.shutdown(task, :brutal_kill) end)
  end

  defp async_group_task(task_sup, fun, node, group) do
    Task.Supervisor.async_nolink(task_sup, fn -> normalize_task_result(fun.(node, group)) end)
  end

  defp normalize_task_result({:ok, partial}), do: {:ok, partial}
  defp normalize_task_result({:error, _} = err), do: err

  defp zip_merge_slots(acc, partial, nil) do
    Enum.zip_with(acc, partial, fn a, p -> if p != nil, do: p, else: a end)
  end

  defp zip_merge_slots(acc, partial, false) do
    Enum.zip_with(acc, partial, fn a, p -> a or p end)
  end

  defp with_telemetry(command, conn, keys_or_ops, fun) when is_atom(command) and is_atom(conn) do
    {namespace, set} = telemetry_ns_set(keys_or_ops)
    meta = %{command: command, namespace: namespace, set: set, conn: conn}

    :telemetry.span([:aerospike, :command], meta, fn ->
      case fun.() do
        {:ok, _} = ok ->
          {ok, Map.merge(meta, %{result: :ok, batch_size: batch_size(keys_or_ops)})}

        {:error, %Error{code: code}} = err ->
          {err, Map.merge(meta, %{result: {:error, code}, batch_size: batch_size(keys_or_ops)})}
      end
    end)
  end

  defp batch_size(list) when is_list(list), do: length(list)

  defp telemetry_ns_set([]), do: {"", ""}

  defp telemetry_ns_set([%Key{} = k | _]), do: {k.namespace, k.set}

  defp telemetry_ns_set([op | _]) do
    k = Batch.key(op)
    {k.namespace, k.set}
  end

  defp telemetry_ns_set(_), do: {"", ""}

  defp maybe_record_device_overload(conn, node_name, {:error, %Error{code: :device_overload}}) do
    CircuitBreaker.record_error(conn, node_name, :device_overload)
  end

  defp maybe_record_device_overload(_conn, _node_name, _result), do: :ok
end
