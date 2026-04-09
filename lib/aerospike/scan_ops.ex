defmodule Aerospike.ScanOps do
  @moduledoc false

  # Scan/query orchestration: partition routing, per-node wire build, fan-out, streaming.

  alias Aerospike.CircuitBreaker
  alias Aerospike.Connection
  alias Aerospike.Cursor
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Page
  alias Aerospike.PartitionFilter
  alias Aerospike.Policy
  alias Aerospike.Protocol.ScanQuery
  alias Aerospike.Protocol.ScanResponse
  alias Aerospike.Query
  alias Aerospike.Record
  alias Aerospike.Router
  alias Aerospike.Scan
  alias Aerospike.Tables

  @count_per_node_cap 2_147_483_647
  @max_all_iterations 20

  @typep partition_groups :: %{String.t() => Router.node_partition_group()}

  @typep fanout_prepared :: %{
           groups: partition_groups(),
           node_wires: [{String.t(), pid(), iodata(), ScanQuery.node_partitions()}],
           replica_index: non_neg_integer()
         }

  @doc false
  @spec distribute_record_max(pos_integer(), pos_integer()) :: [pos_integer()]
  def distribute_record_max(total, num_nodes)
      when is_integer(total) and total > 0 and is_integer(num_nodes) and num_nodes > 0 do
    base = div(total, num_nodes)
    remainder = rem(total, num_nodes)
    [base + remainder | List.duplicate(base, num_nodes - 1)]
  end

  defp all_after_max_ok(conn_name, scannable, opts) do
    merged = Policy.merge_defaults(conn_name, policy_kind(scannable), opts)
    max_records = scannable_max_for_distribute(scannable)
    cmd = scannable_command(:all, scannable)

    with_scan_telemetry(cmd, conn_name, scannable, fn ->
      iterate_all(conn_name, scannable, merged, max_records, [], 0)
    end)
  end

  defp iterate_all(_conn_name, _scannable, _opts, max_records, acc, _iteration)
       when length(acc) >= max_records do
    {:ok, Enum.take(acc, max_records)}
  end

  defp iterate_all(_conn_name, _scannable, _opts, _max_records, acc, iteration)
       when iteration >= @max_all_iterations do
    {:ok, acc}
  end

  defp iterate_all(conn_name, scannable, opts, max_records, acc, iteration) do
    remaining = max_records - length(acc)
    scannable_with_budget = set_max_records(scannable, remaining)

    with {:ok, prepared} <-
           prepare_fanout(conn_name, scannable_with_budget, opts, &distribute_record_max/2),
         {:ok, page} <- run_fanout_page(conn_name, prepared, scannable_with_budget, opts) do
      apply_page_result(conn_name, scannable, opts, max_records, acc, iteration, page)
    end
  end

  defp apply_page_result(_conn_name, _scannable, _opts, max_records, acc, _iteration, %Page{
         records: new_records,
         done?: true
       }) do
    {:ok, Enum.take(acc ++ new_records, max_records)}
  end

  defp apply_page_result(_conn_name, _scannable, _opts, max_records, acc, _iteration, %Page{
         records: new_records
       })
       when length(acc) + length(new_records) >= max_records do
    {:ok, Enum.take(acc ++ new_records, max_records)}
  end

  defp apply_page_result(_conn_name, _scannable, _opts, _max_records, acc, _iteration, %Page{
         records: []
       }) do
    {:ok, acc}
  end

  defp apply_page_result(conn_name, scannable, opts, max_records, acc, iteration, %Page{
         records: new_records,
         cursor: cursor
       }) do
    scannable_with_cursor = attach_cursor_partition_filter(scannable, cursor)

    iterate_all(
      conn_name,
      scannable_with_cursor,
      opts,
      max_records,
      acc ++ new_records,
      iteration + 1
    )
  end

  defp page_after_max_ok(conn_name, scannable2, opts2) do
    merged = Policy.merge_defaults(conn_name, policy_kind(scannable2), opts2)

    case prepare_fanout(conn_name, scannable2, merged, &distribute_record_max/2) do
      {:ok, prepared} ->
        cmd = scannable_command(:page, scannable2)

        with_scan_telemetry(cmd, conn_name, scannable2, fn ->
          run_fanout_page(conn_name, prepared, scannable2, merged)
        end)

      {:error, _} = err ->
        err
    end
  end

  @spec all(atom(), Scan.t() | Query.t(), keyword()) ::
          {:ok, [Record.t()]} | {:error, Error.t()}
  def all(conn_name, scannable, opts \\ [])
      when is_atom(conn_name) and is_list(opts) do
    with :ok <- require_max_records(scannable) do
      all_after_max_ok(conn_name, scannable, opts)
    end
  end

  @spec count(atom(), Scan.t() | Query.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def count(conn_name, scannable, opts \\ [])
      when is_atom(conn_name) and is_list(opts) do
    scannable2 = force_no_bins(scannable)
    merged = Policy.merge_defaults(conn_name, policy_kind(scannable), opts)

    case prepare_fanout(conn_name, scannable2, merged, fn _total, n ->
           List.duplicate(@count_per_node_cap, n)
         end) do
      {:ok, prepared} ->
        cmd = scannable_command(:count, scannable)

        with_scan_telemetry(cmd, conn_name, scannable, fn ->
          run_fanout_count(conn_name, prepared, scannable2, merged)
        end)

      {:error, _} = err ->
        err
    end
  end

  @spec page(atom(), Scan.t() | Query.t(), keyword()) :: {:ok, Page.t()} | {:error, Error.t()}
  def page(conn_name, scannable, opts \\ [])
      when is_atom(conn_name) and is_list(opts) do
    {cursor, opts2} = Keyword.pop(opts, :cursor)

    with {:ok, scannable2} <- apply_optional_cursor(scannable, cursor),
         :ok <- require_max_records(scannable2) do
      page_after_max_ok(conn_name, scannable2, opts2)
    end
  end

  @spec stream(atom(), Scan.t() | Query.t(), keyword()) :: Enumerable.t()
  def stream(conn_name, scannable, opts \\ [])
      when is_atom(conn_name) and is_list(opts) do
    merged = Policy.merge_defaults(conn_name, policy_kind(scannable), opts)

    Stream.resource(
      fn -> init_stream_state(conn_name, scannable, merged) end,
      &next_stream/1,
      &cleanup_stream/1
    )
  end

  defp init_stream_state(conn_name, scannable, opts) do
    distribute_fn = fn total, n ->
      case total do
        t when is_integer(t) and t > 0 -> distribute_record_max(t, n)
        _ -> List.duplicate(0, n)
      end
    end

    {ns, set} = namespace_set(scannable)

    case prepare_fanout(conn_name, scannable, opts, distribute_fn) do
      {:ok, prepared} ->
        parent = self()
        checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, 5_000)
        meta = stream_telemetry_meta(conn_name, scannable, ns, set)
        t0 = :erlang.monotonic_time()

        :telemetry.execute(
          [:aerospike, :command, :start],
          %{monotonic_time: t0},
          meta
        )

        producer =
          spawn_link(fn ->
            run_stream_producer(conn_name, prepared.node_wires, parent, ns, set, checkout_timeout)
          end)

        monitor_ref = Process.monitor(producer)

        %{
          producer: producer,
          monitor_ref: monitor_ref,
          done: false,
          telemetry: %{t0: t0, meta: meta}
        }

      {:error, %Error{} = err} ->
        t0 = :erlang.monotonic_time()
        meta = stream_telemetry_meta(conn_name, scannable, ns, set)

        :telemetry.execute(
          [:aerospike, :command, :start],
          %{monotonic_time: t0},
          meta
        )

        %{failed: err, telemetry: %{t0: t0, meta: meta}}
    end
  end

  defp next_stream(%{failed: %Error{} = e, telemetry: %{t0: t0, meta: meta}}) do
    duration = :erlang.monotonic_time() - t0

    :telemetry.execute(
      [:aerospike, :command, :exception],
      %{duration: duration, monotonic_time: :erlang.monotonic_time()},
      Map.put(meta, :kind, :error)
    )

    raise e
  end

  defp next_stream(%{failed: %Error{} = e}), do: raise(e)

  defp next_stream(%{done: true} = state), do: {:halt, state}

  defp next_stream(state) do
    receive do
      {:EXIT, pid, reason} when pid == state.producer ->
        stream_producer_terminated(reason, state)

      {:DOWN, ref, :process, _pid, reason} when ref == state.monitor_ref ->
        stream_producer_terminated(reason, state)

      {:scan_records, _pid, recs} when recs != [] ->
        {recs, state}

      {:scan_records, _pid, _empty} ->
        next_stream(state)

      {:scan_done, _pid} ->
        {:halt, %{state | done: true}}

      {:scan_error, _pid, %Error{} = e} ->
        raise e

      {:scan_error, _pid, reason} ->
        raise Error.from_result_code(:network_error, message: inspect(reason))
    end
  end

  defp stream_producer_terminated(reason, state) do
    case reason do
      :normal ->
        {:halt, state}

      :shutdown ->
        {:halt, state}

      _ ->
        raise Error.from_result_code(:network_error,
                message: "stream producer crashed: #{inspect(reason)}"
              )
    end
  end

  defp cleanup_stream(state) do
    maybe_emit_stream_stop(state)
    cleanup_stream_producer(state)
  end

  defp maybe_emit_stream_stop(%{failed: _}), do: :ok

  defp maybe_emit_stream_stop(%{telemetry: %{t0: t0, meta: meta}}) do
    duration = :erlang.monotonic_time() - t0

    :telemetry.execute(
      [:aerospike, :command, :stop],
      %{duration: duration, monotonic_time: :erlang.monotonic_time()},
      meta
    )
  end

  defp maybe_emit_stream_stop(_), do: :ok

  defp cleanup_stream_producer(%{producer: pid} = state) when is_pid(pid) do
    case Map.get(state, :monitor_ref) do
      ref when is_reference(ref) -> Process.demonitor(ref, [:flush])
      _ -> :ok
    end

    Process.unlink(pid)
    if Process.alive?(pid), do: Process.exit(pid, :shutdown)

    receive do
      {:EXIT, ^pid, _} -> :ok
    after
      0 -> :ok
    end
  end

  defp cleanup_stream_producer(_), do: :ok

  # Nodes are processed sequentially; concurrent fan-out is a future optimization.
  defp run_stream_producer(_conn_name, [], parent, _ns, _set, _checkout_timeout) do
    send(parent, {:scan_done, self()})
  end

  defp run_stream_producer(
         conn_name,
         [{node_name, pool_pid, wire, _meta} | rest],
         parent,
         ns,
         set,
         checkout_timeout
       ) do
    result =
      try do
        NimblePool.checkout!(
          pool_pid,
          :checkout,
          fn _from, conn ->
            case Connection.send_command(conn, wire) do
              {:ok, conn2} ->
                read_stream_frames(conn2, conn_name, node_name, parent, ns, set)

              {:error, reason} ->
                send(parent, {:scan_error, self(), reason})
                {{:error, reason}, :close}
            end
          end,
          checkout_timeout
        )
      catch
        :exit, {:timeout, {NimblePool, :checkout, _}} ->
          send(parent, {:scan_error, self(), Error.from_result_code(:pool_timeout)})
          :error_sent

        :exit, {:noproc, {NimblePool, :checkout, _}} ->
          send(parent, {:scan_error, self(), Error.from_result_code(:invalid_node)})
          :error_sent

        :exit, reason ->
          send(
            parent,
            {:scan_error, self(),
             Error.from_result_code(:network_error, message: inspect(reason))}
          )

          :error_sent
      end

    case result do
      :error_sent -> :ok
      {:error, _} -> :ok
      _ -> run_stream_producer(conn_name, rest, parent, ns, set, checkout_timeout)
    end
  end

  defp read_stream_frames(conn, conn_name, node_name, parent, ns, set) do
    case Connection.recv_frame(conn) do
      {:ok, conn2, body, proto_last?} ->
        dispatch_parsed_stream_frame(
          conn2,
          body,
          conn_name,
          node_name,
          parent,
          ns,
          set,
          proto_last?
        )

      {:error, reason} ->
        send(parent, {:scan_error, self(), reason})
        {{:error, reason}, :close}
    end
  end

  defp dispatch_parsed_stream_frame(
         conn2,
         body,
         conn_name,
         node_name,
         parent,
         ns,
         set,
         proto_last?
       ) do
    case ScanResponse.parse_stream_chunk(body, ns, set) do
      {:ok, rs, _parts, stream_done?} ->
        if rs != [], do: send(parent, {:scan_records, self(), rs})

        if stream_done? or proto_last? do
          {{:ok, :done}, conn2}
        else
          read_stream_frames(conn2, conn_name, node_name, parent, ns, set)
        end

      {:error, %Error{} = e} ->
        maybe_record_device_overload(conn_name, node_name, {:error, e})
        send(parent, {:scan_error, self(), e})
        {{:error, e}, :close}
    end
  end

  @spec prepare_fanout(
          atom(),
          Scan.t() | Query.t(),
          keyword(),
          (term(), pos_integer() -> [pos_integer()])
        ) :: {:ok, fanout_prepared()} | {:error, Error.t()}
  defp prepare_fanout(conn_name, scannable, opts, distribute_fn) do
    replica_index = replica_index_from_opts(opts)
    {full_ids, partials} = Router.expand_partition_filter(scannable.partition_filter)
    set = scannable_set(scannable)

    with {:ok, groups} <-
           Router.group_partitions_by_node(
             conn_name,
             scannable.namespace,
             full_ids,
             replica_index
           ),
         {:ok, groups2} <-
           merge_partial_entries(
             conn_name,
             scannable.namespace,
             set,
             groups,
             partials,
             replica_index
           ),
         :ok <- ensure_has_node_groups(groups2) do
      sorted = Enum.sort_by(groups2, &elem(&1, 0))
      n = length(sorted)
      max_vals = distribute_fn.(scannable_max_for_distribute(scannable), n)

      node_wires =
        sorted
        |> Enum.zip(max_vals)
        |> Enum.map(fn {{node_name, group}, node_max} ->
          page_meta = %{
            parts_full: group.parts_full,
            parts_partial: group.parts_partial,
            record_max: node_max
          }

          wire = build_wire(scannable, page_meta, opts)
          {node_name, group.pool_pid, wire, page_meta}
        end)

      {:ok, %{groups: groups2, node_wires: node_wires, replica_index: replica_index}}
    end
  end

  defp scannable_max_for_distribute(%Scan{max_records: m}), do: m
  defp scannable_max_for_distribute(%Query{max_records: m}), do: m

  defp scannable_set(%Scan{set: s}), do: s || ""
  defp scannable_set(%Query{set: s}), do: s

  @spec ensure_has_node_groups(partition_groups()) :: :ok | {:error, Error.t()}
  defp ensure_has_node_groups(groups) do
    if map_size(groups) == 0 do
      {:error, Error.from_result_code(:invalid_cluster_partition_map)}
    else
      :ok
    end
  end

  @spec merge_partial_entries(
          atom(),
          String.t(),
          String.t(),
          partition_groups(),
          [PartitionFilter.partition_entry()],
          non_neg_integer()
        ) :: {:ok, partition_groups()} | {:error, Error.t()}
  defp merge_partial_entries(conn_name, namespace, set, groups, partials, replica_index) do
    with {:ok, groups2} <-
           Enum.reduce_while(partials, {:ok, groups}, fn entry, {:ok, acc} ->
             merge_one_partial(conn_name, namespace, set, acc, entry, replica_index)
           end) do
      {:ok,
       Map.new(groups2, fn {k, g} -> {k, %{g | parts_partial: Enum.reverse(g.parts_partial)}} end)}
    end
  end

  defp merge_one_partial(conn_name, namespace, set, acc, entry, replica_index) do
    with {:ok, part_id} <- resolve_partial_partition_id(entry, namespace, set),
         {:ok, pool_pid, node_name} <-
           Router.resolve_pool_for_partition(conn_name, namespace, part_id, replica_index),
         {:ok, acc2} <-
           insert_partial_into_group(acc, node_name, pool_pid, Map.put(entry, :id, part_id)) do
      {:cont, {:ok, acc2}}
    else
      {:error, _} = err -> {:halt, err}
    end
  end

  defp resolve_partial_partition_id(entry, namespace, set) do
    id = Map.get(entry, :id)

    cond do
      is_integer(id) and id >= 0 ->
        {:ok, id}

      match?(%{digest: d} when is_binary(d) and byte_size(d) == 20, entry) ->
        d = Map.fetch!(entry, :digest)
        {:ok, Key.partition_id(Key.from_digest(namespace, set, d))}

      true ->
        {:error,
         Error.from_result_code(:parse_error,
           message: "partition entry needs a non-negative :id or a 20-byte :digest"
         )}
    end
  end

  defp insert_partial_into_group(acc, node_name, pool_pid, entry) do
    case Map.get(acc, node_name) do
      nil ->
        {:ok,
         Map.put(acc, node_name, %{
           pool_pid: pool_pid,
           parts_full: [],
           parts_partial: [entry]
         })}

      %{pool_pid: ^pool_pid} = g ->
        {:ok, Map.put(acc, node_name, %{g | parts_partial: [entry | g.parts_partial]})}

      %{pool_pid: other} ->
        {:error,
         Error.from_result_code(:invalid_node,
           message:
             "inconsistent pool_pid for node #{inspect(node_name)}: #{inspect(other)} vs #{inspect(pool_pid)}"
         )}
    end
  end

  defp run_fanout_count(conn_name, %{node_wires: wires}, _scannable, opts) do
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, 5_000)

    collect_and_merge(
      conn_name,
      wires,
      fn node_name, pool_pid, wire ->
        with {:ok, body} <- Router.checkout_and_request_stream(pool_pid, wire, checkout_timeout) do
          result = ScanResponse.count_records(body)
          maybe_record_device_overload(conn_name, node_name, result)
          result
        end
      end,
      fn counts -> {:ok, Enum.sum(counts)} end,
      opts
    )
  end

  defp run_fanout_page(conn_name, prepared, scannable, opts) do
    {ns, set} = namespace_set(scannable)
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, 5_000)
    timeout_ms = scan_task_await_timeout_ms(opts)
    task_sup = Tables.task_sup(conn_name)

    node_jobs = page_jobs_from_node_wires(prepared.node_wires)
    prior_partials = prior_partial_digests(scannable)

    run_fn = fn job ->
      with {:ok, body} <-
             Router.checkout_and_request_stream(job.pool_pid, job.wire, checkout_timeout),
           {:ok, records, _parts} <- ScanResponse.parse(body, ns, set) do
        {:ok, records}
      else
        {:error, %Error{} = e} = err ->
          maybe_record_device_overload(conn_name, job.node_name, err)
          {:error, e}
      end
    end

    tasks =
      Enum.map(node_jobs, fn job ->
        Task.Supervisor.async_nolink(task_sup, fn -> run_fn.(job) end)
      end)

    pairs = Task.yield_many(tasks, timeout_ms)

    with {:ok, per_node_results} <- finalize_yield_pairs(pairs) do
      finalize_page_results(node_jobs, per_node_results, prior_partials)
    end
  end

  defp page_jobs_from_node_wires(node_wires) do
    Enum.map(node_wires, fn {node_name, pool_pid, wire, page_meta} ->
      page_meta
      |> Map.put(:node_name, node_name)
      |> Map.put(:pool_pid, pool_pid)
      |> Map.put(:wire, wire)
    end)
  end

  defp prior_partial_digests(scannable) do
    pf = scannable.partition_filter

    case pf do
      %PartitionFilter{partitions: parts} when is_list(parts) and parts != [] ->
        Map.new(
          Enum.filter(parts, fn entry -> Map.get(entry, :digest) != nil end),
          fn %{id: id} = e -> {id, {Map.get(e, :digest), Map.get(e, :bval)}} end
        )

      _ ->
        %{}
    end
  end

  defp finalize_page_results(node_jobs, per_node_results, prior_partials) do
    paired = Enum.zip(node_jobs, per_node_results)

    case Enum.find(paired, fn {_job, result} -> match?({:error, _}, result) end) do
      {_job, {:error, _} = err} ->
        err

      nil ->
        all_records =
          Enum.flat_map(paired, fn {_job, {:ok, recs}} -> recs end)

        any_node_hit_max? =
          Enum.any?(paired, fn {job, {:ok, recs}} ->
            length(recs) >= job.record_max
          end)

        if any_node_hit_max? do
          cursor = build_page_cursor_from_records(paired, prior_partials)
          {:ok, %Page{records: all_records, cursor: cursor, done?: false}}
        else
          {:ok, %Page{records: all_records, cursor: nil, done?: true}}
        end
    end
  end

  defp build_page_cursor_from_records(paired, prior_partials) do
    parts =
      paired
      |> Enum.filter(fn {job, {:ok, recs}} -> length(recs) >= job.record_max end)
      |> Enum.flat_map(fn {job, {:ok, recs}} ->
        new_digests = last_digest_per_partition(recs)
        all_part_ids = node_partition_ids(job)
        Enum.map(all_part_ids, &partition_cursor_entry(&1, new_digests, prior_partials))
      end)

    %Cursor{partitions: parts}
  end

  defp partition_cursor_entry(pid, new_digests, prior_partials) do
    case Map.get(new_digests, pid) || Map.get(prior_partials, pid) do
      {digest, bval} -> %{id: pid, digest: digest, bval: bval}
      nil -> %{id: pid}
    end
  end

  defp last_digest_per_partition(records) do
    Enum.reduce(records, %{}, fn %Record{key: key} = _rec, acc ->
      pid = Key.partition_id(key)
      Map.put(acc, pid, {key.digest, nil})
    end)
  end

  defp node_partition_ids(%{parts_full: full, parts_partial: partial}) do
    full_ids = full
    partial_ids = Enum.map(partial, fn %{id: id} -> id end)
    full_ids ++ partial_ids
  end

  defp collect_and_merge(conn_name, wires, node_fun, finalize, merged_opts) do
    timeout_ms = scan_task_await_timeout_ms(merged_opts)
    task_sup = Tables.task_sup(conn_name)

    tasks =
      Enum.map(wires, fn {node_name, pool_pid, wire, _page_meta} ->
        Task.Supervisor.async_nolink(task_sup, fn ->
          node_fun.(node_name, pool_pid, wire)
        end)
      end)

    pairs = Task.yield_many(tasks, timeout_ms)

    with {:ok, results} <- finalize_yield_pairs(pairs),
         :ok <- check_no_errors(results) do
      values = Enum.map(results, fn {:ok, v} -> v end)
      finalize.(values)
    end
  end

  defp check_no_errors(results) do
    case Enum.find(results, &match?({:error, _}, &1)) do
      {:error, _} = err -> err
      nil -> :ok
    end
  end

  defp finalize_yield_pairs(pairs) do
    bad =
      Enum.find(pairs, fn {_task, out} ->
        out == nil or match?({:exit, _}, out)
      end)

    case bad do
      nil ->
        results = Enum.map(pairs, fn {_task, {:ok, res}} -> res end)
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

  defp scan_task_await_timeout_ms(opts) do
    case Keyword.get(opts, :timeout) do
      t when is_integer(t) and t > 0 -> t
      _ -> 30_000
    end
  end

  defp apply_optional_cursor(scannable, nil), do: {:ok, scannable}

  defp apply_optional_cursor(scannable, %Cursor{} = c),
    do: {:ok, attach_cursor_partition_filter(scannable, c)}

  defp apply_optional_cursor(scannable, bin) when is_binary(bin) do
    case Cursor.decode(bin) do
      {:ok, c} -> {:ok, attach_cursor_partition_filter(scannable, c)}
      {:error, _} = err -> err
    end
  end

  defp apply_optional_cursor(_scannable, other) do
    {:error,
     Error.from_result_code(:parameter_error, message: "invalid cursor: #{inspect(other)}")}
  end

  defp attach_cursor_partition_filter(%Scan{} = s, %Cursor{partitions: p}) do
    n = PartitionFilter.partition_count()

    %{s | partition_filter: %PartitionFilter{begin: 0, count: n, partitions: p}}
  end

  defp attach_cursor_partition_filter(%Query{} = q, %Cursor{partitions: p}) do
    n = PartitionFilter.partition_count()

    %{q | partition_filter: %PartitionFilter{begin: 0, count: n, partitions: p}}
  end

  defp set_max_records(%Scan{} = s, n) when is_integer(n) and n > 0, do: %{s | max_records: n}
  defp set_max_records(%Query{} = q, n) when is_integer(n) and n > 0, do: %{q | max_records: n}

  defp require_max_records(%Scan{max_records: n}) when is_integer(n) and n > 0, do: :ok
  defp require_max_records(%Query{max_records: n}) when is_integer(n) and n > 0, do: :ok
  defp require_max_records(_), do: {:error, Error.from_result_code(:max_records_required)}

  defp force_no_bins(%Scan{} = s), do: %{s | no_bins: true}
  defp force_no_bins(%Query{} = q), do: %{q | no_bins: true}

  defp namespace_set(%Scan{namespace: ns, set: set}), do: {ns, set}
  defp namespace_set(%Query{namespace: ns, set: set}), do: {ns, set}

  @spec build_wire(Scan.t(), ScanQuery.node_partitions(), keyword()) :: iodata()
  defp build_wire(%Scan{} = s, np, opts), do: ScanQuery.build_scan(s, np, opts)

  @spec build_wire(Query.t(), ScanQuery.node_partitions(), keyword()) :: iodata()
  defp build_wire(%Query{} = q, np, opts), do: ScanQuery.build_query(q, np, opts)

  defp replica_index_from_opts(opts) do
    case Keyword.get(opts, :replica_index) do
      n when is_integer(n) and n >= 0 ->
        n

      _ ->
        replica_index_from_replica_atom(Keyword.get(opts, :replica))
    end
  end

  defp replica_index_from_replica_atom(n) when is_integer(n) and n >= 0, do: n
  defp replica_index_from_replica_atom(:master), do: 0
  defp replica_index_from_replica_atom(:sequence), do: 0
  defp replica_index_from_replica_atom(:any), do: 0
  defp replica_index_from_replica_atom(_), do: 0

  defp policy_kind(%Scan{}), do: :scan
  defp policy_kind(%Query{}), do: :query

  defp scan_query_kind(%Scan{}), do: :scan
  defp scan_query_kind(%Query{}), do: :query

  defp scannable_command(:all, %Scan{}), do: :scan_all
  defp scannable_command(:all, %Query{}), do: :query_all
  defp scannable_command(:count, %Scan{}), do: :scan_count
  defp scannable_command(:count, %Query{}), do: :query_count
  defp scannable_command(:page, %Scan{}), do: :scan_page
  defp scannable_command(:page, %Query{}), do: :query_page

  defp stream_telemetry_meta(conn_name, scannable, ns, set) do
    %{
      command: :stream,
      conn: conn_name,
      namespace: ns,
      set: set || "",
      kind: scan_query_kind(scannable)
    }
  end

  defp with_scan_telemetry(command, conn, scannable, fun)
       when is_atom(command) and is_atom(conn) and is_function(fun, 0) do
    {ns, set} = namespace_set(scannable)
    set_str = set || ""

    meta = %{
      command: command,
      conn: conn,
      namespace: ns,
      set: set_str,
      kind: scan_query_kind(scannable)
    }

    :telemetry.span([:aerospike, :command], meta, fn ->
      result = fun.()
      stop = %{result: telemetry_scan_result(result)}
      {result, stop}
    end)
  end

  defp telemetry_scan_result({:ok, _}), do: :ok
  defp telemetry_scan_result({:error, %Error{code: code}}), do: {:error, code}

  defp maybe_record_device_overload(
         conn_name,
         node_name,
         {:error, %Error{code: :device_overload}}
       ) do
    CircuitBreaker.record_error(conn_name, node_name, :device_overload)
  end

  defp maybe_record_device_overload(_conn_name, _node_name, _result), do: :ok
end
