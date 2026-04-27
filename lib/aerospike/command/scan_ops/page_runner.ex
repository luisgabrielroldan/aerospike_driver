defmodule Aerospike.Command.ScanOps.PageRunner do
  @moduledoc false

  alias Aerospike.Cluster
  alias Aerospike.Cluster.PartitionMap
  alias Aerospike.Cluster.Router
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.NodePartitions
  alias Aerospike.Command.PartitionTracker
  alias Aerospike.Command.ScanOps
  alias Aerospike.Command.StreamingCommand
  alias Aerospike.Cursor
  alias Aerospike.Error
  alias Aerospike.Page
  alias Aerospike.PartitionFilter
  alias Aerospike.Policy
  alias Aerospike.Protocol.ScanQuery
  alias Aerospike.Protocol.ScanResponse
  alias Aerospike.Query
  alias Aerospike.Record
  alias Aerospike.Runtime.StreamingExecutor
  alias Aerospike.Scan

  @typep runtime :: %{
           tender: GenServer.server(),
           transport: module(),
           tables: Router.tables()
         }

  @spec page(GenServer.server(), Scan.t() | Query.t(), keyword()) ::
          {:ok, Page.t()} | {:error, Error.t()}
  def page(tender, scannable, opts) when is_list(opts) do
    {cursor, opts2} = Keyword.pop(opts, :cursor)

    with {:ok, scannable2} <- apply_optional_cursor(scannable, cursor) do
      page_internal(tender, scannable2, opts2, nil)
    end
  end

  @spec page_node(GenServer.server(), String.t(), Scan.t() | Query.t(), keyword()) ::
          {:ok, Page.t()} | {:error, Error.t()}
  def page_node(tender, node_name, scannable, opts)
      when is_binary(node_name) and is_list(opts) do
    {cursor, opts2} = Keyword.pop(opts, :cursor)

    with {:ok, scannable2} <- apply_optional_cursor(scannable, cursor) do
      page_internal(tender, scannable2, opts2, node_name)
    end
  end

  @spec all(GenServer.server(), Query.t(), keyword()) ::
          {:ok, [Record.t()]} | {:error, Error.t()}
  def all(tender, %Query{} = query, opts) when is_list(opts) do
    with :ok <- require_max_records(query),
         {:ok, first_page} <- page(tender, query, opts) do
      collect_all(tender, query, opts, first_page.records, first_page)
    end
  end

  @spec all_node(GenServer.server(), String.t(), Query.t(), keyword()) ::
          {:ok, [Record.t()]} | {:error, Error.t()}
  def all_node(tender, node_name, %Query{} = query, opts)
      when is_binary(node_name) and is_list(opts) do
    with :ok <- require_max_records(query),
         {:ok, first_page} <- page_node(tender, node_name, query, opts) do
      collect_all_node(tender, node_name, query, opts, first_page.records, first_page)
    end
  end

  @doc false
  @spec runtime(GenServer.server(), Scan.t() | Query.t()) ::
          {:ok, runtime()} | {:error, Error.t()}
  def runtime(tender, scannable) do
    if Cluster.ready?(tender) do
      {:ok,
       %{
         tender: tender,
         transport: Tender.transport(tender),
         tables: Cluster.tables(tender)
       }}
    else
      {:error,
       Error.from_result_code(
         :cluster_not_ready,
         message: "#{operation_name(scannable)} requires a ready cluster"
       )}
    end
  end

  @doc false
  @spec prepare_node_requests(
          runtime(),
          Scan.t() | Query.t(),
          String.t() | nil,
          keyword() | Policy.ScanQueryRuntime.t()
        ) :: {:ok, PartitionTracker.t(), [map()]} | {:error, Error.t()}
  def prepare_node_requests(runtime, scannable, node_filter, opts) when is_list(opts) do
    with {:ok, policy} <- Policy.scan_query_runtime(opts) do
      prepare_node_requests(runtime, scannable, node_filter, policy)
    end
  end

  def prepare_node_requests(runtime, scannable, node_filter, %Policy.ScanQueryRuntime{} = policy) do
    do_prepare_node_requests(runtime, scannable, node_filter, policy)
  end

  defp do_prepare_node_requests(runtime, scannable, node_filter, policy) do
    with {:ok, node_names} <- active_nodes(runtime.tender, node_filter, scannable),
         {:ok, tracker} <- new_tracker(scannable, node_names, node_filter),
         {:ok, partition_map} <- partition_map(runtime, scannable),
         {:ok, tracker, node_partitions} <-
           PartitionTracker.assign_partitions_to_nodes(tracker, partition_map) do
      tracker = activate_record_budget(tracker, scannable)
      {:ok, tracker, build_node_requests(node_partitions, scannable, policy)}
    end
  end

  defp collect_all(_tender, _query, _opts, acc, %Page{done?: true}), do: {:ok, acc}
  defp collect_all(_tender, _query, _opts, acc, %Page{cursor: nil}), do: {:ok, acc}

  defp collect_all(tender, query, opts, acc, %Page{cursor: cursor}) do
    case page(tender, query, Keyword.put(opts, :cursor, cursor)) do
      {:ok, %Page{} = page} ->
        collect_all(tender, attach_cursor(query, cursor), opts, acc ++ page.records, page)

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp collect_all_node(_tender, _node_name, _query, _opts, acc, %Page{done?: true}),
    do: {:ok, acc}

  defp collect_all_node(_tender, _node_name, _query, _opts, acc, %Page{cursor: nil}),
    do: {:ok, acc}

  defp collect_all_node(tender, node_name, query, opts, acc, %Page{cursor: cursor}) do
    case page_node(tender, node_name, query, Keyword.put(opts, :cursor, cursor)) do
      {:ok, %Page{} = page} ->
        collect_all_node(
          tender,
          node_name,
          attach_cursor(query, cursor),
          opts,
          acc ++ page.records,
          page
        )

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp page_internal(tender, scannable, opts, node_filter) do
    with {:ok, runtime} <- runtime(tender, scannable),
         {:ok, policy} <- Policy.scan_query_runtime(opts),
         {:ok, tracker, node_requests} <-
           prepare_node_requests(runtime, scannable, node_filter, policy) do
      run_tracker_page(tender, scannable, node_filter, runtime, policy, tracker, node_requests)
    end
  end

  defp active_nodes(tender, nil, scannable) do
    tender
    |> Cluster.active_nodes()
    |> case do
      [] ->
        {:error,
         Error.from_result_code(
           :cluster_not_ready,
           message: "#{operation_name(scannable)} requires active nodes"
         )}

      active ->
        {:ok, active}
    end
  end

  defp active_nodes(tender, node_name, _scannable) do
    if Cluster.active_node?(tender, node_name),
      do: {:ok, [node_name]},
      else: {:error, unknown_node(node_name)}
  end

  defp new_tracker(%Scan{} = scan, node_names, node_filter) do
    filter = scan.partition_filter || PartitionFilter.all()

    {:ok,
     PartitionTracker.new(filter,
       nodes: node_names,
       max_records: scan.max_records || 0,
       node_filter: node_filter
     )}
  rescue
    err in [ArgumentError] ->
      {:error, Error.from_result_code(:invalid_argument, message: err.message)}
  end

  defp new_tracker(%Query{} = query, node_names, node_filter) do
    filter = query.partition_filter || PartitionFilter.all()

    {:ok,
     PartitionTracker.new(filter,
       nodes: node_names,
       max_records: query.max_records || 0,
       node_filter: node_filter
     )}
  rescue
    err in [ArgumentError] ->
      {:error, Error.from_result_code(:invalid_argument, message: err.message)}
  end

  defp partition_map(runtime, %Scan{namespace: namespace}) do
    partition_map_for_namespace(runtime, namespace)
  end

  defp partition_map(runtime, %Query{namespace: namespace}) do
    partition_map_for_namespace(runtime, namespace)
  end

  defp partition_map_for_namespace(runtime, namespace) do
    owners_tab = runtime.tables.owners
    count = PartitionMap.partition_count()

    0..(count - 1)
    |> Enum.reduce_while({:ok, %{}}, fn partition_id, {:ok, acc} ->
      case PartitionMap.owners(owners_tab, namespace, partition_id) do
        {:ok, %{replicas: replicas}} ->
          {:cont, {:ok, Map.put(acc, partition_id, replicas)}}

        {:error, :unknown_partition} ->
          {:halt,
           {:error,
            Error.from_result_code(:cluster_not_ready, message: "query requires a ready cluster")}}
      end
    end)
  end

  defp build_node_requests(node_partitions, scannable, %Policy.ScanQueryRuntime{} = policy) do
    Enum.map(node_partitions, fn node_partitions_item ->
      %{
        node_name: node_partitions_item.node,
        node_partitions: node_partitions_item,
        policy: policy,
        scannable: scannable,
        pool_checkout_timeout: policy.pool_checkout_timeout
      }
    end)
  end

  defp run_tracker_page(
         tender,
         scannable,
         node_filter,
         runtime,
         policy,
         tracker,
         node_requests,
         acc \\ []
       ) do
    case run_page_jobs(
           runtime,
           scannable,
           tracker,
           node_requests,
           policy
         ) do
      {:ok, tracker, node_partitions_list, page_records} ->
        tracker = %{tracker | node_partitions_list: node_partitions_list}
        all_records = acc ++ page_records

        case PartitionTracker.is_complete?(tracker, true) do
          {:complete, filter, _tracker2} ->
            {:ok,
             %Page{records: all_records, cursor: cursor_from_filter(filter), done?: filter.done?}}

          {:continue, tracker2} ->
            maybe_sleep_between_iterations(tracker2)

            continue_tracker_page(
              tender,
              scannable,
              node_filter,
              runtime,
              policy,
              tracker2,
              all_records
            )

          {:error, %Error{} = err, _tracker2} ->
            {:error, err}
        end

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp prepare_iteration(tender, scannable, tracker, %Policy.ScanQueryRuntime{} = policy) do
    with {:ok, partition_map} <- partition_map(%{tables: Cluster.tables(tender)}, scannable),
         {:ok, tracker, node_partitions_list} <-
           PartitionTracker.assign_partitions_to_nodes(tracker, partition_map) do
      tracker =
        activate_record_budget(%{tracker | node_partitions_list: node_partitions_list}, scannable)

      {:ok, tracker, build_node_requests(node_partitions_list, scannable, policy)}
    end
  end

  defp continue_tracker_page(
         tender,
         scannable,
         node_filter,
         runtime,
         policy,
         tracker,
         all_records
       ) do
    case prepare_iteration(tender, scannable, tracker, policy) do
      {:ok, next_tracker, next_node_requests} ->
        run_tracker_page(
          tender,
          scannable,
          node_filter,
          runtime,
          policy,
          next_tracker,
          next_node_requests,
          all_records
        )

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp run_page_jobs(
         runtime,
         scannable,
         tracker,
         node_requests,
         policy
       ) do
    command = page_command()

    StreamingExecutor.run_node_requests(command, runtime, scannable, node_requests, policy)
    |> Enum.reduce_while({:ok, tracker, [], []}, fn
      {:ok, {:ok, records, parts, np}}, {:ok, tracker_acc, node_parts_acc, records_acc} ->
        {tracker2, np2, kept_records} =
          fold_successful_page_result(tracker_acc, np, records, parts)

        {:cont, {:ok, tracker2, [np2 | node_parts_acc], records_acc ++ kept_records}}

      {:ok, {:error, %Error{code: code} = err, np}},
      {:ok, tracker_acc, node_parts_acc, records_acc}
      when code in [:invalid_node, :pool_timeout] ->
        {tracker2, np2} = mark_node_partitions_unavailable(tracker_acc, np, err)
        {:cont, {:ok, tracker2, [np2 | node_parts_acc], records_acc}}

      {:ok, {:error, %Error{} = err, np}}, {:ok, tracker_acc, node_parts_acc, records_acc} ->
        case PartitionTracker.should_retry?(tracker_acc, np, err) do
          {true, tracker2, np2} ->
            {:cont, {:ok, tracker2, [np2 | node_parts_acc], records_acc}}

          {false, _tracker2, _np2} ->
            {:halt, {:error, err}}
        end

      {:exit, reason}, _acc ->
        {:halt, {:error, Error.from_result_code(:network_error, message: inspect(reason))}}
    end)
    |> case do
      {:ok, tracker, node_partitions, page_records} ->
        {:ok, tracker, Enum.reverse(node_partitions), page_records}

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp fold_successful_page_result(tracker, np, records, parts) do
    {tracker, np} = fold_result_records(tracker, np, records)
    {tracker, np, kept_records} = ScanOps.allow_record_fold(tracker, np, records)
    {tracker, np} = do_fold_partition_done_info(tracker, np, parts)
    {tracker, np, kept_records}
  end

  defp fold_result_records(tracker, np, records) do
    Enum.reduce(records, {tracker, np}, fn %Record{key: key}, {tracker_acc, np_acc} ->
      PartitionTracker.set_digest(
        tracker_acc,
        np_acc,
        {Aerospike.Key.partition_id(key), key.digest}
      )
    end)
  end

  defp do_fold_partition_done_info(tracker, np, parts) do
    Enum.reduce(parts, {tracker, np}, fn part, {tracker_acc, np_acc} ->
      cond do
        Map.get(part, :unavailable?, false) ->
          PartitionTracker.partition_unavailable(tracker_acc, np_acc, part.id)

        is_binary(Map.get(part, :digest)) and is_integer(Map.get(part, :bval)) ->
          PartitionTracker.set_last(tracker_acc, np_acc, part.id, {part.digest, part.bval})

        is_binary(Map.get(part, :digest)) ->
          PartitionTracker.set_digest(tracker_acc, np_acc, {part.id, part.digest})

        true ->
          {tracker_acc, np_acc}
      end
    end)
  end

  defp mark_node_partitions_unavailable(tracker, np, err) do
    ids = Enum.map(np.parts_full, & &1.id) ++ Enum.map(np.parts_partial, & &1.id)

    {tracker, np} =
      Enum.reduce(ids, {tracker, np}, fn id, {tracker_acc, np_acc} ->
        PartitionTracker.partition_unavailable(tracker_acc, np_acc, id)
      end)

    {%{tracker | exceptions: tracker.exceptions ++ [err]}, np}
  end

  defp maybe_sleep_between_iterations(tracker) do
    case PartitionTracker.should_sleep_for(tracker) do
      ms when is_integer(ms) and ms > 0 ->
        Process.sleep(ms)
        :ok

      _ ->
        :ok
    end
  end

  defp cursor_from_filter(%PartitionFilter{done?: true}), do: nil
  defp cursor_from_filter(%PartitionFilter{partitions: parts}), do: %Cursor{partitions: parts}

  defp attach_cursor(%Query{} = query, %Cursor{partitions: partitions}) do
    %{query | partition_filter: %{PartitionFilter.all() | partitions: partitions}}
  end

  defp require_max_records(%Query{max_records: n}) when is_integer(n) and n > 0, do: :ok
  defp require_max_records(_), do: {:error, Error.from_result_code(:max_records_required)}

  defp activate_record_budget(tracker, %Scan{max_records: n}) when is_integer(n) and n > 0 do
    %{tracker | record_count: 0}
  end

  defp activate_record_budget(tracker, %Query{max_records: n}) when is_integer(n) and n > 0 do
    %{tracker | record_count: 0}
  end

  defp activate_record_budget(tracker, _scannable), do: tracker

  defp apply_optional_cursor(scannable, nil), do: {:ok, scannable}

  defp apply_optional_cursor(scannable, %Cursor{} = cursor),
    do: {:ok, attach_cursor_partition_filter(scannable, cursor)}

  defp apply_optional_cursor(scannable, bin) when is_binary(bin) do
    case Cursor.decode(bin) do
      {:ok, cursor} -> {:ok, attach_cursor_partition_filter(scannable, cursor)}
      {:error, _} = err -> err
    end
  end

  defp apply_optional_cursor(_scannable, other) do
    {:error,
     Error.from_result_code(:parameter_error, message: "invalid cursor: #{inspect(other)}")}
  end

  defp attach_cursor_partition_filter(%Query{} = query, %Cursor{partitions: partitions}) do
    %{query | partition_filter: %{PartitionFilter.all() | partitions: partitions}}
  end

  defp attach_cursor_partition_filter(%Scan{} = scan, %Cursor{partitions: partitions}) do
    %{scan | partition_filter: %{PartitionFilter.all() | partitions: partitions}}
  end

  defp namespace(%Scan{namespace: namespace}), do: namespace
  defp namespace(%Query{namespace: namespace}), do: namespace

  defp set(%Scan{set: set}), do: set
  defp set(%Query{set: set}), do: set

  defp build_wire(%Scan{} = scan, node_partitions, %Policy.ScanQueryRuntime{} = policy) do
    ScanQuery.build_scan(scan, request_partitions(node_partitions), policy)
  end

  defp build_wire(%Query{} = query, node_partitions, %Policy.ScanQueryRuntime{} = policy) do
    ScanQuery.build_query(query, request_partitions(node_partitions), policy)
  end

  defp page_command do
    StreamingCommand.new!(
      name: __MODULE__,
      build_request: fn %{scannable: scannable, node_request: node_request} ->
        build_wire(scannable, node_request.node_partitions, node_request.policy)
      end,
      init: fn _input -> {[], []} end,
      consume_frame: fn body,
                        %{scannable: scannable, node_request: node_request},
                        {records, parts} ->
        with {:ok, new_records, new_parts, done?} <-
               ScanResponse.parse_stream_chunk(body, namespace(scannable), set(scannable)) do
          next_records = Enum.reverse(new_records, records)
          next_parts = Enum.reverse(new_parts, parts)

          if done? do
            {:halt, page_result(next_records, next_parts, node_request)}
          else
            {:cont, {next_records, next_parts}}
          end
        end
      end,
      finish: fn {records, parts}, %{node_request: node_request} ->
        page_result(records, parts, node_request)
      end,
      error_result: fn err, %{node_request: node_request} ->
        {:error, err, node_request.node_partitions}
      end
    )
  end

  defp page_result(records, parts, node_request) do
    {:ok, Enum.reverse(records), Enum.reverse(parts), node_request.node_partitions}
  end

  defp request_partitions(%NodePartitions{
         parts_full: full,
         parts_partial: partials,
         record_max: record_max
       }) do
    %{
      parts_full: Enum.map(full, & &1.id),
      parts_partial: Enum.map(partials, &partition_entry/1),
      record_max: record_max
    }
  end

  defp partition_entry(%{id: id} = ps) do
    %{
      id: id,
      digest: Map.get(ps, :digest),
      bval: Map.get(ps, :bval)
    }
  end

  defp unknown_node(node_name) do
    Error.from_result_code(:invalid_node, message: "query target node unavailable: #{node_name}")
  end

  defp operation_name(%Scan{}), do: "scan"
  defp operation_name(%Query{}), do: "query"
end
