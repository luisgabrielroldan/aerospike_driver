defmodule Aerospike.ScanOps.StreamRunner do
  @moduledoc false

  alias Aerospike.CircuitBreaker
  alias Aerospike.Error
  alias Aerospike.PartitionFilter
  alias Aerospike.PartitionMap
  alias Aerospike.PartitionTracker
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.ScanQuery
  alias Aerospike.Protocol.ScanResponse
  alias Aerospike.Query
  alias Aerospike.Scan
  alias Aerospike.Tender

  @default_timeout 5_000
  @default_pool_checkout_timeout 5_000

  @spec stream(GenServer.server(), Scan.t() | Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def stream(tender, scannable, opts) when is_list(opts) do
    stream_internal(tender, scannable, opts, nil)
  end

  @spec stream_node(GenServer.server(), String.t(), Scan.t() | Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def stream_node(tender, node_name, scannable, opts)
      when is_binary(node_name) and is_list(opts) do
    stream_internal(tender, scannable, opts, node_name)
  end

  defp stream_internal(tender, scannable, opts, node_filter) do
    with {:ok, runtime} <- runtime(tender),
         {:ok, node_requests} <- prepare_node_requests(runtime, scannable, node_filter, opts) do
      task_timeout = Keyword.get(opts, :task_timeout, :infinity)
      max_concurrency = max_concurrency(opts, length(node_requests))

      stream =
        node_requests
        |> Task.async_stream(
          fn node_request -> run_node_request(runtime, scannable, node_request, opts) end,
          ordered: true,
          max_concurrency: max_concurrency,
          timeout: task_timeout,
          on_timeout: :kill_task
        )
        |> Stream.flat_map(&flatten_task_result/1)

      {:ok, stream}
    end
  end

  defp runtime(tender) do
    if Tender.ready?(tender) do
      {:ok,
       %{
         tender: tender,
         transport: Tender.transport(tender),
         tables: Tender.tables(tender)
       }}
    else
      {:error,
       Error.from_result_code(:cluster_not_ready, message: "scan requires a ready cluster")}
    end
  end

  defp prepare_node_requests(runtime, %Scan{} = scan, node_filter, opts) do
    with {:ok, node_names} <- active_nodes(runtime.tender, node_filter),
         {:ok, tracker} <- new_tracker(scan, node_names, node_filter),
         {:ok, partition_map} <- partition_map(runtime, scan),
         {:ok, tracker, node_partitions} <-
           PartitionTracker.assign_partitions_to_nodes(tracker, partition_map) do
      {:ok, build_node_requests(tracker, node_partitions, scan, opts)}
    end
  end

  defp prepare_node_requests(runtime, %Query{} = query, node_filter, opts) do
    with {:ok, node_names} <- active_nodes(runtime.tender, node_filter),
         {:ok, tracker} <- new_tracker(query, node_names, node_filter),
         {:ok, partition_map} <- partition_map(runtime, query),
         {:ok, tracker, node_partitions} <-
           PartitionTracker.assign_partitions_to_nodes(tracker, partition_map) do
      {:ok, build_node_requests(tracker, node_partitions, query, opts)}
    end
  end

  defp active_nodes(tender, nil) do
    tender
    |> Tender.nodes_status()
    |> Enum.filter(fn {_node_name, meta} -> Map.get(meta, :status) == :active end)
    |> Enum.map(&elem(&1, 0))
    |> case do
      [] ->
        {:error,
         Error.from_result_code(:cluster_not_ready, message: "scan requires active nodes")}

      active ->
        {:ok, active}
    end
  end

  defp active_nodes(tender, node_name) do
    case Tender.nodes_status(tender) |> Map.fetch(node_name) do
      {:ok, %{status: :active}} -> {:ok, [node_name]}
      {:ok, %{status: :inactive}} -> {:error, unknown_node(node_name)}
      :error -> {:error, unknown_node(node_name)}
    end
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
            Error.from_result_code(:cluster_not_ready, message: "scan requires a ready cluster")}}
      end
    end)
  end

  defp partition_map(runtime, %Query{namespace: namespace}) do
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
            Error.from_result_code(:cluster_not_ready, message: "scan requires a ready cluster")}}
      end
    end)
  end

  defp build_node_requests(_tracker, node_partitions, scannable, opts) do
    task_id = Keyword.get(opts, :task_id, System.unique_integer([:positive]))
    request_opts = [timeout: Keyword.get(opts, :timeout, @default_timeout), task_id: task_id]

    Enum.map(node_partitions, fn node_partitions_item ->
      %{
        node_name: node_partitions_item.node,
        node_partitions: node_partitions_item,
        request_opts: request_opts,
        scannable: scannable,
        pool_checkout_timeout:
          Keyword.get(opts, :pool_checkout_timeout, @default_pool_checkout_timeout)
      }
    end)
  end

  defp run_node_request(runtime, scannable, %{node_name: node_name} = node_request, opts) do
    case Tender.node_handle(runtime.tender, node_name) do
      {:ok, handle} ->
        case CircuitBreaker.allow?(handle.counters, handle.breaker) do
          :ok ->
            connect_and_stream(runtime, scannable, node_request, handle, opts)

          {:error, %Error{} = err} ->
            {:error, err}
        end

      {:error, :unknown_node} ->
        {:error, unknown_node(node_name)}
    end
  end

  defp connect_and_stream(runtime, scannable, node_request, handle, opts) do
    transport = runtime.transport
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    node_opts = [use_compression: handle.use_compression, attempt: 0]

    request = build_wire(scannable, node_request.node_partitions, node_request.request_opts)

    case transport.connect(handle.host, handle.port, handle.connect_opts) do
      {:ok, conn} ->
        case transport.stream_open(conn, request, timeout, node_opts) do
          {:ok, stream} ->
            read_stream(transport, stream, timeout, namespace(scannable), set(scannable))

          {:error, %Error{} = err} ->
            {:error, err}
        end

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp read_stream(transport, stream, timeout, namespace, set) do
    result = do_read_stream(transport, stream, timeout, namespace, set, [])
    _ = transport.stream_close(stream)
    result
  end

  defp do_read_stream(transport, stream, timeout, namespace, set, acc) do
    case transport.stream_read(stream, timeout) do
      {:ok, frame} ->
        handle_stream_frame(transport, stream, timeout, namespace, set, acc, frame)

      :done ->
        {:ok, Enum.reverse(acc)}

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp handle_stream_frame(transport, stream, timeout, namespace, set, acc, frame) do
    with {:ok, body} <- decode_stream_body(frame),
         {:ok, records, _partition_done, done?} <-
           ScanResponse.parse_stream_chunk(body, namespace, set) do
      next_acc = Enum.reverse(records, acc)
      continue_stream(transport, stream, timeout, namespace, set, next_acc, done?)
    end
  end

  defp continue_stream(_transport, _stream, _timeout, _namespace, _set, acc, true) do
    {:ok, Enum.reverse(acc)}
  end

  defp continue_stream(transport, stream, timeout, namespace, set, acc, false) do
    do_read_stream(transport, stream, timeout, namespace, set, acc)
  end

  defp request_partitions(%{parts_full: full, parts_partial: partials, record_max: record_max}) do
    %{
      parts_full: Enum.map(full, & &1.id),
      parts_partial: Enum.map(partials, &partition_entry/1),
      record_max: record_max
    }
  end

  defp build_wire(%Scan{} = scan, node_partitions, opts) do
    ScanQuery.build_scan(scan, request_partitions(node_partitions), opts)
  end

  defp build_wire(%Query{} = query, node_partitions, opts) do
    ScanQuery.build_query(query, request_partitions(node_partitions), opts)
  end

  defp namespace(%Scan{namespace: namespace}), do: namespace
  defp namespace(%Query{namespace: namespace}), do: namespace

  defp set(%Scan{set: set}), do: set
  defp set(%Query{set: set}), do: set

  defp partition_entry(%{id: id} = ps) do
    %{
      id: id,
      digest: Map.get(ps, :digest),
      bval: Map.get(ps, :bval)
    }
  end

  defp flatten_task_result({:ok, {:ok, records}}), do: records

  defp flatten_task_result({:ok, {:error, %Error{} = err}}) do
    raise err
  end

  defp flatten_task_result({:exit, reason}) do
    raise Error.from_result_code(:network_error, message: "scan task exited: #{inspect(reason)}")
  end

  defp max_concurrency(opts, fallback) do
    case Keyword.get(opts, :max_concurrent_nodes, fallback) do
      n when is_integer(n) and n > 0 -> min(n, fallback)
      _ -> fallback
    end
  end

  defp unknown_node(node_name) do
    Error.from_result_code(:invalid_node, message: "scan target node unavailable: #{node_name}")
  end

  defp decode_stream_body(frame) do
    case Message.decode(frame) do
      {:ok, {2, 3, body}} ->
        {:ok, body}

      {:ok, {_version, _type, _body}} ->
        {:error,
         Error.from_result_code(:parse_error, message: "unexpected stream frame type from server")}

      {:error, _reason} ->
        {:error, Error.from_result_code(:parse_error, message: "invalid stream frame")}
    end
  end
end
