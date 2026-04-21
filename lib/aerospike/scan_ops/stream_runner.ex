defmodule Aerospike.ScanOps.StreamRunner do
  @moduledoc false

  alias Aerospike.CircuitBreaker
  alias Aerospike.Error
  alias Aerospike.ExecuteTask
  alias Aerospike.NodePool
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Protocol.ScanQuery
  alias Aerospike.Protocol.ScanResponse
  alias Aerospike.Query
  alias Aerospike.Scan
  alias Aerospike.ScanOps.PageRunner
  alias Aerospike.Tender

  @default_timeout 5_000
  @spec stream(GenServer.server(), Scan.t() | Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def stream(tender, scannable, opts) when is_list(opts) do
    stream_internal(tender, scannable, opts, nil, nil, &ScanResponse.parse_stream_chunk/3)
  end

  @spec stream_node(GenServer.server(), String.t(), Scan.t() | Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def stream_node(tender, node_name, scannable, opts)
      when is_binary(node_name) and is_list(opts) do
    stream_internal(tender, scannable, opts, node_name, nil, &ScanResponse.parse_stream_chunk/3)
  end

  @spec query_aggregate(GenServer.server(), Query.t(), keyword(), String.t(), String.t(), list()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def query_aggregate(tender, %Query{} = query, opts, package, function, args)
      when is_list(opts) and is_binary(package) and is_binary(function) and is_list(args) do
    stream_internal(
      tender,
      query,
      opts,
      nil,
      fn q, node_partitions, run_opts ->
        ScanQuery.build_query_aggregate(q, node_partitions, package, function, args, run_opts)
      end,
      &ScanResponse.parse_aggregate_stream_chunk/3
    )
  end

  @spec query_execute(GenServer.server(), Query.t(), list(), keyword()) ::
          {:ok, ExecuteTask.t()} | {:error, Error.t()}
  def query_execute(tender, %Query{} = query, ops, opts \\ [])
      when is_list(ops) and is_list(opts) do
    run_background_query(
      tender,
      query,
      opts,
      nil,
      fn q, node_partitions, run_opts ->
        ScanQuery.build_query_execute(q, node_partitions, ops, run_opts)
      end,
      :query_execute
    )
  end

  @spec query_execute_node(GenServer.server(), String.t(), Query.t(), list(), keyword()) ::
          {:ok, ExecuteTask.t()} | {:error, Error.t()}
  def query_execute_node(tender, node_name, %Query{} = query, ops, opts \\ [])
      when is_binary(node_name) and is_list(ops) and is_list(opts) do
    run_background_query(
      tender,
      query,
      opts,
      node_name,
      fn q, node_partitions, run_opts ->
        ScanQuery.build_query_execute(q, node_partitions, ops, run_opts)
      end,
      :query_execute
    )
  end

  @spec query_udf(GenServer.server(), Query.t(), String.t(), String.t(), list(), keyword()) ::
          {:ok, ExecuteTask.t()} | {:error, Error.t()}
  def query_udf(tender, %Query{} = query, package, function, args, opts \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    run_background_query(
      tender,
      query,
      opts,
      nil,
      fn q, node_partitions, run_opts ->
        ScanQuery.build_query_udf(q, node_partitions, package, function, args, run_opts)
      end,
      :query_udf
    )
  end

  @spec query_udf_node(
          GenServer.server(),
          String.t(),
          Query.t(),
          String.t(),
          String.t(),
          list(),
          keyword()
        ) :: {:ok, ExecuteTask.t()} | {:error, Error.t()}
  def query_udf_node(tender, node_name, %Query{} = query, package, function, args, opts \\ [])
      when is_binary(node_name) and is_binary(package) and is_binary(function) and is_list(args) and
             is_list(opts) do
    run_background_query(
      tender,
      query,
      opts,
      node_name,
      fn q, node_partitions, run_opts ->
        ScanQuery.build_query_udf(q, node_partitions, package, function, args, run_opts)
      end,
      :query_udf
    )
  end

  defp stream_internal(tender, scannable, opts, node_filter, wire_builder, parser) do
    with {:ok, runtime} <- PageRunner.runtime(tender, scannable),
         {:ok, _tracker, node_requests} <-
           PageRunner.prepare_node_requests(runtime, scannable, node_filter, opts) do
      task_timeout = Keyword.get(opts, :task_timeout, :infinity)
      max_concurrency = max_concurrency(opts, length(node_requests))

      stream =
        node_requests
        |> Task.async_stream(
          fn node_request ->
            run_node_request(runtime, scannable, node_request, opts, wire_builder, parser)
          end,
          ordered: true,
          max_concurrency: max_concurrency,
          timeout: task_timeout,
          on_timeout: :kill_task
        )
        |> Stream.flat_map(&flatten_task_result/1)

      {:ok, stream}
    end
  end

  defp run_background_query(tender, query, opts, node_filter, wire_builder, kind) do
    task_id = Keyword.get(opts, :task_id, System.unique_integer([:positive]))
    opts = Keyword.put_new(opts, :task_id, task_id)

    with {:ok, runtime} <- PageRunner.runtime(tender, query),
         {:ok, _tracker, node_requests} <-
           PageRunner.prepare_node_requests(runtime, query, node_filter, opts) do
      task_timeout = Keyword.get(opts, :task_timeout, :infinity)
      max_concurrency = max_concurrency(opts, length(node_requests))

      results =
        node_requests
        |> Task.async_stream(
          fn node_request ->
            run_background_node_request(runtime, query, node_request, opts, wire_builder)
          end,
          ordered: true,
          max_concurrency: max_concurrency,
          timeout: task_timeout,
          on_timeout: :kill_task
        )
        |> Enum.to_list()

      case background_results(results) do
        :ok ->
          {:ok,
           %ExecuteTask{
             conn: tender,
             namespace: query.namespace,
             set: query.set,
             task_id: task_id,
             kind: kind,
             node_name: node_filter
           }}

        {:error, %Error{} = err} ->
          {:error, err}
      end
    end
  end

  defp run_node_request(
         runtime,
         scannable,
         %{node_name: node_name} = node_request,
         opts,
         wire_builder,
         parser
       ) do
    case Tender.node_handle(runtime.tender, node_name) do
      {:ok, handle} ->
        case CircuitBreaker.allow?(handle.counters, handle.breaker) do
          :ok ->
            connect_and_stream(
              runtime,
              scannable,
              node_request,
              handle,
              opts,
              wire_builder,
              parser
            )

          {:error, %Error{} = err} ->
            {:error, err}
        end

      {:error, :unknown_node} ->
        {:error, unknown_node(node_name)}
    end
  end

  defp connect_and_stream(runtime, scannable, node_request, handle, opts, wire_builder, parser) do
    transport = runtime.transport
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    node_opts = [use_compression: handle.use_compression, attempt: 0]

    request =
      build_wire(scannable, node_request.node_partitions, node_request.request_opts, wire_builder)

    case transport.connect(handle.host, handle.port, handle.connect_opts) do
      {:ok, conn} ->
        case transport.stream_open(conn, request, timeout, node_opts) do
          {:ok, stream} ->
            read_stream(transport, stream, timeout, namespace(scannable), set(scannable), parser)

          {:error, %Error{} = err} ->
            {:error, err}
        end

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp read_stream(transport, stream, timeout, namespace, set, parser) do
    result = do_read_stream(transport, stream, timeout, namespace, set, parser, [])
    _ = transport.stream_close(stream)
    result
  end

  defp do_read_stream(transport, stream, timeout, namespace, set, parser, acc) do
    case transport.stream_read(stream, timeout) do
      {:ok, frame} ->
        handle_stream_frame(transport, stream, timeout, namespace, set, parser, acc, frame)

      :done ->
        {:ok, Enum.reverse(acc)}

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp handle_stream_frame(transport, stream, timeout, namespace, set, parser, acc, frame) do
    with {:ok, body} <- decode_stream_body(frame),
         {:ok, records, _partition_done, done?} <- parser.(body, namespace, set) do
      next_acc = Enum.reverse(records, acc)
      continue_stream(transport, stream, timeout, namespace, set, parser, next_acc, done?)
    end
  end

  defp continue_stream(_transport, _stream, _timeout, _namespace, _set, _parser, acc, true) do
    {:ok, Enum.reverse(acc)}
  end

  defp continue_stream(transport, stream, timeout, namespace, set, parser, acc, false) do
    do_read_stream(transport, stream, timeout, namespace, set, parser, acc)
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

  defp build_wire(%Query{} = query, node_partitions, opts, wire_builder)
       when is_function(wire_builder, 3) do
    wire_builder.(query, request_partitions(node_partitions), opts)
  end

  defp build_wire(%Scan{} = scan, node_partitions, opts, nil),
    do: build_wire(scan, node_partitions, opts)

  defp build_wire(%Query{} = query, node_partitions, opts, nil),
    do: build_wire(query, node_partitions, opts)

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

  defp background_results(results) do
    case Enum.find(results, fn
           {:ok, {:error, %Error{}}} -> true
           {:exit, _} -> true
           _ -> false
         end) do
      {:ok, {:error, %Error{} = err}} ->
        {:error, err}

      {:exit, reason} ->
        {:error,
         Error.from_result_code(:network_error,
           message: "background task exited: #{inspect(reason)}"
         )}

      nil ->
        :ok
    end
  end

  defp run_background_node_request(
         runtime,
         query,
         %{node_name: node_name} = node_request,
         opts,
         wire_builder
       ) do
    case Tender.node_handle(runtime.tender, node_name) do
      {:ok, handle} ->
        case CircuitBreaker.allow?(handle.counters, handle.breaker) do
          :ok ->
            connect_and_run_background(runtime, query, node_request, handle, opts, wire_builder)

          {:error, %Error{} = err} ->
            {:error, err}
        end

      {:error, :unknown_node} ->
        {:error, unknown_node(node_name)}
    end
  end

  defp connect_and_run_background(runtime, query, node_request, handle, opts, wire_builder) do
    transport = runtime.transport
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    node_opts = [use_compression: handle.use_compression, attempt: 0]

    request =
      build_wire(query, node_request.node_partitions, node_request.request_opts, wire_builder)

    NodePool.checkout!(
      node_request.node_name,
      handle.pool,
      fn conn ->
        case transport.command(conn, request, timeout, node_opts) do
          {:ok, body} ->
            result = decode_background_response(body)
            {result, background_checkin_value(result, conn)}

          {:error, %Error{} = err} ->
            result = {:error, err}
            {result, background_checkin_value(result, conn)}
        end
      end,
      node_request.pool_checkout_timeout
    )
  end

  defp decode_background_response(body) when is_binary(body) do
    with {:ok, msg} <- Response.decode_as_msg(body) do
      Response.parse_record_metadata_response(msg)
    end
  end

  defp background_checkin_value(result, conn) do
    case background_checkin_classification(result) do
      %{close_connection?: true, node_failure?: true} -> {:close, :failure}
      %{close_connection?: true} -> :close
      _ -> conn
    end
  end

  defp background_checkin_classification(result) do
    case result do
      {:ok, _} ->
        %{close_connection?: false, node_failure?: false}

      {:error, %Error{code: code}} when code in [:network_error, :timeout, :connection_error] ->
        %{close_connection?: true, node_failure?: true}

      {:error, %Error{}} ->
        %{close_connection?: false, node_failure?: false}
    end
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
