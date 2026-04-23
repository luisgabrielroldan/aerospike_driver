defmodule Aerospike.Runtime.StreamingExecutor do
  @moduledoc """
  Internal executor for node-scoped streaming commands.

  This module owns the shared operational flow around streaming command
  hooks: node-handle resolution, breaker checks, connect/open/read/close,
  stream frame decoding, and common invalid-node shaping.
  """

  alias Aerospike.Cluster.CircuitBreaker
  alias Aerospike.Error
  alias Aerospike.Protocol.Message
  alias Aerospike.Query
  alias Aerospike.Scan
  alias Aerospike.Command.StreamingCommand
  alias Aerospike.Cluster.Tender

  @type runtime :: %{
          required(:tender) => GenServer.server(),
          required(:transport) => module()
        }

  @type resolve_handle_fun ::
          (GenServer.server(), String.t() ->
             {:ok, Tender.node_handle()} | {:error, :unknown_node})

  @type allow_dispatch_fun :: (term(), term() -> :ok | {:error, Error.t()})

  @type command_ctx :: %{
          required(:scannable) => Scan.t() | Query.t(),
          required(:node_request) => map(),
          optional(:resolve_handle) => resolve_handle_fun(),
          optional(:allow_dispatch) => allow_dispatch_fun()
        }

  @type task_result :: {:ok, term()} | {:exit, term()}
  @type task_runner ::
          ([map()], (map() -> term()), keyword() -> Enumerable.t())

  @spec run_node_requests(
          StreamingCommand.t(),
          runtime(),
          Scan.t() | Query.t(),
          [map()],
          %{
            required(:task_timeout) => non_neg_integer(),
            required(:max_concurrent_nodes) => integer()
          },
          keyword()
        ) :: Enumerable.t()
  def run_node_requests(
        %StreamingCommand{} = command,
        runtime,
        scannable,
        node_requests,
        %{task_timeout: task_timeout} = policy,
        opts \\ []
      )
      when is_map(runtime) and is_list(node_requests) and is_list(opts) do
    ctx_overrides = Keyword.get(opts, :ctx_overrides, %{})
    task_runner = Keyword.get(opts, :task_runner, &Task.async_stream/3)

    task_runner.(
      node_requests,
      &run_command(
        command,
        runtime,
        Map.merge(ctx_overrides, %{scannable: scannable, node_request: &1})
      ),
      ordered: true,
      max_concurrency: max_concurrency(policy, length(node_requests)),
      timeout: task_timeout,
      on_timeout: :kill_task
    )
  end

  @spec run_command(StreamingCommand.t(), runtime(), command_ctx()) :: term()
  def run_command(%StreamingCommand{} = command, runtime, ctx)
      when is_map(runtime) and is_map(ctx) do
    %{node_request: %{node_name: node_name}} = ctx
    resolve_handle = Map.get(ctx, :resolve_handle, &Tender.node_handle/2)

    case resolve_handle.(runtime.tender, node_name) do
      {:ok, handle} ->
        check_breaker(command, runtime, ctx, handle)

      {:error, :unknown_node} ->
        {:error, unknown_node(ctx.scannable, node_name)}
    end
  end

  defp check_breaker(command, runtime, ctx, handle) do
    allow_dispatch = Map.get(ctx, :allow_dispatch, &CircuitBreaker.allow?/2)

    case allow_dispatch.(handle.counters, handle.breaker) do
      :ok ->
        connect_and_stream(command, runtime.transport, ctx, handle)

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp connect_and_stream(command, transport, ctx, handle) do
    timeout = ctx.node_request.policy.timeout
    node_opts = [use_compression: handle.use_compression, attempt: 0]
    request = StreamingCommand.build_request(command, ctx)

    case transport.connect(handle.host, handle.port, handle.connect_opts) do
      {:ok, conn} ->
        case transport.stream_open(conn, request, timeout, node_opts) do
          {:ok, stream} ->
            read_stream(command, transport, stream, timeout, ctx)

          {:error, %Error{} = err} ->
            StreamingCommand.error_result(command, err, ctx)
        end

      {:error, %Error{} = err} ->
        StreamingCommand.error_result(command, err, ctx)
    end
  end

  defp read_stream(command, transport, stream, timeout, ctx) do
    result =
      do_read_stream(
        command,
        transport,
        stream,
        timeout,
        ctx,
        StreamingCommand.init(command, ctx)
      )

    _ = transport.stream_close(stream)
    result
  end

  defp do_read_stream(command, transport, stream, timeout, ctx, acc) do
    case transport.stream_read(stream, timeout) do
      {:ok, frame} ->
        handle_stream_frame(command, transport, stream, timeout, ctx, acc, frame)

      :done ->
        StreamingCommand.finish(command, acc, ctx)

      {:error, %Error{} = err} ->
        StreamingCommand.error_result(command, err, ctx)
    end
  end

  defp handle_stream_frame(command, transport, stream, timeout, ctx, acc, frame) do
    with {:ok, body} <- decode_stream_body(frame) do
      case StreamingCommand.consume_frame(command, body, ctx, acc) do
        {:cont, next_acc} ->
          do_read_stream(command, transport, stream, timeout, ctx, next_acc)

        {:halt, result} ->
          result

        {:error, %Error{} = err} ->
          StreamingCommand.error_result(command, err, ctx)
      end
    else
      {:error, %Error{} = err} ->
        StreamingCommand.error_result(command, err, ctx)
    end
  end

  defp unknown_node(%Scan{}, node_name) do
    Error.from_result_code(:invalid_node, message: "scan target node unavailable: #{node_name}")
  end

  defp unknown_node(%Query{}, node_name) do
    Error.from_result_code(:invalid_node, message: "query target node unavailable: #{node_name}")
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

  defp max_concurrency(%{max_concurrent_nodes: 0}, fallback), do: fallback

  defp max_concurrency(%{max_concurrent_nodes: n}, fallback) when is_integer(n) and n > 0 do
    min(n, fallback)
  end
end
