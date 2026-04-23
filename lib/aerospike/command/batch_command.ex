defmodule Aerospike.Command.BatchCommand do
  @moduledoc """
  Internal contract for grouped batch command hooks.

  Batch execution owns node-level dispatch, retry, and bounded fan-out.
  Command modules supply only the per-node request builder, reply parser,
  and final merge step back into caller-facing order.
  """

  alias Aerospike.Error
  alias Aerospike.Runtime.Executor
  alias Aerospike.RetryPolicy
  alias Aerospike.Cluster.Router

  @type dispatch :: :write | {:read, Router.replica_policy(), non_neg_integer()}
  @type entry_kind :: :read | :read_header | :exists | :put | :delete | :operate | :udf
  @type result_status :: :ok | :error

  defmodule Entry do
    @moduledoc false

    @enforce_keys [:index, :key, :kind, :dispatch]
    defstruct [:index, :key, :kind, :dispatch, :payload]

    @type t :: %__MODULE__{
            index: non_neg_integer(),
            key: Aerospike.Key.t(),
            kind: Aerospike.Command.BatchCommand.entry_kind(),
            dispatch: Aerospike.Command.BatchCommand.dispatch(),
            payload: term()
          }
  end

  defmodule Result do
    @moduledoc false

    @enforce_keys [:index, :key, :kind, :status, :record, :error, :in_doubt]
    defstruct [:index, :key, :kind, :status, :record, :error, :in_doubt]

    @type t :: %__MODULE__{
            index: non_neg_integer(),
            key: Aerospike.Key.t(),
            kind: Aerospike.Command.BatchCommand.entry_kind(),
            status: Aerospike.Command.BatchCommand.result_status(),
            record: term() | nil,
            error: Error.t() | atom() | nil,
            in_doubt: boolean()
          }
  end

  defmodule NodeRequest do
    @moduledoc false

    @enforce_keys [:node_name, :entries]
    defstruct [:node_name, :entries, :payload]

    @type t :: %__MODULE__{
            node_name: String.t(),
            entries: [Entry.t()],
            payload: term()
          }
  end

  defmodule NodeResult do
    @moduledoc false

    @enforce_keys [:request, :result, :attempt]
    defstruct [:request, :result, :attempt]

    @type t :: %__MODULE__{
            request: NodeRequest.t(),
            result: Aerospike.Command.BatchCommand.command_result(),
            attempt: non_neg_integer()
          }
  end

  defmodule Regroup do
    @moduledoc false

    @enforce_keys [:node_requests, :node_results]
    defstruct [:node_requests, :node_results]

    @type t :: %__MODULE__{
            node_requests: [NodeRequest.t()],
            node_results: [NodeResult.t()]
          }
  end

  @enforce_keys [:name, :build_request, :parse_response, :merge_results, :transport_mode]
  defstruct [:name, :build_request, :parse_response, :merge_results, :transport_mode]

  @type batch_input :: term()
  @type command_result :: {:ok, term()} | {:error, Error.t()} | {:error, atom()}
  @type build_request_fun :: (NodeRequest.t() -> iodata())
  @type parse_response_fun :: (body :: binary(), NodeRequest.t() -> command_result() | Error.t())
  @type merge_results_fun :: ([NodeResult.t()], batch_input() -> term())
  @type transport_mode :: :command | :command_stream
  @type reroute_kind :: :rebalance | :transport
  @type on_rebalance_fun :: Executor.on_rebalance_fun()
  @type reroute_request_fun ::
          (reroute_kind(), NodeRequest.t(), non_neg_integer() ->
             {:ok, Regroup.t() | NodeRequest.t() | [NodeRequest.t()] | Executor.Progress.t()}
             | {:error, Error.t() | atom()})
  @type resolve_handle_fun :: Executor.resolve_handle_fun()
  @type allow_dispatch_fun :: Executor.allow_dispatch_fun()
  @type checkout_fun :: Executor.checkout_fun()
  @type task_runner_fun ::
          ([NodeRequest.t()], (NodeRequest.t() -> [NodeResult.t()]), keyword() ->
             [[NodeResult.t()]])
  @type dispatch_ctx :: %{
          required(:tender) => GenServer.server(),
          required(:transport) => module(),
          optional(:max_concurrency) => pos_integer(),
          optional(:reroute_request) => reroute_request_fun(),
          optional(:resolve_handle) => resolve_handle_fun(),
          optional(:allow_dispatch) => allow_dispatch_fun(),
          optional(:checkout) => checkout_fun(),
          optional(:task_runner) => task_runner_fun()
        }

  @type t :: %__MODULE__{
          name: module(),
          build_request: build_request_fun(),
          parse_response: parse_response_fun(),
          merge_results: merge_results_fun(),
          transport_mode: transport_mode()
        }

  @doc """
  Builds a batch command contract.
  """
  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    %__MODULE__{
      name: Keyword.fetch!(opts, :name),
      build_request: Keyword.fetch!(opts, :build_request),
      parse_response: Keyword.fetch!(opts, :parse_response),
      merge_results: Keyword.fetch!(opts, :merge_results),
      transport_mode: Keyword.get(opts, :transport_mode, :command)
    }
  end

  @doc """
  Applies the command's final merge step to per-node outcomes.
  """
  @spec merge_results(t(), [NodeResult.t()], batch_input()) :: term()
  def merge_results(%__MODULE__{merge_results: merge_results}, node_results, batch_input)
      when is_function(merge_results, 2) and is_list(node_results) do
    merge_results.(node_results, batch_input)
  end

  @doc """
  Executes grouped node requests through the shared runtime and applies the
  command's final merge step.
  """
  @spec run(t(), Executor.t(), batch_input(), [NodeRequest.t()], dispatch_ctx()) :: term()
  def run(%__MODULE__{} = command, %Executor{} = executor, batch_input, node_requests, ctx)
      when is_list(node_requests) and is_map(ctx) do
    node_results =
      run_node_requests(executor, command, node_requests, ctx)

    merge_results(command, node_results, batch_input)
  end

  @doc """
  Runs the transport-facing edge for one grouped node request.
  """
  @spec run_transport(
          t(),
          module(),
          conn :: term(),
          NodeRequest.t(),
          deadline_ms :: non_neg_integer(),
          command_opts :: keyword()
        ) :: {command_result(), Aerospike.Cluster.NodePool.checkin_value()}
  def run_transport(
        %__MODULE__{
          build_request: build_request,
          parse_response: parse_response,
          transport_mode: transport_mode
        },
        transport,
        conn,
        %NodeRequest{} = node_request,
        deadline_ms,
        command_opts
      )
      when is_atom(transport) and is_function(build_request, 1) and is_function(parse_response, 2) do
    request = build_request.(node_request)

    case run_transport_request(
           transport_mode,
           transport,
           conn,
           request,
           deadline_ms,
           command_opts
         ) do
      {:ok, body} ->
        result = normalize_result(parse_response.(body, node_request))
        {result, checkin_value(result, conn)}

      {:error, %Error{}} = err ->
        {err, checkin_value(err, conn)}
    end
  end

  defp normalize_result({:ok, _} = ok), do: ok
  defp normalize_result({:error, %Error{}} = err), do: err
  defp normalize_result({:error, reason}) when is_atom(reason), do: {:error, reason}
  defp normalize_result(%Error{} = err), do: {:error, err}

  defp run_transport_request(:command, transport, conn, request, deadline_ms, command_opts) do
    transport.command(conn, request, deadline_ms, command_opts)
  end

  defp run_transport_request(:command_stream, transport, conn, request, deadline_ms, command_opts) do
    transport.command_stream(conn, request, deadline_ms, command_opts)
  end

  defp run_node_requests(executor, command, node_requests, ctx) do
    task_runner = Map.get(ctx, :task_runner, &default_task_runner/3)

    task_runner.(
      node_requests,
      &run_node_request(executor, command, ctx, &1),
      max_concurrency: Map.get(ctx, :max_concurrency, max(length(node_requests), 1)),
      timeout: max(Executor.remaining_budget(executor), 0)
    )
    |> List.flatten()
  end

  defp default_task_runner(node_requests, node_fun, opts) do
    timeout = Keyword.fetch!(opts, :timeout)
    max_concurrency = Keyword.fetch!(opts, :max_concurrency)

    node_requests
    |> Task.async_stream(node_fun,
      ordered: true,
      max_concurrency: max_concurrency,
      timeout: timeout,
      on_timeout: :kill_task
    )
    |> Enum.zip(node_requests)
    |> Enum.map(&normalize_task_result/1)
  end

  defp normalize_task_result({{:ok, node_results}, _node_request}) when is_list(node_results),
    do: node_results

  defp normalize_task_result({{:exit, reason}, %NodeRequest{} = node_request}) do
    [
      %NodeResult{
        request: node_request,
        attempt: 0,
        result:
          {:error,
           %Error{
             code: :timeout,
             message: "Aerospike batch node request task exited: #{inspect(reason)}"
           }}
      }
    ]
  end

  defp run_node_request(executor, command, ctx, %NodeRequest{} = node_request) do
    callbacks = %{
      route_unit: fn %NodeRequest{node_name: node_name}, _attempt -> {:ok, node_name} end,
      run_transport: fn %NodeRequest{} = request,
                        _node_name,
                        transport,
                        conn,
                        remaining,
                        command_opts ->
        run_transport(command, transport, conn, request, remaining, command_opts)
      end,
      progress_retry: fn reroute_kind, %NodeRequest{} = request, next_attempt, _last_result ->
        reroute_request(ctx, reroute_kind, request, next_attempt)
      end
    }

    executor
    |> Executor.run_unit(node_request, ctx, callbacks)
    |> Enum.map(&node_result_from_outcome/1)
  end

  defp reroute_request(ctx, reroute_kind, %NodeRequest{} = node_request, next_attempt) do
    reroute_request = Map.get(ctx, :reroute_request, &default_reroute_request/3)

    reroute_request.(reroute_kind, node_request, next_attempt)
    |> normalize_regroup()
  end

  defp default_reroute_request(_reroute_kind, %NodeRequest{} = node_request, _next_attempt) do
    {:ok, Executor.progress([node_request])}
  end

  defp normalize_regroup({:ok, %Regroup{} = regroup}) do
    {:ok,
     Executor.progress(
       regroup.node_requests,
       Enum.map(regroup.node_results, &outcome_from_node_result/1)
     )}
  end

  defp normalize_regroup({:ok, %Executor.Progress{} = progress}), do: {:ok, progress}

  defp normalize_regroup({:ok, %NodeRequest{} = node_request}) do
    {:ok, Executor.progress([node_request])}
  end

  defp normalize_regroup({:ok, node_requests}) when is_list(node_requests) do
    {:ok, Executor.progress(node_requests)}
  end

  defp normalize_regroup(other), do: other

  defp node_result_from_outcome(%Executor.Outcome{
         unit: request,
         result: result,
         attempt: attempt
       }) do
    %NodeResult{request: request, result: result, attempt: attempt}
  end

  defp outcome_from_node_result(%NodeResult{} = node_result) do
    %Executor.Outcome{
      unit: node_result.request,
      node_name: node_result.request.node_name,
      result: node_result.result,
      attempt: node_result.attempt
    }
  end

  defp checkin_value(result, conn) do
    case RetryPolicy.classify(result) do
      %{close_connection?: true, node_failure?: true} -> {:close, :failure}
      %{close_connection?: true} -> :close
      _ -> conn
    end
  end
end
