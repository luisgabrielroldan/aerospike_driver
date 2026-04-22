defmodule Aerospike.BatchExecutor do
  @moduledoc """
  Internal runtime for grouped batch execution.

  The executor owns bounded fan-out, node-handle resolution, breaker checks,
  pool checkout, per-node retry boundaries, and the handoff from grouped
  node results to the command's merge hook.
  """

  alias Aerospike.BatchCommand
  alias Aerospike.BatchCommand.NodeRequest
  alias Aerospike.BatchCommand.NodeResult
  alias Aerospike.CircuitBreaker
  alias Aerospike.Error
  alias Aerospike.NodePool
  alias Aerospike.Policy
  alias Aerospike.RetryPolicy
  alias Aerospike.Tender

  @default_max_concurrency max(System.schedulers_online(), 1)

  @type reroute_kind :: :rebalance | :transport
  @type on_rebalance_fun :: (-> :ok)
  @type reroute_request_fun ::
          (reroute_kind(), NodeRequest.t(), non_neg_integer() ->
             {:ok, NodeRequest.t()} | {:error, Error.t() | atom()})
  @type resolve_handle_fun ::
          (GenServer.server(), String.t() ->
             {:ok, Tender.node_handle()} | {:error, :unknown_node})
  @type allow_dispatch_fun :: (term(), term() -> :ok | {:error, Error.t()})
  @type checkout_fun ::
          (String.t(), pid(), (term() -> {term(), NodePool.checkin_value()}), integer() -> term())
  @type task_runner_fun ::
          ([NodeRequest.t()], (NodeRequest.t() -> NodeResult.t()), keyword() -> [NodeResult.t()])
  @type dispatch_ctx :: %{
          required(:tender) => GenServer.server(),
          required(:transport) => module(),
          optional(:reroute_request) => reroute_request_fun(),
          optional(:resolve_handle) => resolve_handle_fun(),
          optional(:allow_dispatch) => allow_dispatch_fun(),
          optional(:checkout) => checkout_fun(),
          optional(:task_runner) => task_runner_fun()
        }

  @type t :: %__MODULE__{
          policy: Policy.BatchRead.t(),
          deadline: integer(),
          max_concurrency: pos_integer(),
          on_rebalance: on_rebalance_fun()
        }

  @enforce_keys [:policy, :deadline, :max_concurrency, :on_rebalance]
  defstruct [:policy, :deadline, :max_concurrency, :on_rebalance]

  @doc """
  Builds a batch executor for one grouped batch call.
  """
  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    policy = Keyword.fetch!(opts, :policy)
    max_concurrency = Keyword.get(opts, :max_concurrency, @default_max_concurrency)
    on_rebalance = Keyword.get(opts, :on_rebalance, fn -> :ok end)

    %__MODULE__{
      policy: policy,
      deadline: monotonic_now() + policy.timeout,
      max_concurrency: validate_max_concurrency!(max_concurrency),
      on_rebalance: on_rebalance
    }
  end

  @doc """
  Returns the remaining monotonic budget in milliseconds.
  """
  @spec remaining_budget(t()) :: integer()
  def remaining_budget(%__MODULE__{deadline: deadline}), do: deadline - monotonic_now()

  @doc """
  Executes grouped node requests and delegates the final merge to `command`.
  """
  @spec run_command(t(), BatchCommand.t(), term(), [NodeRequest.t()], dispatch_ctx()) :: term()
  def run_command(
        %__MODULE__{} = executor,
        %BatchCommand{} = command,
        batch_input,
        node_requests,
        ctx
      )
      when is_list(node_requests) and is_map(ctx) do
    node_results =
      run_node_requests(executor, command, node_requests, ctx)

    BatchCommand.merge_results(command, node_results, batch_input)
  end

  defp run_node_requests(executor, command, node_requests, ctx) do
    task_runner = Map.get(ctx, :task_runner, &default_task_runner/3)

    task_runner.(
      node_requests,
      &run_node_request(executor, command, ctx, &1),
      max_concurrency: executor.max_concurrency,
      timeout: max(remaining_budget(executor), 0)
    )
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

  defp normalize_task_result({{:ok, %NodeResult{} = node_result}, _node_request}), do: node_result

  defp normalize_task_result({{:exit, reason}, %NodeRequest{} = node_request}) do
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
  end

  defp run_node_request(executor, command, ctx, %NodeRequest{} = node_request) do
    attempt_loop(executor, command, ctx, node_request, 0, nil)
  end

  defp attempt_loop(executor, _command, _ctx, %NodeRequest{} = node_request, attempt, last_error)
       when attempt > executor.policy.retry.max_retries do
    %NodeResult{
      request: node_request,
      result: exhausted(last_error, :max_retries),
      attempt: max(attempt - 1, 0)
    }
  end

  defp attempt_loop(executor, command, ctx, %NodeRequest{} = node_request, attempt, last_error) do
    if budget_exhausted?(executor) do
      %NodeResult{
        request: node_request,
        result: exhausted(last_error, :deadline),
        attempt: max(attempt - 1, 0)
      }
    else
      result = dispatch_attempt(executor, attempt, node_request, command, ctx)
      handle_classification(executor, command, ctx, node_request, attempt, result)
    end
  end

  defp dispatch_attempt(executor, attempt, node_request, command, ctx) do
    case dispatch_node(executor, attempt, node_request, command, ctx) do
      {:error, %Error{}} = err -> err
      {:error, reason} when is_atom(reason) -> {:error, reason}
      {:ok, _} = ok -> ok
    end
  end

  defp dispatch_node(executor, attempt, %NodeRequest{} = node_request, command, ctx) do
    resolve_handle = Map.get(ctx, :resolve_handle, &Tender.node_handle/2)

    case resolve_handle.(ctx.tender, node_request.node_name) do
      {:ok, handle} ->
        check_breaker(executor, attempt, node_request, command, ctx, handle)

      {:error, :unknown_node} = err ->
        err
    end
  end

  defp check_breaker(executor, attempt, node_request, command, ctx, handle) do
    allow_dispatch = Map.get(ctx, :allow_dispatch, &CircuitBreaker.allow?/2)

    case allow_dispatch.(handle.counters, handle.breaker) do
      :ok ->
        checkout(executor, attempt, node_request, command, ctx, handle)

      {:error, %Error{}} = err ->
        err
    end
  end

  defp checkout(executor, attempt, node_request, command, ctx, handle) do
    checkout = Map.get(ctx, :checkout, &NodePool.checkout!/4)
    remaining = max(remaining_budget(executor), 0)
    command_opts = [use_compression: handle.use_compression, attempt: attempt]

    checkout.(
      node_request.node_name,
      handle.pool,
      fn conn ->
        BatchCommand.run_transport(
          command,
          ctx.transport,
          conn,
          node_request,
          remaining,
          command_opts
        )
      end,
      remaining
    )
  end

  defp handle_classification(executor, command, ctx, node_request, attempt, result) do
    case RetryPolicy.classify(result) do
      %{bucket: :ok} ->
        %NodeResult{request: node_request, result: result, attempt: attempt}

      %{bucket: :routing_refusal} ->
        %NodeResult{request: node_request, result: result, attempt: attempt}

      %{bucket: :server_fatal} ->
        %NodeResult{request: node_request, result: result, attempt: attempt}

      %{bucket: :transport} ->
        retry_after_error(executor, command, ctx, :transport, node_request, attempt, result)

      %{bucket: :rebalance} ->
        executor.on_rebalance.()
        retry_after_error(executor, command, ctx, :rebalance, node_request, attempt, result)
    end
  end

  defp retry_after_error(executor, command, ctx, reroute_kind, node_request, attempt, result) do
    maybe_sleep(executor.policy.retry)

    case reroute_request(ctx, reroute_kind, node_request, attempt + 1) do
      {:ok, %NodeRequest{} = next_request} ->
        attempt_loop(executor, command, ctx, next_request, attempt + 1, result)

      {:error, %Error{}} = err ->
        %NodeResult{request: node_request, result: err, attempt: attempt}

      {:error, reason} when is_atom(reason) ->
        %NodeResult{request: node_request, result: {:error, reason}, attempt: attempt}
    end
  end

  defp reroute_request(ctx, reroute_kind, %NodeRequest{} = node_request, next_attempt) do
    reroute_request = Map.get(ctx, :reroute_request, &default_reroute_request/3)
    reroute_request.(reroute_kind, node_request, next_attempt)
  end

  defp default_reroute_request(_reroute_kind, %NodeRequest{} = node_request, _next_attempt) do
    {:ok, node_request}
  end

  defp maybe_sleep(%{sleep_between_retries_ms: 0}), do: :ok

  defp maybe_sleep(%{sleep_between_retries_ms: ms}) when is_integer(ms) and ms > 0 do
    Process.sleep(ms)
  end

  defp exhausted(nil, reason) do
    {:error,
     %Error{
       code: :timeout,
       message: "Aerospike batch retry budget exhausted (#{reason}) with no attempts succeeding"
     }}
  end

  defp exhausted(last_error, _reason), do: last_error

  defp budget_exhausted?(executor), do: remaining_budget(executor) <= 0

  defp validate_max_concurrency!(value) when is_integer(value) and value > 0, do: value

  defp validate_max_concurrency!(value) do
    raise ArgumentError,
          "expected batch executor max_concurrency to be a positive integer, got: #{inspect(value)}"
  end

  defp monotonic_now, do: System.monotonic_time(:millisecond)
end
