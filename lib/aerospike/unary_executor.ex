defmodule Aerospike.UnaryExecutor do
  @moduledoc """
  Internal retry driver for unary commands.

  This module owns the shared per-call setup and retry loop that unary
  commands should not copy: policy merge, monotonic deadline budgeting,
  retry telemetry, classification-driven redispatch, and the unary
  node-dispatch path (`Router` -> `Tender.node_handle/2` ->
  `CircuitBreaker` -> `NodePool.checkout!/4`).

  Command modules supply only their request builder and response parser
  via `Aerospike.UnaryCommand`. The executor owns the operational flow
  around those hooks so future unary commands do not need to copy the
  same routing, breaker, checkout, and retry logic.
  """

  alias Aerospike.CircuitBreaker
  alias Aerospike.Error
  alias Aerospike.NodePool
  alias Aerospike.RetryPolicy
  alias Aerospike.Router
  alias Aerospike.Telemetry
  alias Aerospike.Tender
  alias Aerospike.UnaryCommand

  @default_timeout 5_000

  @enforce_keys [:policy, :deadline, :on_rebalance]
  defstruct [:policy, :deadline, :on_rebalance]

  @type attempt_result :: {node_name :: String.t() | nil, result :: term()}
  @type on_rebalance_fun :: (-> :ok)
  @type attempt_fun :: (t(), non_neg_integer() -> attempt_result())
  @type pick_node_fun ::
          (dispatch :: UnaryCommand.dispatch_kind(),
           tables :: term(),
           route_key :: term(),
           RetryPolicy.replica_policy(),
           non_neg_integer() ->
             {:ok, String.t()} | {:error, term()})
  @type resolve_handle_fun ::
          (GenServer.server(), String.t() ->
             {:ok, Tender.node_handle()} | {:error, :unknown_node})
  @type allow_dispatch_fun :: (term(), term() -> :ok | {:error, Error.t()})
  @type checkout_fun ::
          (String.t(), pid(), (term() -> {term(), NodePool.checkin_value()}), integer() -> term())
  @type dispatch_ctx :: %{
          required(:tables) => term(),
          required(:tender) => GenServer.server(),
          required(:transport) => module(),
          required(:route_key) => term(),
          required(:command_input) => term(),
          optional(:pick_node) => pick_node_fun(),
          optional(:resolve_handle) => resolve_handle_fun(),
          optional(:allow_dispatch) => allow_dispatch_fun(),
          optional(:checkout) => checkout_fun()
        }

  @type t :: %__MODULE__{
          policy: RetryPolicy.t(),
          deadline: integer(),
          on_rebalance: on_rebalance_fun()
        }

  @doc """
  Builds a retry executor for one unary command call.
  """
  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    base_policy = Keyword.fetch!(opts, :base_policy)
    command_opts = Keyword.get(opts, :command_opts, [])
    timeout = Keyword.get(command_opts, :timeout, @default_timeout)
    on_rebalance = Keyword.get(opts, :on_rebalance, fn -> :ok end)

    %__MODULE__{
      policy: RetryPolicy.merge(base_policy, command_opts),
      deadline: monotonic_now() + timeout,
      on_rebalance: on_rebalance
    }
  end

  @doc """
  Runs the retry loop for one unary command call.
  """
  @spec run(t(), attempt_fun()) :: term()
  def run(%__MODULE__{} = executor, attempt_fun) when is_function(attempt_fun, 2) do
    attempt_loop(executor, attempt_fun, 0, nil)
  end

  @doc """
  Runs one unary command through the shared routing, breaker, checkout,
  transport, and retry pipeline.
  """
  @spec run_command(t(), UnaryCommand.t(), dispatch_ctx()) :: term()
  def run_command(%__MODULE__{} = executor, %UnaryCommand{} = command, ctx) when is_map(ctx) do
    run(executor, fn retry_ctx, attempt ->
      dispatch_attempt(retry_ctx, attempt, command, ctx)
    end)
  end

  @doc """
  Returns the remaining monotonic budget in milliseconds.
  """
  @spec remaining_budget(t()) :: integer()
  def remaining_budget(%__MODULE__{deadline: deadline}), do: deadline - monotonic_now()

  defp dispatch_attempt(retry_ctx, attempt, command, ctx) do
    pick_node = Map.get(ctx, :pick_node, &pick_node/5)
    dispatch = UnaryCommand.dispatch_kind(command)

    case pick_node.(dispatch, ctx.tables, ctx.route_key, retry_ctx.policy.replica_policy, attempt) do
      {:ok, node_name} ->
        {node_name, dispatch_node(retry_ctx, attempt, command, ctx, node_name)}

      {:error, _reason} = routing_error ->
        {nil, routing_error}
    end
  end

  defp dispatch_node(retry_ctx, attempt, command, ctx, node_name) do
    resolve_handle = Map.get(ctx, :resolve_handle, &Tender.node_handle/2)

    case resolve_handle.(ctx.tender, node_name) do
      {:ok, handle} ->
        check_breaker(retry_ctx, attempt, command, ctx, node_name, handle)

      {:error, :unknown_node} = err ->
        err
    end
  end

  defp check_breaker(retry_ctx, attempt, command, ctx, node_name, handle) do
    allow_dispatch = Map.get(ctx, :allow_dispatch, &CircuitBreaker.allow?/2)

    case allow_dispatch.(handle.counters, handle.breaker) do
      :ok ->
        checkout(retry_ctx, attempt, command, ctx, node_name, handle)

      {:error, %Error{}} = err ->
        err
    end
  end

  defp checkout(retry_ctx, attempt, command, ctx, node_name, handle) do
    checkout = Map.get(ctx, :checkout, &NodePool.checkout!/4)
    remaining = remaining_budget(retry_ctx)
    command_opts = [use_compression: handle.use_compression, attempt: attempt]

    checkout.(
      node_name,
      handle.pool,
      fn conn ->
        UnaryCommand.run_transport(
          command,
          ctx.transport,
          conn,
          ctx.command_input,
          remaining,
          command_opts
        )
      end,
      remaining
    )
  end

  defp pick_node(:read, tables, route_key, replica_policy, attempt) do
    Router.pick_for_read(tables, route_key, replica_policy, attempt)
  end

  defp pick_node(:write, tables, route_key, _replica_policy, _attempt) do
    Router.pick_for_write(tables, route_key)
  end

  defp attempt_loop(executor, attempt_fun, attempt, last_error) do
    cond do
      attempt > executor.policy.max_retries ->
        exhausted(last_error, :max_retries)

      budget_exhausted?(executor) ->
        exhausted(last_error, :deadline)

      true ->
        {node_name, result} = attempt_fun.(executor, attempt)
        handle_classification(executor, attempt_fun, node_name, attempt, result, last_error)
    end
  end

  defp handle_classification(executor, attempt_fun, node_name, attempt, result, last_error) do
    case result do
      {:no_retry, inner_result} ->
        inner_result

      _ ->
        handle_retryable_result(executor, attempt_fun, node_name, attempt, result, last_error)
    end
  end

  defp handle_retryable_result(executor, attempt_fun, node_name, attempt, result, last_error) do
    case RetryPolicy.classify(result) do
      %{bucket: :ok} ->
        result

      %{bucket: :rebalance, retry_classification: classification} ->
        executor.on_rebalance.()
        retry_after_error(executor, attempt_fun, node_name, attempt, result, classification)

      %{bucket: :transport, retry_classification: classification} ->
        retry_after_error(executor, attempt_fun, node_name, attempt, result, classification)

      %{bucket: :routing_refusal} ->
        fatal(last_error, result)

      %{bucket: :server_fatal} ->
        result
    end
  end

  defp retry_after_error(executor, attempt_fun, node_name, attempt, err, classification) do
    maybe_sleep(executor.policy)
    next_attempt = attempt + 1

    emit_retry_event(executor, node_name, next_attempt, classification)

    attempt_loop(executor, attempt_fun, next_attempt, err)
  end

  defp emit_retry_event(executor, node_name, next_attempt, classification) do
    :telemetry.execute(
      Telemetry.retry_attempt(),
      %{remaining_budget_ms: max(remaining_budget(executor), 0)},
      %{
        classification: classification,
        attempt: next_attempt,
        node_name: node_name
      }
    )
  end

  defp maybe_sleep(%{sleep_between_retries_ms: 0}), do: :ok

  defp maybe_sleep(%{sleep_between_retries_ms: ms}) when is_integer(ms) and ms > 0 do
    Process.sleep(ms)
  end

  defp exhausted(nil, reason) do
    {:error,
     %Error{
       code: :timeout,
       message: "Aerospike unary retry budget exhausted (#{reason}) with no attempts succeeding"
     }}
  end

  defp exhausted(last_error, _reason), do: last_error

  defp fatal(_last_error, routing_error), do: routing_error

  defp budget_exhausted?(executor), do: remaining_budget(executor) <= 0

  defp monotonic_now, do: System.monotonic_time(:millisecond)
end
