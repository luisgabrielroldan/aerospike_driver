defmodule Aerospike.Runtime.Executor do
  @moduledoc """
  Internal per-attempt runtime shared by unary and grouped batch dispatch.

  This module owns the retry lifecycle and node-scoped operational flow:
  deadline budgeting, retry exhaustion, retry sleep, retry classification,
  rebalance hooks, retry telemetry, node-handle resolution, breaker checks,
  pool checkout, and transport handoff.
  """

  alias Aerospike.Cluster.CircuitBreaker
  alias Aerospike.Error
  alias Aerospike.Cluster.NodePool
  alias Aerospike.Policy
  alias Aerospike.RetryPolicy
  alias Aerospike.Telemetry
  alias Aerospike.Cluster.Tender
  alias Aerospike.RuntimeMetrics

  defmodule Outcome do
    @moduledoc false

    @enforce_keys [:unit, :result, :attempt]
    defstruct [:unit, :node_name, :result, :attempt]

    @type t :: %__MODULE__{
            unit: term(),
            node_name: String.t() | nil,
            result: term(),
            attempt: non_neg_integer()
          }
  end

  defmodule Progress do
    @moduledoc false

    @enforce_keys [:units, :outcomes]
    defstruct [:units, :outcomes]

    @type t :: %__MODULE__{
            units: [term()],
            outcomes: [Outcome.t()]
          }
  end

  @enforce_keys [:policy, :deadline, :on_rebalance]
  defstruct [:policy, :deadline, :on_rebalance]

  @type policy ::
          Policy.UnaryRead.t() | Policy.UnaryWrite.t() | Policy.Batch.t() | Policy.BatchRead.t()

  @type on_rebalance_fun :: (-> :ok)
  @type route_unit_fun ::
          (unit :: term(), non_neg_integer() ->
             {:ok, String.t()} | {:error, term()})
  @type resolve_handle_fun ::
          (GenServer.server(), String.t() ->
             {:ok, Tender.node_handle()} | {:error, :unknown_node})
  @type allow_dispatch_fun :: (term(), term() -> :ok | {:error, Error.t()})
  @type checkout_fun ::
          (String.t(), pid(), (term() -> {term(), NodePool.checkin_value()}), integer() -> term())
  @type transport_fun ::
          (unit :: term(),
           node_name :: String.t(),
           transport :: module(),
           conn :: term(),
           deadline_ms :: non_neg_integer(),
           command_opts :: keyword() ->
             {term(), NodePool.checkin_value()})
  @type progress_retry_fun ::
          (kind :: :transport | :rebalance,
           unit :: term(),
           next_attempt :: non_neg_integer(),
           last_result :: term() ->
             {:ok, Progress.t()} | {:error, Error.t() | atom()})

  @type callbacks :: %{
          required(:route_unit) => route_unit_fun(),
          required(:run_transport) => transport_fun(),
          required(:progress_retry) => progress_retry_fun(),
          optional(:resolve_handle) => resolve_handle_fun(),
          optional(:allow_dispatch) => allow_dispatch_fun(),
          optional(:checkout) => checkout_fun()
        }

  @type dispatch_ctx :: %{
          required(:tender) => GenServer.server(),
          required(:transport) => module(),
          optional(:resolve_handle) => resolve_handle_fun(),
          optional(:allow_dispatch) => allow_dispatch_fun(),
          optional(:checkout) => checkout_fun(),
          optional(:metrics_cluster) => atom() | pid()
        }

  @type attempt_result :: {node_name :: String.t() | nil, result :: term()}
  @type attempt_fun :: (t(), non_neg_integer() -> attempt_result())

  @type t :: %__MODULE__{
          policy: policy(),
          deadline: integer(),
          on_rebalance: on_rebalance_fun()
        }

  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    policy = Keyword.fetch!(opts, :policy)
    on_rebalance = Keyword.get(opts, :on_rebalance, fn -> :ok end)

    %__MODULE__{
      policy: policy,
      deadline: monotonic_now() + policy.timeout,
      on_rebalance: on_rebalance
    }
  end

  @spec remaining_budget(t()) :: integer()
  def remaining_budget(%__MODULE__{deadline: deadline}), do: deadline - monotonic_now()

  @spec run(t(), attempt_fun()) :: term()
  def run(%__MODULE__{} = executor, attempt_fun) when is_function(attempt_fun, 2) do
    attempt_fun_loop(executor, attempt_fun, 0, nil)
  end

  @spec run_unit(t(), term(), dispatch_ctx(), callbacks()) :: [Outcome.t()]
  def run_unit(%__MODULE__{} = executor, unit, ctx, callbacks)
      when is_map(ctx) and is_map(callbacks) do
    attempt_loop(executor, unit, ctx, callbacks, 0, nil)
  end

  @spec progress([term()], [Outcome.t()]) :: Progress.t()
  def progress(units, outcomes \\ []) when is_list(units) and is_list(outcomes) do
    %Progress{units: units, outcomes: outcomes}
  end

  defp attempt_fun_loop(executor, _attempt_fun, attempt, last_error)
       when attempt > executor.policy.retry.max_retries do
    exhausted(last_error, :max_retries)
  end

  defp attempt_fun_loop(executor, attempt_fun, attempt, last_error) do
    if remaining_budget(executor) <= 0 do
      exhausted(last_error, :deadline)
    else
      {node_name, result} = attempt_fun.(executor, attempt)
      handle_attempt_fun_result(executor, attempt_fun, node_name, attempt, result)
    end
  end

  defp handle_attempt_fun_result(
         _executor,
         _attempt_fun,
         _node_name,
         _attempt,
         {:no_retry, inner_result}
       ) do
    inner_result
  end

  defp handle_attempt_fun_result(executor, attempt_fun, node_name, attempt, result) do
    case RetryPolicy.classify(result) do
      %{bucket: :ok} ->
        result

      %{bucket: :routing_refusal} ->
        result

      %{bucket: :server_fatal} ->
        result

      %{bucket: :transport, retry_classification: classification} ->
        retry_attempt_fun(executor, attempt_fun, node_name, attempt, result, classification)

      %{bucket: :rebalance, retry_classification: classification} ->
        executor.on_rebalance.()
        retry_attempt_fun(executor, attempt_fun, node_name, attempt, result, classification)
    end
  end

  defp retry_attempt_fun(executor, attempt_fun, node_name, attempt, result, classification) do
    maybe_sleep(executor.policy.retry)
    next_attempt = attempt + 1

    Telemetry.emit_retry_attempt(
      node_name,
      next_attempt,
      classification,
      remaining_budget(executor)
    )

    attempt_fun_loop(executor, attempt_fun, next_attempt, result)
  end

  defp attempt_loop(executor, unit, _ctx, _callbacks, attempt, last_error)
       when attempt > executor.policy.retry.max_retries do
    [outcome(unit, nil, max(attempt - 1, 0), exhausted(last_error, :max_retries))]
  end

  defp attempt_loop(executor, unit, ctx, callbacks, attempt, last_error) do
    if remaining_budget(executor) <= 0 do
      [outcome(unit, nil, max(attempt - 1, 0), exhausted(last_error, :deadline))]
    else
      {node_name, result} = dispatch_attempt(executor, unit, ctx, callbacks, attempt)

      handle_classification(
        executor,
        unit,
        ctx,
        callbacks,
        node_name,
        attempt,
        result,
        last_error
      )
    end
  end

  defp dispatch_attempt(executor, unit, ctx, callbacks, attempt) do
    case callbacks.route_unit.(unit, attempt) do
      {:ok, node_name} ->
        {node_name, dispatch_node(executor, unit, ctx, callbacks, node_name, attempt)}

      {:error, reason} ->
        {nil, reason}
    end
  end

  defp dispatch_node(executor, unit, ctx, callbacks, node_name, attempt) do
    resolve_handle =
      Map.get(callbacks, :resolve_handle, Map.get(ctx, :resolve_handle, &Tender.node_handle/2))

    case resolve_handle.(ctx.tender, node_name) do
      {:ok, handle} ->
        check_breaker(executor, unit, ctx, callbacks, node_name, handle, attempt)

      {:error, :unknown_node} = err ->
        err
    end
  end

  defp check_breaker(executor, unit, ctx, callbacks, node_name, handle, attempt) do
    allow_dispatch =
      Map.get(callbacks, :allow_dispatch, Map.get(ctx, :allow_dispatch, &CircuitBreaker.allow?/2))

    case allow_dispatch.(handle.counters, handle.breaker) do
      :ok ->
        checkout(executor, unit, ctx, callbacks, node_name, handle, attempt)

      {:error, %Error{}} = err ->
        err
    end
  end

  defp checkout(executor, unit, ctx, callbacks, node_name, handle, attempt) do
    checkout = Map.get(callbacks, :checkout, Map.get(ctx, :checkout, &NodePool.checkout!/4))
    remaining = max(remaining_budget(executor), 0)
    command_opts = [use_compression: handle.use_compression, attempt: attempt]

    result =
      checkout.(
        node_name,
        handle.pool,
        fn conn ->
          callbacks.run_transport.(unit, node_name, ctx.transport, conn, remaining, command_opts)
        end,
        remaining
      )

    maybe_record_checkout_failure(ctx, node_name, result)
    result
  end

  defp handle_classification(
         executor,
         unit,
         ctx,
         callbacks,
         node_name,
         attempt,
         result,
         last_error
       ) do
    case result do
      {:no_retry, inner_result} ->
        [outcome(unit, node_name, attempt, inner_result)]

      _ ->
        handle_retryable_result(
          executor,
          unit,
          ctx,
          callbacks,
          node_name,
          attempt,
          result,
          last_error
        )
    end
  end

  defp handle_retryable_result(
         executor,
         unit,
         ctx,
         callbacks,
         node_name,
         attempt,
         result,
         last_error
       ) do
    case RetryPolicy.classify(result) do
      %{bucket: :ok} ->
        [outcome(unit, node_name, attempt, result)]

      %{bucket: :routing_refusal} ->
        [outcome(unit, node_name, attempt, result)]

      %{bucket: :server_fatal} ->
        [outcome(unit, node_name, attempt, result)]

      %{bucket: :transport, retry_classification: classification} ->
        retry_after_error(
          executor,
          unit,
          ctx,
          callbacks,
          node_name,
          attempt,
          result,
          last_error,
          :transport,
          classification
        )

      %{bucket: :rebalance, retry_classification: classification} ->
        executor.on_rebalance.()

        retry_after_error(
          executor,
          unit,
          ctx,
          callbacks,
          node_name,
          attempt,
          result,
          last_error,
          :rebalance,
          classification
        )
    end
  end

  defp retry_after_error(
         executor,
         unit,
         ctx,
         callbacks,
         node_name,
         attempt,
         result,
         _last_error,
         reroute_kind,
         classification
       ) do
    maybe_sleep(executor.policy.retry)
    next_attempt = attempt + 1

    case callbacks.progress_retry.(reroute_kind, unit, next_attempt, result) do
      {:ok, %Progress{} = progress} ->
        Telemetry.emit_retry_attempt(
          node_name,
          next_attempt,
          classification,
          remaining_budget(executor)
        )

        maybe_record_retry_attempt(ctx, classification)

        progress.outcomes ++
          Enum.flat_map(progress.units, fn next_unit ->
            attempt_loop(executor, next_unit, ctx, callbacks, next_attempt, result)
          end)

      {:error, %Error{} = err} ->
        [outcome(unit, node_name, attempt, {:error, err})]

      {:error, reason} when is_atom(reason) ->
        [outcome(unit, node_name, attempt, {:error, reason})]
    end
  end

  defp outcome(unit, node_name, attempt, result) do
    %Outcome{unit: unit, node_name: node_name, attempt: attempt, result: result}
  end

  defp maybe_record_checkout_failure(
         %{metrics_cluster: cluster},
         node_name,
         {:error, %Error{code: :pool_timeout}}
       ) do
    RuntimeMetrics.record_checkout_failure(cluster, node_name, :pool_timeout)
  end

  defp maybe_record_checkout_failure(
         %{metrics_cluster: cluster},
         node_name,
         {:error, %Error{code: :network_error}}
       ) do
    RuntimeMetrics.record_checkout_failure(cluster, node_name, :network_error)
  end

  defp maybe_record_checkout_failure(_ctx, _node_name, _result), do: :ok

  defp maybe_record_retry_attempt(%{metrics_cluster: cluster}, classification) do
    RuntimeMetrics.record_retry_attempt(cluster, classification)
  end

  defp maybe_record_retry_attempt(_ctx, _classification), do: :ok

  defp maybe_sleep(%{sleep_between_retries_ms: 0}), do: :ok

  defp maybe_sleep(%{sleep_between_retries_ms: ms}) when is_integer(ms) and ms > 0 do
    Process.sleep(ms)
  end

  defp exhausted(nil, reason) do
    {:error,
     %Error{
       code: :timeout,
       message: "Aerospike retry budget exhausted (#{reason}) with no attempts succeeding"
     }}
  end

  defp exhausted(last_error, _reason), do: last_error

  defp monotonic_now, do: System.monotonic_time(:millisecond)
end
