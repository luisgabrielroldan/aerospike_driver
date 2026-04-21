defmodule Aerospike.UnaryExecutor do
  @moduledoc """
  Internal retry driver for unary commands.

  This module owns the shared per-call setup and retry loop that unary
  commands should not copy: policy merge, monotonic deadline budgeting,
  retry telemetry, and classification-driven redispatch.

  Command modules supply only the per-attempt dispatch callback. That
  callback can keep command-local routing or transport details for now
  while the retry driver remains shared.
  """

  alias Aerospike.Error
  alias Aerospike.RetryPolicy
  alias Aerospike.Telemetry

  @default_timeout 5_000

  @enforce_keys [:policy, :deadline, :on_rebalance]
  defstruct [:policy, :deadline, :on_rebalance]

  @type attempt_result :: {node_name :: String.t() | nil, result :: term()}
  @type on_rebalance_fun :: (-> :ok)
  @type attempt_fun :: (t(), non_neg_integer() -> attempt_result())

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
  Returns the remaining monotonic budget in milliseconds.
  """
  @spec remaining_budget(t()) :: integer()
  def remaining_budget(%__MODULE__{deadline: deadline}), do: deadline - monotonic_now()

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
