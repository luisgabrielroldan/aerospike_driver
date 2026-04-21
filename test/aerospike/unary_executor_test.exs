defmodule Aerospike.UnaryExecutorTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Telemetry
  alias Aerospike.UnaryExecutor

  describe "new!/1" do
    test "merges per-call retry options over the base policy" do
      executor =
        UnaryExecutor.new!(
          base_policy: %{max_retries: 5, sleep_between_retries_ms: 10, replica_policy: :master},
          command_opts: [max_retries: 1, replica_policy: :sequence]
        )

      assert executor.policy == %{
               max_retries: 1,
               sleep_between_retries_ms: 10,
               replica_policy: :sequence
             }
    end
  end

  describe "run/2" do
    test "returns the most recent error when retries are exhausted" do
      executor = executor_for(max_retries: 1)
      errors = [network_error("first"), timeout_error("second")]

      assert {:error, %Error{code: :timeout, message: "second"}} =
               UnaryExecutor.run(executor, fn _executor, attempt ->
                 {"A#{attempt}", {:error, Enum.at(errors, attempt)}}
               end)
    end

    test "returns the most recent error when the deadline expires between attempts" do
      executor =
        executor_for(
          max_retries: 3,
          sleep_between_retries_ms: 25,
          timeout: 5
        )

      parent = self()

      assert {:error, %Error{code: :network_error, message: "budget"}} =
               UnaryExecutor.run(executor, fn _executor, attempt ->
                 send(parent, {:attempt, attempt})
                 {"A#{attempt}", {:error, network_error("budget")}}
               end)

      assert_receive {:attempt, 0}
      refute_receive {:attempt, 1}
    end

    test "emits transport retry telemetry with the next attempt index" do
      executor = executor_for(max_retries: 1)
      handler = attach_retry_handler(:transport_retry)

      try do
        assert {:ok, :done} =
                 UnaryExecutor.run(executor, fn _executor, attempt ->
                   case attempt do
                     0 -> {"A1", {:error, network_error("fake")}}
                     1 -> {"B1", {:ok, :done}}
                   end
                 end)

        assert_receive {:event, [:aerospike, :retry, :attempt], %{remaining_budget_ms: budget},
                        %{classification: :transport, attempt: 1, node_name: "A1"}},
                       500

        assert is_integer(budget) and budget >= 0
      after
        :telemetry.detach(handler)
      end
    end

    test "rebalance retries invoke the rebalance hook and emit rebalance telemetry" do
      parent = self()

      executor =
        executor_for(
          max_retries: 1,
          on_rebalance: fn ->
            send(parent, :rebalance_triggered)
            :ok
          end
        )

      handler = attach_retry_handler(:rebalance_retry)

      try do
        assert {:ok, :done} =
                 UnaryExecutor.run(executor, fn _executor, attempt ->
                   case attempt do
                     0 -> {"A1", {:error, Error.from_result_code(:partition_unavailable)}}
                     1 -> {"B1", {:ok, :done}}
                   end
                 end)

        assert_receive :rebalance_triggered, 500

        assert_receive {:event, [:aerospike, :retry, :attempt], _measurements,
                        %{classification: :rebalance, attempt: 1, node_name: "A1"}},
                       500
      after
        :telemetry.detach(handler)
      end
    end

    test "circuit-open retries keep the specialized telemetry classification" do
      executor = executor_for(max_retries: 1)
      handler = attach_retry_handler(:circuit_retry)

      try do
        assert {:ok, :done} =
                 UnaryExecutor.run(executor, fn _executor, attempt ->
                   case attempt do
                     0 -> {"A1", {:error, Error.from_result_code(:circuit_open)}}
                     1 -> {"B1", {:ok, :done}}
                   end
                 end)

        assert_receive {:event, [:aerospike, :retry, :attempt], _measurements,
                        %{classification: :circuit_open, attempt: 1, node_name: "A1"}},
                       500
      after
        :telemetry.detach(handler)
      end
    end
  end

  @doc false
  def forward(event, measurements, metadata, test_pid) do
    send(test_pid, {:event, event, measurements, metadata})
  end

  defp executor_for(opts) do
    base_policy = %{max_retries: 2, sleep_between_retries_ms: 0, replica_policy: :sequence}

    command_opts =
      Keyword.take(opts, [:max_retries, :sleep_between_retries_ms, :replica_policy, :timeout])

    on_rebalance = Keyword.get(opts, :on_rebalance, fn -> :ok end)

    UnaryExecutor.new!(
      base_policy: base_policy,
      command_opts: command_opts,
      on_rebalance: on_rebalance
    )
  end

  defp attach_retry_handler(tag) do
    handler_id = {__MODULE__, tag, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        Telemetry.retry_attempt(),
        &__MODULE__.forward/4,
        self()
      )

    handler_id
  end

  defp network_error(message), do: Error.from_result_code(:network_error, message: message)
  defp timeout_error(message), do: Error.from_result_code(:timeout, message: message)
end
