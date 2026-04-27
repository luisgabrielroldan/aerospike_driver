defmodule Aerospike.Runtime.ExecutorTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Policy
  alias Aerospike.Runtime.Executor
  alias Aerospike.Telemetry

  describe "run/2" do
    test "preserves retry telemetry semantics for the shared retry loop" do
      executor = executor_for(max_retries: 1)
      handler = attach_retry_handler(:shared_run_retry)

      try do
        assert {:ok, :done} =
                 Executor.run(executor, fn _executor, attempt ->
                   case attempt do
                     0 -> {"A1", {:error, network_error("boom")}}
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
  end

  describe "run_unit/4" do
    test "rebalance retries can split one failed unit into multiple replacement units" do
      parent = self()
      handler = attach_retry_handler(:shared_unit_retry)
      unit_a = %{node_name: "A1", request: "req-a1"}
      unit_b = %{node_name: "B1", request: "req-b1"}
      unit_c = %{node_name: "C1", request: "req-c1"}

      responses =
        start_responses!(%{
          "A1" => [{:error, Error.from_result_code(:partition_unavailable)}],
          "B1" => [{:ok, :done_b}],
          "C1" => [{:ok, :done_c}]
        })

      executor =
        executor_for(
          max_retries: 1,
          on_rebalance: fn ->
            send(parent, :rebalance_triggered)
            :ok
          end
        )

      callbacks = %{
        route_unit: fn %{node_name: node_name}, _attempt -> {:ok, node_name} end,
        run_transport: fn unit, _node_name, _transport, conn, timeout, command_opts ->
          send(parent, {:transport, unit.node_name, timeout, command_opts})
          result = next_response(conn)
          {result, conn}
        end,
        progress_retry: fn
          :rebalance, %{node_name: "A1"}, 1, _last_result ->
            {:ok, Executor.progress([unit_b, unit_c])}

          _kind, unit, _next_attempt, _last_result ->
            {:ok, Executor.progress([unit])}
        end,
        resolve_handle: fn :fake_tender, node_name ->
          {:ok,
           %{
             pool: {responses, node_name},
             counters: :counters,
             breaker: :breaker,
             use_compression: false
           }}
        end,
        allow_dispatch: fn _counters, _breaker -> :ok end,
        checkout: fn _node_name, pool, fun, _timeout ->
          elem(fun.(pool), 0)
        end
      }

      try do
        outcomes =
          Executor.run_unit(
            executor,
            unit_a,
            %{tender: :fake_tender, transport: __MODULE__.TransportStub},
            callbacks
          )

        assert [
                 %Executor.Outcome{unit: ^unit_b, attempt: 1, result: {:ok, :done_b}},
                 %Executor.Outcome{unit: ^unit_c, attempt: 1, result: {:ok, :done_c}}
               ] = outcomes

        assert_receive :rebalance_triggered, 500
        assert_receive {:transport, "A1", timeout_a, [use_compression: false, attempt: 0]}
        assert is_integer(timeout_a) and timeout_a >= 0
        assert_receive {:transport, "B1", timeout_b, [use_compression: false, attempt: 1]}
        assert is_integer(timeout_b) and timeout_b >= 0
        assert_receive {:transport, "C1", timeout_c, [use_compression: false, attempt: 1]}
        assert is_integer(timeout_c) and timeout_c >= 0

        assert_receive {:event, [:aerospike, :retry, :attempt], _measurements,
                        %{classification: :rebalance, attempt: 1, node_name: "A1"}},
                       500
      after
        :telemetry.detach(handler)
      end
    end

    test "terminal no-retry results stop at the shared runtime boundary" do
      executor = executor_for(max_retries: 1)

      callbacks = %{
        route_unit: fn :unit, _attempt -> {:ok, "A1"} end,
        run_transport: fn :unit, _node_name, _transport, _conn, _remaining, _command_opts ->
          {{:no_retry, {:error, network_error("boom")}}, :conn}
        end,
        progress_retry: fn _kind, unit, _next_attempt, _last_result ->
          flunk("no-retry results must not ask for retry progression: #{inspect(unit)}")
        end,
        resolve_handle: fn :fake_tender, "A1" ->
          {:ok, %{pool: :pool, counters: :counters, breaker: :breaker, use_compression: false}}
        end,
        allow_dispatch: fn _counters, _breaker -> :ok end,
        checkout: fn _node_name, _pool, fun, _timeout -> elem(fun.(:conn), 0) end
      }

      assert [%Executor.Outcome{result: {:error, %Error{code: :network_error, message: "boom"}}}] =
               Executor.run_unit(
                 executor,
                 :unit,
                 %{tender: :fake_tender, transport: __MODULE__.TransportStub},
                 callbacks
               )
    end

    test "defaults transport timeout to the remaining total budget" do
      parent = self()
      executor = executor_for(timeout: 1_000)

      callbacks = callbacks_for_timeout_probe(parent)

      assert [%Executor.Outcome{result: {:ok, :done}}] =
               Executor.run_unit(
                 executor,
                 :unit,
                 %{tender: :fake_tender, transport: __MODULE__.TransportStub},
                 callbacks
               )

      assert_receive {:checkout_timeout, checkout_timeout}
      assert_receive {:transport_timeout, transport_timeout}
      assert transport_timeout == checkout_timeout
    end

    test "passes socket timeout to transport without extending the remaining total budget" do
      parent = self()
      executor = executor_for(timeout: 1_000, socket_timeout: 25)

      callbacks = callbacks_for_timeout_probe(parent)

      assert [%Executor.Outcome{result: {:ok, :done}}] =
               Executor.run_unit(
                 executor,
                 :unit,
                 %{tender: :fake_tender, transport: __MODULE__.TransportStub},
                 callbacks
               )

      assert_receive {:checkout_timeout, checkout_timeout}
      assert_receive {:transport_timeout, 25}
      assert is_integer(checkout_timeout) and checkout_timeout >= 25
    end

    test "caps socket timeout at the remaining total budget" do
      parent = self()
      executor = executor_for(timeout: 50, socket_timeout: 1_000)

      callbacks = callbacks_for_timeout_probe(parent)

      assert [%Executor.Outcome{result: {:ok, :done}}] =
               Executor.run_unit(
                 executor,
                 :unit,
                 %{tender: :fake_tender, transport: __MODULE__.TransportStub},
                 callbacks
               )

      assert_receive {:checkout_timeout, checkout_timeout}
      assert_receive {:transport_timeout, transport_timeout}
      assert transport_timeout == checkout_timeout
      assert transport_timeout <= 50
    end
  end

  defp executor_for(opts) do
    max_retries = Keyword.get(opts, :max_retries, 0)
    timeout = Keyword.get(opts, :timeout, 5_000)
    on_rebalance = Keyword.get(opts, :on_rebalance, fn -> :ok end)

    Executor.new!(
      policy: %Policy.UnaryRead{
        timeout: timeout,
        socket_timeout: Keyword.get(opts, :socket_timeout, 0),
        filter: nil,
        read_mode_ap: :one,
        read_mode_sc: :session,
        read_touch_ttl_percent: 0,
        send_key: false,
        use_compression: nil,
        retry: %{max_retries: max_retries, sleep_between_retries_ms: 0, replica_policy: :sequence}
      },
      on_rebalance: on_rebalance
    )
  end

  defp start_responses!(responses_by_node) do
    start_supervised!({Agent, fn -> responses_by_node end})
  end

  defp next_response({responses, node_name}) do
    Agent.get_and_update(responses, fn state ->
      case Map.get(state, node_name, []) do
        [next | rest] -> {next, Map.put(state, node_name, rest)}
        [] -> {{:error, network_error("missing script")}, state}
      end
    end)
  end

  defp attach_retry_handler(tag) do
    parent = self()
    handler_id = "executor-retry-#{tag}-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        Telemetry.retry_attempt(),
        fn event, measurements, metadata, _config ->
          send(parent, {:event, event, measurements, metadata})
        end,
        nil
      )

    handler_id
  end

  defp network_error(message) do
    Error.from_result_code(:network_error, message: message)
  end

  defp callbacks_for_timeout_probe(parent) do
    %{
      route_unit: fn :unit, _attempt -> {:ok, "A1"} end,
      run_transport: fn :unit, _node_name, _transport, conn, timeout, _command_opts ->
        send(parent, {:transport_timeout, timeout})
        {{:ok, :done}, conn}
      end,
      progress_retry: fn _kind, unit, _next_attempt, _last_result ->
        {:ok, Executor.progress([unit])}
      end,
      resolve_handle: fn :fake_tender, "A1" ->
        {:ok, %{pool: :pool, counters: :counters, breaker: :breaker, use_compression: false}}
      end,
      allow_dispatch: fn _counters, _breaker -> :ok end,
      checkout: fn _node_name, _pool, fun, timeout ->
        send(parent, {:checkout_timeout, timeout})
        elem(fun.(:conn), 0)
      end
    }
  end

  defmodule TransportStub do
  end
end
