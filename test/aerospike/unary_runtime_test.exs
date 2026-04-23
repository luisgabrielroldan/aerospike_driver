defmodule Aerospike.UnaryRuntimeTest do
  use ExUnit.Case, async: true

  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Error
  alias Aerospike.Policy
  alias Aerospike.Runtime.Executor
  alias Aerospike.Telemetry

  describe "Executor.run/2" do
    test "accepts a validated unary policy struct" do
      executor =
        Executor.new!(
          policy: %Policy.UnaryRead{
            timeout: 5_000,
            retry: %{max_retries: 1, sleep_between_retries_ms: 10, replica_policy: :sequence}
          }
        )

      assert executor.policy == %Policy.UnaryRead{
               timeout: 5_000,
               retry: %{max_retries: 1, sleep_between_retries_ms: 10, replica_policy: :sequence}
             }
    end

    test "returns the most recent error when retries are exhausted" do
      executor = executor_for(max_retries: 1)
      errors = [network_error("first"), timeout_error("second")]

      assert {:error, %Error{code: :timeout, message: "second"}} =
               Executor.run(executor, fn _executor, attempt ->
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
               Executor.run(executor, fn _executor, attempt ->
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
                 Executor.run(executor, fn _executor, attempt ->
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
                 Executor.run(executor, fn _executor, attempt ->
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
                 Executor.run(executor, fn _executor, attempt ->
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

  describe "UnaryCommand.run/3" do
    test "keeps the unary fast path to one pick, resolve, breaker, checkout, and transport" do
      executor = executor_for(max_retries: 1)
      command = test_command()
      parent = self()
      __MODULE__.TransportStub.put_results([{:ok, {:ok, :done}}])
      __MODULE__.TransportStub.notify(parent)
      on_exit(fn -> __MODULE__.TransportStub.notify(nil) end)

      assert {:ok, :done} =
               UnaryCommand.run(command, executor, %{
                 tables: :fake_tables,
                 tender: :fake_tender,
                 transport: __MODULE__.TransportStub,
                 route_key: :route_key,
                 command_input: :payload,
                 pick_node: fn :read, :fake_tables, :route_key, :sequence, 0 ->
                   send(parent, :picked)
                   {:ok, "A1"}
                 end,
                 resolve_handle: fn :fake_tender, "A1" ->
                   send(parent, :resolved)
                   {:ok, fake_handle()}
                 end,
                 allow_dispatch: fn _counters, _breaker ->
                   send(parent, :allowed)
                   :ok
                 end,
                 checkout: fn _node_name, _pool, fun, _timeout ->
                   send(parent, :checked_out)
                   elem(fun.(:conn), 0)
                 end
               })

      assert_receive :picked
      assert_receive :resolved
      assert_receive :allowed
      assert_receive :checked_out
      assert_receive {:transport, :request, [use_compression: false, attempt: 0]}
      refute_receive {:transport, _, [use_compression: false, attempt: 1]}
    end

    test "returns breaker refusal without checking out a worker" do
      executor = executor_for(max_retries: 0)
      command = test_command()

      assert {:error, %Error{code: :circuit_open}} =
               UnaryCommand.run(command, executor, %{
                 tables: :fake_tables,
                 tender: :fake_tender,
                 transport: TransportStub,
                 route_key: :route_key,
                 command_input: :payload,
                 pick_node: fn :read, :fake_tables, :route_key, :sequence, 0 -> {:ok, "A1"} end,
                 resolve_handle: fn :fake_tender, "A1" -> {:ok, fake_handle()} end,
                 allow_dispatch: fn _counters, _breaker ->
                   {:error, Error.from_result_code(:circuit_open)}
                 end,
                 checkout: fn _node_name, _pool, _fun, _timeout ->
                   flunk("breaker refusal must not check out a worker")
                 end
               })
    end

    test "pool checkout failure retries through the shared dispatch path" do
      executor = executor_for(max_retries: 1)
      command = test_command()
      parent = self()
      __MODULE__.TransportStub.put_results([{:ok, {:ok, :done}}])

      assert {:ok, :done} =
               UnaryCommand.run(command, executor, %{
                 tables: :fake_tables,
                 tender: :fake_tender,
                 transport: __MODULE__.TransportStub,
                 route_key: :route_key,
                 command_input: :payload,
                 pick_node: fn :read, :fake_tables, :route_key, :sequence, attempt ->
                   {:ok, Enum.fetch!(["A1", "B1"], attempt)}
                 end,
                 resolve_handle: fn :fake_tender, node_name ->
                   {:ok,
                    fake_handle(pool: {:pool, node_name}, use_compression: node_name == "B1")}
                 end,
                 allow_dispatch: fn _counters, _breaker -> :ok end,
                 checkout: fn node_name, pool, fun, timeout ->
                   send(parent, {:checkout, node_name, pool, timeout})

                   case node_name do
                     "A1" -> {:error, Error.from_result_code(:pool_timeout)}
                     "B1" -> elem(fun.(:conn_b1), 0)
                   end
                 end
               })

      assert_receive {:checkout, "A1", {:pool, "A1"}, timeout_a}
      assert is_integer(timeout_a)
      assert_receive {:checkout, "B1", {:pool, "B1"}, timeout_b}
      assert is_integer(timeout_b)
    end

    test "commands that opt out do not retry after the transport edge returns an error" do
      executor = executor_for(max_retries: 1)
      command = test_command(retry_transport: false)

      __MODULE__.TransportStub.put_results([
        {:error, network_error("boom")},
        {:ok, {:ok, :done}}
      ])

      assert {:error, %Error{code: :network_error, message: "boom"}} =
               UnaryCommand.run(command, executor, %{
                 tables: :fake_tables,
                 tender: :fake_tender,
                 transport: __MODULE__.TransportStub,
                 route_key: :route_key,
                 command_input: :payload,
                 pick_node: fn :read, :fake_tables, :route_key, :sequence, 0 -> {:ok, "A1"} end,
                 resolve_handle: fn :fake_tender, "A1" -> {:ok, fake_handle()} end,
                 allow_dispatch: fn _counters, _breaker -> :ok end,
                 checkout: fn _node_name, _pool, fun, _timeout ->
                   elem(fun.(:conn), 0)
                 end
               })
    end

    test "transport-class failure closes the worker with node-failure accounting before retry" do
      executor = executor_for(max_retries: 1)
      command = test_command()
      parent = self()

      __MODULE__.TransportStub.put_results([
        {:error, network_error("boom")},
        {:ok, {:ok, :done}}
      ])

      assert {:ok, :done} =
               UnaryCommand.run(command, executor, %{
                 tables: :fake_tables,
                 tender: :fake_tender,
                 transport: __MODULE__.TransportStub,
                 route_key: :route_key,
                 command_input: :payload,
                 pick_node: fn :read, :fake_tables, :route_key, :sequence, attempt ->
                   {:ok, Enum.fetch!(["A1", "B1"], attempt)}
                 end,
                 resolve_handle: fn :fake_tender, node_name ->
                   {:ok, fake_handle(pool: {:pool, node_name})}
                 end,
                 allow_dispatch: fn _counters, _breaker -> :ok end,
                 checkout: fn node_name, _pool, fun, _timeout ->
                   result = fun.({:conn, node_name})
                   send(parent, {:checkin, node_name, elem(result, 1)})
                   elem(result, 0)
                 end
               })

      assert_receive {:checkin, "A1", {:close, :failure}}
      assert_receive {:checkin, "B1", {:conn, "B1"}}
    end

    test "rebalance retry triggers tend hook through the shared dispatch path" do
      parent = self()

      executor =
        executor_for(
          max_retries: 1,
          on_rebalance: fn ->
            send(parent, :rebalance_triggered)
            :ok
          end
        )

      command = test_command()

      __MODULE__.TransportStub.put_results([
        {:error, Error.from_result_code(:partition_unavailable)},
        {:ok, {:ok, :done}}
      ])

      assert {:ok, :done} =
               UnaryCommand.run(command, executor, %{
                 tables: :fake_tables,
                 tender: :fake_tender,
                 transport: __MODULE__.TransportStub,
                 route_key: :route_key,
                 command_input: :payload,
                 pick_node: fn :read, :fake_tables, :route_key, :sequence, attempt ->
                   {:ok, Enum.fetch!(["A1", "B1"], attempt)}
                 end,
                 resolve_handle: fn :fake_tender, node_name ->
                   {:ok, fake_handle(pool: {:pool, node_name})}
                 end,
                 allow_dispatch: fn _counters, _breaker -> :ok end,
                 checkout: fn _node_name, _pool, fun, _timeout ->
                   elem(fun.(:conn), 0)
                 end
               })

      assert_receive :rebalance_triggered, 500
    end

    test "write-routed commands reuse the same retry and checkout path" do
      executor = executor_for(max_retries: 1)
      command = test_command(dispatch: :write)
      parent = self()
      __MODULE__.TransportStub.put_results([{:ok, {:ok, :done}}])

      assert {:ok, :done} =
               UnaryCommand.run(command, executor, %{
                 tables: :fake_tables,
                 tender: :fake_tender,
                 transport: __MODULE__.TransportStub,
                 route_key: :route_key,
                 command_input: :payload,
                 pick_node: fn :write, :fake_tables, :route_key, :sequence, attempt ->
                   send(parent, {:pick_node, attempt})
                   {:ok, Enum.fetch!(["A1", "B1"], attempt)}
                 end,
                 resolve_handle: fn :fake_tender, node_name ->
                   {:ok, fake_handle(pool: {:pool, node_name})}
                 end,
                 allow_dispatch: fn _counters, _breaker -> :ok end,
                 checkout: fn node_name, pool, fun, timeout ->
                   send(parent, {:checkout, node_name, pool, timeout})

                   case node_name do
                     "A1" -> {:error, Error.from_result_code(:pool_timeout)}
                     "B1" -> elem(fun.(:conn_b1), 0)
                   end
                 end
               })

      assert_receive {:pick_node, 0}
      assert_receive {:checkout, "A1", {:pool, "A1"}, timeout_a}
      assert is_integer(timeout_a)
      assert_receive {:pick_node, 1}
      assert_receive {:checkout, "B1", {:pool, "B1"}, timeout_b}
      assert is_integer(timeout_b)
    end
  end

  @doc false
  def forward(event, measurements, metadata, test_pid) do
    send(test_pid, {:event, event, measurements, metadata})
  end

  defp executor_for(opts) do
    on_rebalance = Keyword.get(opts, :on_rebalance, fn -> :ok end)

    Executor.new!(
      policy: %Policy.UnaryRead{
        timeout: Keyword.get(opts, :timeout, 5_000),
        retry: %{
          max_retries: Keyword.get(opts, :max_retries, 2),
          sleep_between_retries_ms: Keyword.get(opts, :sleep_between_retries_ms, 0),
          replica_policy: Keyword.get(opts, :replica_policy, :sequence)
        }
      },
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

  defp test_command(opts \\ []) do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: Keyword.get(opts, :dispatch, :read),
      retry_transport: Keyword.get(opts, :retry_transport, true),
      build_request: fn _ -> :request end,
      parse_response: fn
        {:ok, value}, _input -> {:ok, value}
        {:error, %Error{} = err}, _input -> {:error, err}
      end
    )
  end

  defp fake_handle(opts \\ []) do
    %{
      pool: Keyword.get(opts, :pool, :pool),
      counters: :counters,
      breaker: %{circuit_open_threshold: 2, max_concurrent_ops_per_node: 10},
      use_compression: Keyword.get(opts, :use_compression, false)
    }
  end

  defp network_error(message), do: Error.from_result_code(:network_error, message: message)
  defp timeout_error(message), do: Error.from_result_code(:timeout, message: message)

  defmodule TransportStub do
    alias Aerospike.Error

    def put_results(results), do: Process.put({__MODULE__, :results}, results)
    def notify(pid), do: Process.put({__MODULE__, :notify}, pid)

    def command(_conn, request, _deadline_ms, command_opts) do
      if pid = Process.get({__MODULE__, :notify}) do
        send(pid, {:transport, request, command_opts})
      end

      case Process.get({__MODULE__, :results}, []) do
        [result | rest] ->
          Process.put({__MODULE__, :results}, rest)

          case result do
            {:ok, value} -> {:ok, value}
            {:error, %Error{}} = err -> err
          end

        [] ->
          raise "TransportStub has no scripted results"
      end
    end
  end
end
