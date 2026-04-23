defmodule Aerospike.Command.BatchRuntimeTest do
  use ExUnit.Case, async: true

  alias Aerospike.Command.BatchCommand
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Command.BatchCommand.NodeRequest
  alias Aerospike.Command.BatchCommand.Regroup
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Runtime.Executor
  alias Aerospike.Telemetry

  describe "BatchCommand.run/5" do
    test "hands grouped node requests to a bounded runner and merges by caller order" do
      parent = self()
      responses = start_responses!(%{"A1" => [{:ok, "body-a1"}], "B1" => [{:ok, "body-b1"}]})

      command =
        command(fn results, %{order: order} ->
          by_index =
            results
            |> Enum.flat_map(fn node_result ->
              value =
                case node_result.result do
                  {:ok, body} -> {:ok, node_result.request.node_name, body}
                  {:error, reason} -> {:error, reason}
                end

              Enum.map(node_result.request.entries, fn entry -> {entry.index, value} end)
            end)
            |> Map.new()

          Enum.map(order, &Map.fetch!(by_index, &1))
        end)

      node_requests = [
        node_request("A1", [1]),
        node_request("B1", [0, 2])
      ]

      result =
        BatchCommand.run(
          command,
          runtime_for(max_retries: 0),
          %{order: [0, 1, 2]},
          node_requests,
          ctx(parent, responses,
            max_concurrency: 2,
            task_runner: fn requests, node_fun, opts ->
              send(parent, {:task_runner, opts})
              Enum.map(requests, node_fun)
            end
          )
        )

      assert result == [
               {:ok, "B1", "body-b1"},
               {:ok, "A1", "body-a1"},
               {:ok, "B1", "body-b1"}
             ]

      assert_receive {:task_runner, opts}
      assert opts[:max_concurrency] == 2
      assert is_integer(opts[:timeout]) and opts[:timeout] >= 0

      assert_receive {:transport, "A1", "batch:A1:1", _deadline,
                      [use_compression: false, attempt: 0]}

      assert_receive {:transport, "B1", "batch:B1:0,2", _deadline,
                      [use_compression: false, attempt: 0]}
    end

    test "transport retry reroutes one grouped node request without aborting the batch" do
      parent = self()
      handler = attach_retry_handler(:batch_transport_retry)
      node_a = unique_node_name("A1")
      node_b = unique_node_name("B1")
      node_c = unique_node_name("C1")

      try do
        responses =
          start_responses!(%{
            node_a => [{:error, Error.from_result_code(:network_error, message: "boom-a1")}],
            node_b => [{:ok, "body-b1"}],
            node_c => [{:ok, "body-c1"}]
          })

        command =
          command(fn results, _input ->
            Enum.map(results, fn node_result ->
              {entry_index(node_result.request), node_result.request.node_name,
               node_result.attempt, node_result.result}
            end)
          end)

        result =
          BatchCommand.run(
            command,
            runtime_for(max_retries: 1),
            :batch,
            [node_request(node_a, [0]), node_request(node_c, [1])],
            ctx(parent, responses,
              reroute_request: fn
                :transport, %NodeRequest{} = request, 1 -> {:ok, %{request | node_name: node_b}}
                kind, %NodeRequest{} = request, _attempt -> {:ok, %{request | payload: kind}}
              end
            )
          )

        assert result == [
                 {0, node_b, 1, {:ok, "body-b1"}},
                 {1, node_c, 0, {:ok, "body-c1"}}
               ]

        assert_receive {:event, [:aerospike, :retry, :attempt], %{remaining_budget_ms: budget},
                        %{classification: :transport, attempt: 1, node_name: ^node_a}},
                       500

        assert is_integer(budget) and budget >= 0

        assert_receive {:transport, ^node_a, request_a, _deadline,
                        [use_compression: false, attempt: 0]}

        assert request_a == "batch:#{node_a}:0"

        assert_receive {:transport, ^node_b, request_b, _deadline,
                        [use_compression: false, attempt: 1]}

        assert request_b == "batch:#{node_b}:0"

        assert_receive {:transport, ^node_c, request_c, _deadline,
                        [use_compression: false, attempt: 0]}

        assert request_c == "batch:#{node_c}:1"
      after
        :telemetry.detach(handler)
      end
    end

    test "rebalance retries call the tend hook and preserve per-node outcomes" do
      parent = self()
      handler = attach_retry_handler(:batch_rebalance_retry)
      node_a = unique_node_name("A1")
      node_b = unique_node_name("B1")
      node_c = unique_node_name("C1")

      try do
        responses =
          start_responses!(%{
            node_a => [{:error, Error.from_result_code(:partition_unavailable)}],
            node_b => [{:ok, "body-b1"}],
            node_c => [{:error, Error.from_result_code(:key_not_found)}]
          })

        command =
          command(fn results, _input ->
            Map.new(results, fn node_result ->
              {entry_index(node_result.request),
               {node_result.request.node_name, node_result.attempt, node_result.result}}
            end)
          end)

        result =
          BatchCommand.run(
            command,
            runtime_for(max_retries: 1, on_rebalance: fn -> send(parent, :tend_now) end),
            :batch,
            [node_request(node_a, [0]), node_request(node_c, [1])],
            ctx(parent, responses,
              reroute_request: fn
                :rebalance, %NodeRequest{} = request, 1 -> {:ok, %{request | node_name: node_b}}
                _kind, %NodeRequest{} = request, _attempt -> {:ok, request}
              end
            )
          )

        assert result == %{
                 0 => {node_b, 1, {:ok, "body-b1"}},
                 1 => {node_c, 0, {:error, Error.from_result_code(:key_not_found)}}
               }

        assert_receive :tend_now

        assert_receive {:event, [:aerospike, :retry, :attempt], _measurements,
                        %{classification: :rebalance, attempt: 1, node_name: ^node_a}},
                       500

        assert_receive {:transport, ^node_a, request_a, _deadline,
                        [use_compression: false, attempt: 0]}

        assert request_a == "batch:#{node_a}:0"

        assert_receive {:transport, ^node_b, request_b, _deadline,
                        [use_compression: false, attempt: 1]}

        assert request_b == "batch:#{node_b}:0"

        assert_receive {:transport, ^node_c, request_c, _deadline,
                        [use_compression: false, attempt: 0]}

        assert request_c == "batch:#{node_c}:1"
      after
        :telemetry.detach(handler)
      end
    end

    test "retry reroute can regroup one failed request into multiple node requests" do
      parent = self()
      node_a = unique_node_name("A1")
      node_b = unique_node_name("B1")
      node_c = unique_node_name("C1")

      responses =
        start_responses!(%{
          node_a => [{:error, Error.from_result_code(:network_error, message: "boom-a1")}],
          node_b => [{:ok, "body-b1"}],
          node_c => [{:ok, "body-c1"}]
        })

      command =
        command(fn results, _input ->
          Enum.map(results, fn node_result ->
            {Enum.map(node_result.request.entries, & &1.index), node_result.request.node_name,
             node_result.attempt, node_result.result}
          end)
        end)

      request = node_request(node_a, [0, 1])
      [entry_0, entry_1] = request.entries

      result =
        BatchCommand.run(
          command,
          runtime_for(max_retries: 1),
          :batch,
          [request],
          ctx(parent, responses,
            reroute_request: fn
              :transport, %NodeRequest{} = failed_request, 1 ->
                {:ok,
                 %Regroup{
                   node_requests: [
                     %{failed_request | node_name: node_b, entries: [entry_0]},
                     %{failed_request | node_name: node_c, entries: [entry_1]}
                   ],
                   node_results: []
                 }}

              _kind, %NodeRequest{} = failed_request, _attempt ->
                {:ok, failed_request}
            end
          )
        )

      assert result == [
               {[0], node_b, 1, {:ok, "body-b1"}},
               {[1], node_c, 1, {:ok, "body-c1"}}
             ]

      assert_receive {:transport, ^node_a, request_a, _deadline,
                      [use_compression: false, attempt: 0]}

      assert request_a == "batch:#{node_a}:0,1"

      assert_receive {:transport, ^node_b, request_b, _deadline,
                      [use_compression: false, attempt: 1]}

      assert request_b == "batch:#{node_b}:0"

      assert_receive {:transport, ^node_c, request_c, _deadline,
                      [use_compression: false, attempt: 1]}

      assert request_c == "batch:#{node_c}:1"
    end

    test "unknown-node refusal remains scoped to one grouped request" do
      parent = self()
      responses = start_responses!(%{"A1" => [{:ok, "body-a1"}]})

      command =
        command(fn results, _input ->
          Map.new(results, fn node_result ->
            {entry_index(node_result.request), node_result.result}
          end)
        end)

      result =
        BatchCommand.run(
          command,
          runtime_for(max_retries: 0),
          :batch,
          [node_request("A1", [0]), node_request("MISSING", [1])],
          ctx(parent, responses)
        )

      assert result == %{
               0 => {:ok, "body-a1"},
               1 => {:error, :unknown_node}
             }
    end
  end

  defp command(merge_results) do
    BatchCommand.new!(
      name: __MODULE__,
      build_request: fn %NodeRequest{} = request ->
        indexes = Enum.map_join(request.entries, ",", &Integer.to_string(&1.index))
        "batch:#{request.node_name}:#{indexes}"
      end,
      parse_response: fn body, _request -> {:ok, body} end,
      merge_results: merge_results
    )
  end

  defp ctx(parent, responses, overrides \\ []) do
    Map.new(
      [
        tender: :fake_tender,
        transport: __MODULE__.TransportStub,
        resolve_handle: fn
          :fake_tender, "MISSING" ->
            {:error, :unknown_node}

          :fake_tender, node_name ->
            {:ok, fake_handle(pool: {responses, node_name})}
        end,
        allow_dispatch: fn _counters, _breaker -> :ok end,
        checkout: fn node_name, _pool, fun, timeout ->
          send(parent, {:checkout, node_name, timeout})
          result = fun.({:conn, node_name, parent, responses})
          elem(result, 0)
        end,
        task_runner: fn requests, node_fun, _opts -> Enum.map(requests, node_fun) end
      ] ++ overrides
    )
  end

  defp runtime_for(opts) do
    on_rebalance = Keyword.get(opts, :on_rebalance, fn -> :ok end)

    Executor.new!(
      policy: %Policy.BatchRead{
        timeout: Keyword.get(opts, :timeout, 5_000),
        retry: %{
          max_retries: Keyword.get(opts, :max_retries, 0),
          sleep_between_retries_ms: 0,
          replica_policy: :sequence
        }
      },
      on_rebalance: on_rebalance
    )
  end

  defp node_request(node_name, indices) do
    %NodeRequest{
      node_name: node_name,
      entries:
        Enum.map(indices, fn index ->
          %Entry{
            index: index,
            key: Key.new("test", "batch", "#{node_name}-#{index}"),
            kind: :read,
            dispatch: {:read, :master, 0},
            payload: {:entry, index}
          }
        end)
    }
  end

  defp entry_index(%NodeRequest{entries: [%Entry{index: index} | _]}), do: index

  defp fake_handle(overrides) do
    Map.merge(
      %{pool: :pool, counters: :counters, breaker: :breaker, use_compression: false},
      Map.new(overrides)
    )
  end

  defp attach_retry_handler(tag) do
    parent = self()
    handler_id = "batch-retry-#{tag}-#{System.unique_integer([:positive])}"

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

  defp unique_node_name(prefix) do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end

  defp start_responses!(responses_by_node) do
    start_supervised!({Agent, fn -> responses_by_node end})
  end

  defmodule TransportStub do
    alias Aerospike.Error

    def command({:conn, node_name, parent, responses}, request, deadline, opts) do
      send(parent, {:transport, node_name, request, deadline, opts})

      Agent.get_and_update(responses, fn state ->
        case Map.get(state, node_name, []) do
          [next | rest] ->
            {next, Map.put(state, node_name, rest)}

          [] ->
            {{:error, Error.from_result_code(:network_error, message: "missing script")}, state}
        end
      end)
    end
  end
end
