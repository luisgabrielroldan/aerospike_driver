defmodule Aerospike.BatchExecutorTest do
  use ExUnit.Case, async: true

  alias Aerospike.BatchCommand
  alias Aerospike.BatchCommand.Entry
  alias Aerospike.BatchCommand.NodeRequest
  alias Aerospike.BatchExecutor
  alias Aerospike.Error
  alias Aerospike.Policy

  describe "run_command/5" do
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
        BatchExecutor.run_command(
          executor_for(max_retries: 0, max_concurrency: 2),
          command,
          %{order: [0, 1, 2]},
          node_requests,
          ctx(parent, responses,
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

      responses =
        start_responses!(%{
          "A1" => [{:error, Error.from_result_code(:network_error, message: "boom-a1")}],
          "B1" => [{:ok, "body-b1"}],
          "C1" => [{:ok, "body-c1"}]
        })

      command =
        command(fn results, _input ->
          Enum.map(results, fn node_result ->
            {entry_index(node_result.request), node_result.request.node_name, node_result.attempt,
             node_result.result}
          end)
        end)

      result =
        BatchExecutor.run_command(
          executor_for(max_retries: 1),
          command,
          :batch,
          [node_request("A1", [0]), node_request("C1", [1])],
          ctx(parent, responses,
            reroute_request: fn
              :transport, %NodeRequest{} = request, 1 -> {:ok, %{request | node_name: "B1"}}
              kind, %NodeRequest{} = request, _attempt -> {:ok, %{request | payload: kind}}
            end
          )
        )

      assert result == [
               {0, "B1", 1, {:ok, "body-b1"}},
               {1, "C1", 0, {:ok, "body-c1"}}
             ]

      assert_receive {:transport, "A1", "batch:A1:0", _deadline,
                      [use_compression: false, attempt: 0]}

      assert_receive {:transport, "B1", "batch:B1:0", _deadline,
                      [use_compression: false, attempt: 1]}

      assert_receive {:transport, "C1", "batch:C1:1", _deadline,
                      [use_compression: false, attempt: 0]}
    end

    test "rebalance retries call the tend hook and preserve per-node outcomes" do
      parent = self()

      responses =
        start_responses!(%{
          "A1" => [{:error, Error.from_result_code(:partition_unavailable)}],
          "B1" => [{:ok, "body-b1"}],
          "C1" => [{:error, Error.from_result_code(:key_not_found)}]
        })

      command =
        command(fn results, _input ->
          Map.new(results, fn node_result ->
            {entry_index(node_result.request),
             {node_result.request.node_name, node_result.attempt, node_result.result}}
          end)
        end)

      result =
        BatchExecutor.run_command(
          executor_for(max_retries: 1, on_rebalance: fn -> send(parent, :tend_now) end),
          command,
          :batch,
          [node_request("A1", [0]), node_request("C1", [1])],
          ctx(parent, responses,
            reroute_request: fn
              :rebalance, %NodeRequest{} = request, 1 -> {:ok, %{request | node_name: "B1"}}
              _kind, %NodeRequest{} = request, _attempt -> {:ok, request}
            end
          )
        )

      assert result == %{
               0 => {"B1", 1, {:ok, "body-b1"}},
               1 => {"C1", 0, {:error, Error.from_result_code(:key_not_found)}}
             }

      assert_receive :tend_now

      assert_receive {:transport, "A1", "batch:A1:0", _deadline,
                      [use_compression: false, attempt: 0]}

      assert_receive {:transport, "B1", "batch:B1:0", _deadline,
                      [use_compression: false, attempt: 1]}

      assert_receive {:transport, "C1", "batch:C1:1", _deadline,
                      [use_compression: false, attempt: 0]}
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
        BatchExecutor.run_command(
          executor_for(max_retries: 0),
          command,
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

  defp executor_for(opts) do
    {max_concurrency, opts} = Keyword.pop(opts, :max_concurrency, 1)
    {on_rebalance, opts} = Keyword.pop(opts, :on_rebalance, fn -> :ok end)

    BatchExecutor.new!(
      policy: %Policy.BatchRead{
        timeout: Keyword.get(opts, :timeout, 5_000),
        retry: %{
          max_retries: Keyword.get(opts, :max_retries, 0),
          sleep_between_retries_ms: 0,
          replica_policy: :sequence
        }
      },
      max_concurrency: max_concurrency,
      on_rebalance: on_rebalance
    )
  end

  defp node_request(node_name, indices) do
    %NodeRequest{
      node_name: node_name,
      entries: Enum.map(indices, &%Entry{index: &1, payload: {:entry, &1}})
    }
  end

  defp entry_index(%NodeRequest{entries: [%Entry{index: index} | _]}), do: index

  defp fake_handle(overrides) do
    Map.merge(
      %{pool: :pool, counters: :counters, breaker: :breaker, use_compression: false},
      Map.new(overrides)
    )
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
