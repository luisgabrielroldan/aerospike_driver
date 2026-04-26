defmodule Aerospike.Command.Batch do
  @moduledoc false

  alias Aerospike.BatchResult
  alias Aerospike.Cluster
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.BatchCommand
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Command.BatchCommand.NodeRequest
  alias Aerospike.Command.BatchCommand.NodeResult
  alias Aerospike.Command.BatchCommand.Regroup
  alias Aerospike.Command.BatchCommand.Result
  alias Aerospike.Command.BatchRouter
  alias Aerospike.Error
  alias Aerospike.Policy
  alias Aerospike.Protocol.Batch, as: BatchProtocol
  alias Aerospike.Protocol.Response
  alias Aerospike.Runtime.Executor
  alias Aerospike.RuntimeMetrics

  @default_max_concurrency max(System.schedulers_online(), 1)

  @type result :: {:ok, [Result.t()]} | {:error, Error.t()} | {:error, :cluster_not_ready}

  @doc false
  @spec to_public_results([Result.t()]) :: [BatchResult.t()]
  def to_public_results(results) when is_list(results) do
    Enum.map(results, &BatchResult.from_command_result/1)
  end

  @spec execute(GenServer.server(), [Entry.t()], keyword()) :: result()
  def execute(tender, entries, opts \\ [])

  def execute(_tender, [], opts) when is_list(opts) do
    with {:ok, _policy} <- Policy.batch(opts) do
      {:ok, []}
    end
  end

  def execute(tender, entries, opts) when is_list(entries) and is_list(opts) do
    with :ok <- validate_entries(entries),
         {:ok, _validated} <- Policy.batch(opts),
         {:ok, policy} <- batch_policy(tender, opts),
         {:ok, grouping} <- BatchRouter.group_entries(runtime_ctx(tender).tables, entries) do
      runtime = runtime_ctx(tender)
      executor = Executor.new!(policy: policy)
      start_mono = System.monotonic_time()

      result =
        BatchCommand.run(
          command(policy),
          executor,
          %{entries: entries, routing_failures: grouping.routing_failures},
          grouping.node_requests,
          %{
            tender: runtime.tender,
            transport: runtime.transport,
            max_concurrency: @default_max_concurrency,
            reroute_request: &reroute_request(runtime.tables, &1, &2, &3),
            metrics_cluster: runtime.tender
          }
        )

      RuntimeMetrics.record_command(runtime.tender, __MODULE__, start_mono, result)
      result
    end
  end

  defp command(%Policy.Batch{} = policy) do
    BatchCommand.new!(
      name: __MODULE__,
      transport_mode: :command_stream,
      build_request: &BatchProtocol.encode_request(&1, timeout: policy.timeout),
      parse_response: &Response.parse_batch_response/2,
      merge_results: &merge_results/2
    )
  end

  defp merge_results(node_results, %{entries: entries, routing_failures: routing_failures}) do
    routing_failures
    |> Enum.reduce(%{}, &put_routing_failure/2)
    |> merge_node_results(node_results)
    |> ordered_results(entries)
  end

  defp put_routing_failure(
         %BatchRouter.RoutingFailure{entry: %Entry{} = entry, reason: reason},
         acc
       ) do
    Map.put(acc, entry.index, error_result(entry, reason))
  end

  defp merge_node_results(acc, node_results) do
    Enum.reduce(node_results, acc, &put_node_result(&2, &1))
  end

  defp put_node_result(
         acc,
         %NodeResult{
           request: %NodeRequest{entries: entries},
           result: {:ok, %BatchProtocol.Reply{results: results}}
         }
       ) do
    result_map = Map.new(results, &{&1.index, &1})

    Enum.reduce(entries, acc, fn %Entry{} = entry, next_acc ->
      Map.put(next_acc, entry.index, Map.get(result_map, entry.index, missing_result(entry)))
    end)
  end

  defp put_node_result(acc, %NodeResult{request: %NodeRequest{entries: entries}, result: error}) do
    Enum.reduce(entries, acc, fn %Entry{} = entry, next_acc ->
      Map.put(next_acc, entry.index, error_result(entry, error))
    end)
  end

  defp ordered_results(outcomes, entries) do
    case ordered_results_if_sorted(outcomes, entries, -1, []) do
      {:ok, ordered} ->
        {:ok, ordered}

      :unsorted ->
        sorted_ordered_results(outcomes, entries)
    end
  end

  defp ordered_results_if_sorted(_outcomes, [], _previous_index, acc) do
    {:ok, Enum.reverse(acc)}
  end

  defp ordered_results_if_sorted(
         outcomes,
         [%Entry{index: index} = entry | rest],
         previous_index,
         acc
       )
       when index >= previous_index do
    result = Map.get(outcomes, index, missing_merge_result(entry))
    ordered_results_if_sorted(outcomes, rest, index, [result | acc])
  end

  defp ordered_results_if_sorted(_outcomes, [%Entry{} | _rest], _previous_index, _acc) do
    :unsorted
  end

  defp sorted_ordered_results(outcomes, entries) do
    ordered =
      entries
      |> Enum.sort_by(& &1.index)
      |> Enum.map(fn %Entry{index: index} = entry ->
        Map.get(outcomes, index, missing_merge_result(entry))
      end)

    {:ok, ordered}
  end

  defp reroute_request(tables, _reroute_kind, %NodeRequest{} = node_request, next_attempt) do
    entries = Enum.map(node_request.entries, &reroute_entry(&1, next_attempt))

    case BatchRouter.group_entries(tables, entries) do
      {:ok, grouping} ->
        {:ok,
         %Regroup{
           node_requests: grouping.node_requests,
           node_results:
             Enum.map(
               grouping.routing_failures,
               &routing_failure_node_result(node_request, &1, next_attempt)
             )
         }}

      {:error, :cluster_not_ready} = err ->
        err
    end
  end

  defp reroute_entry(%Entry{dispatch: {:read, replica_policy, _attempt}} = entry, next_attempt) do
    %{entry | dispatch: {:read, replica_policy, next_attempt}}
  end

  defp reroute_entry(%Entry{} = entry, _next_attempt), do: entry

  defp routing_failure_node_result(
         %NodeRequest{node_name: node_name},
         %BatchRouter.RoutingFailure{entry: %Entry{} = entry, reason: reason},
         attempt
       ) do
    %NodeResult{
      request: %NodeRequest{node_name: node_name, entries: [entry], payload: nil},
      result: {:error, reason},
      attempt: attempt
    }
  end

  defp error_result(%Entry{} = entry, {:error, %Error{} = error}) do
    %Result{
      index: entry.index,
      key: entry.key,
      kind: entry.kind,
      status: :error,
      record: nil,
      error: error,
      in_doubt: error.in_doubt
    }
  end

  defp error_result(%Entry{} = entry, {:error, reason}) when is_atom(reason) do
    %Result{
      index: entry.index,
      key: entry.key,
      kind: entry.kind,
      status: :error,
      record: nil,
      error: reason,
      in_doubt: false
    }
  end

  defp error_result(%Entry{} = entry, reason) when is_atom(reason) do
    error_result(entry, {:error, reason})
  end

  defp missing_result(%Entry{} = entry) do
    error_result(
      entry,
      {:error,
       Error.from_result_code(:parse_error,
         message: "batch reply omitted requested index #{entry.index}"
       )}
    )
  end

  defp missing_merge_result(%Entry{} = entry) do
    error_result(
      entry,
      {:error,
       Error.from_result_code(:parse_error,
         message: "batch merge omitted requested index #{entry.index}"
       )}
    )
  end

  defp validate_entries(entries) do
    if Enum.all?(entries, &match?(%Entry{}, &1)) do
      :ok
    else
      invalid_argument("Aerospike.Command.Batch.execute/3 expects a list of batch entries")
    end
  end

  defp batch_policy(tender, opts) do
    tender
    |> Cluster.retry_policy()
    |> Policy.batch(opts)
  end

  defp runtime_ctx(tender) do
    %{
      tender: tender,
      tables: Cluster.tables(tender),
      transport: Tender.transport(tender)
    }
  end

  defp invalid_argument(message) do
    {:error, Error.from_result_code(:invalid_argument, message: message)}
  end
end
