defmodule Aerospike.BatchGet do
  @moduledoc """
  Narrow public batch read helper for the spike.

  The helper fans out grouped read requests through the shared batch
  substrate and merges node replies back into the caller's key order.
  Each key keeps its own outcome, so routing misses, per-record server
  errors, and node-request failures stay scoped to the affected indices.

  The spike currently supports only full-record reads (`bins: :all`) and
  only the `:timeout` option. Retries are intentionally disabled here:
  regrouping a failed node request may need to split its keys across
  multiple nodes, and the current batch reroute hook does not model that
  honestly yet.
  """

  alias Aerospike.BatchCommand
  alias Aerospike.BatchCommand.NodeRequest
  alias Aerospike.BatchCommand.NodeResult
  alias Aerospike.BatchExecutor
  alias Aerospike.BatchRouter
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.BatchRead
  alias Aerospike.Protocol.Response
  alias Aerospike.Record
  alias Aerospike.RetryPolicy
  alias Aerospike.Tender

  @type option :: {:timeout, non_neg_integer()}

  @type item_result ::
          {:ok, Record.t()}
          | {:error, Error.t()}
          | {:error, :no_master | :unknown_node}

  @type result :: {:ok, [item_result()]} | {:error, Error.t()} | {:error, :cluster_not_ready}

  @spec execute(GenServer.server(), [Key.t()], :all | term(), [option()]) :: result()
  def execute(tender, keys, bins, opts \\ [])

  def execute(_tender, [], :all, opts) when is_list(opts) do
    with :ok <- validate_opts(opts) do
      {:ok, []}
    end
  end

  def execute(tender, keys, :all, opts) when is_list(keys) and is_list(opts) do
    with :ok <- validate_keys(keys),
         :ok <- validate_opts(opts) do
      runtime = runtime_ctx(tender)
      executor = executor(runtime.tables, opts)

      case BatchRouter.group_keys(runtime.tables, keys,
             dispatch: {:read, executor.policy.replica_policy, 0}
           ) do
        {:ok, grouping} ->
          BatchExecutor.run_command(
            executor,
            command(),
            %{key_count: length(keys), routing_failures: grouping.routing_failures},
            grouping.node_requests,
            %{tender: runtime.tender, transport: runtime.transport}
          )

        {:error, :cluster_not_ready} = err ->
          err
      end
    end
  end

  def execute(_tender, _keys, _bins, _opts) do
    invalid_argument("Aerospike.batch_get/4 supports only :all bins in the spike")
  end

  defp command do
    BatchCommand.new!(
      name: __MODULE__,
      transport_mode: :command_stream,
      build_request: &BatchRead.encode_request(&1, mode: :all_bins),
      parse_response: &Response.parse_batch_read_response(&1, &2, mode: :all_bins),
      merge_results: &merge_results/2
    )
  end

  defp merge_results(node_results, %{key_count: key_count, routing_failures: routing_failures}) do
    outcomes =
      routing_failures
      |> Enum.reduce(%{}, &put_routing_failure/2)
      |> merge_node_results(node_results)

    ordered_results(outcomes, key_count)
  end

  defp put_routing_failure(
         %BatchRouter.RoutingFailure{entry: %{index: index}, reason: reason},
         acc
       ) do
    Map.put(acc, index, {:error, reason})
  end

  defp merge_node_results(acc, node_results) do
    Enum.reduce(node_results, acc, &put_node_result(&2, &1))
  end

  defp put_node_result(acc, %NodeResult{
         request: %NodeRequest{entries: entries},
         result: {:ok, %BatchRead.Reply{records: records}}
       }) do
    record_results = Map.new(records, &{&1.index, record_result(&1)})

    Enum.reduce(entries, acc, fn %{index: index}, next_acc ->
      Map.put(next_acc, index, Map.get(record_results, index, missing_record_error(index)))
    end)
  end

  defp put_node_result(acc, %NodeResult{request: %NodeRequest{entries: entries}, result: error}) do
    Enum.reduce(entries, acc, fn %{index: index}, next_acc ->
      Map.put(next_acc, index, error)
    end)
  end

  defp ordered_results(_outcomes, 0), do: {:ok, []}

  defp ordered_results(outcomes, key_count) when key_count > 0 do
    0..(key_count - 1)
    |> Enum.reduce_while([], fn index, acc ->
      case Map.fetch(outcomes, index) do
        {:ok, result} -> {:cont, [result | acc]}
        :error -> {:halt, {:error, missing_merge_error(index)}}
      end
    end)
    |> case do
      {:error, %Error{}} = err -> err
      results -> {:ok, Enum.reverse(results)}
    end
  end

  defp record_result(%BatchRead.RecordResult{
         key: key,
         result: :ok,
         generation: generation,
         ttl: ttl,
         bins: bins
       }) do
    {:ok, %Record{key: key, bins: bins, generation: generation, ttl: ttl}}
  end

  defp record_result(%BatchRead.RecordResult{result: {:error, %Error{}} = err}), do: err

  defp missing_record_error(index) do
    {:error,
     Error.from_result_code(:parse_error,
       message: "batch read reply omitted requested index #{index}"
     )}
  end

  defp missing_merge_error(index) do
    Error.from_result_code(:parse_error, message: "batch merge omitted requested index #{index}")
  end

  defp validate_keys(keys) do
    if Enum.all?(keys, &match?(%Key{}, &1)) do
      :ok
    else
      invalid_argument("Aerospike.batch_get/4 expects a list of %Aerospike.Key{} values")
    end
  end

  defp validate_opts(opts) do
    case Enum.find(opts, fn
           {:timeout, timeout} -> not (is_integer(timeout) and timeout >= 0)
           {key, _value} -> key != :timeout
         end) do
      nil ->
        :ok

      {:timeout, _bad_timeout} ->
        invalid_argument("Aerospike.batch_get/4 expects :timeout to be a non-negative integer")

      {key, _value} ->
        invalid_argument(
          "Aerospike.batch_get/4 currently supports only the :timeout option, got #{inspect(key)}"
        )
    end
  end

  defp executor(tables, opts) do
    base_policy =
      tables.meta
      |> RetryPolicy.load()
      |> Map.put(:max_retries, 0)
      |> Map.put(:sleep_between_retries_ms, 0)

    BatchExecutor.new!(base_policy: base_policy, command_opts: Keyword.take(opts, [:timeout]))
  end

  defp runtime_ctx(tender) do
    %{
      tender: tender,
      tables: Tender.tables(tender),
      transport: Tender.transport(tender)
    }
  end

  defp invalid_argument(message) do
    {:error, Error.from_result_code(:invalid_argument, message: message)}
  end
end
