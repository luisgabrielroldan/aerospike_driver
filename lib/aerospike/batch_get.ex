defmodule Aerospike.BatchGet do
  @moduledoc """
  Narrow public batch read helper for the spike.

  The helper fans out grouped read requests through the shared batch
  substrate and merges node replies back into the caller's key order.
  Each key keeps its own outcome, so routing misses, per-record server
  errors, and node-request failures stay scoped to the affected indices.

  The spike currently supports only full-record reads (`bins: :all`) and
  only the `:timeout` option. Retries remain disabled at the public
  policy surface even though grouped execution now supports honest
  regrouping internally.
  """

  alias Aerospike.Batch, as: MixedBatch
  alias Aerospike.BatchCommand
  alias Aerospike.BatchCommand.Entry
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Record

  @type option :: {:timeout, non_neg_integer()}

  @type item_result ::
          {:ok, Record.t()}
          | {:error, Error.t()}
          | {:error, :no_master | :unknown_node}

  @type result :: {:ok, [item_result()]} | {:error, Error.t()} | {:error, :cluster_not_ready}

  @spec execute(GenServer.server(), [Key.t()], :all | term(), [option()]) :: result()
  def execute(tender, keys, bins, opts \\ [])

  def execute(_tender, [], :all, opts) when is_list(opts) do
    with {:ok, _policy} <- Policy.batch_read(opts) do
      {:ok, []}
    end
  end

  def execute(tender, keys, :all, opts) when is_list(keys) and is_list(opts) do
    with :ok <- validate_keys(keys),
         {:ok, policy} <- batch_policy(tender, opts),
         {:ok, results} <- MixedBatch.execute(tender, entries(keys, policy), opts) do
      {:ok, Enum.map(results, &item_result/1)}
    end
  end

  def execute(_tender, _keys, _bins, _opts) do
    invalid_argument("Aerospike.batch_get/4 supports only :all bins in the spike")
  end

  defp entries(keys, %Policy.BatchRead{} = policy) do
    keys
    |> Enum.with_index()
    |> Enum.map(fn {key, index} ->
      %Entry{
        index: index,
        key: key,
        kind: :read,
        dispatch: {:read, policy.retry.replica_policy, 0},
        payload: nil
      }
    end)
  end

  defp item_result(%BatchCommand.Result{status: :ok, record: %Record{} = record}),
    do: {:ok, record}

  defp item_result(%BatchCommand.Result{status: :error, error: %Error{} = error}),
    do: {:error, error}

  defp item_result(%BatchCommand.Result{status: :error, error: reason}) when is_atom(reason),
    do: {:error, reason}

  defp validate_keys(keys) do
    if Enum.all?(keys, &match?(%Key{}, &1)) do
      :ok
    else
      invalid_argument("Aerospike.batch_get/4 expects a list of %Aerospike.Key{} values")
    end
  end

  defp batch_policy(tender, opts) do
    tender |> Aerospike.Cluster.retry_policy() |> Policy.batch_read(opts)
  end

  defp invalid_argument(message) do
    {:error, Error.from_result_code(:invalid_argument, message: message)}
  end
end
