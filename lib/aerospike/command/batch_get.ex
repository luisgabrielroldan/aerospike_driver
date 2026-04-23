defmodule Aerospike.Command.BatchGet do
  @moduledoc """
  Narrow public batch read helper for the spike.

  The helper fans out grouped batch read-style requests through the
  shared batch substrate and merges node replies back into the caller's
  key order. Each key keeps its own outcome, so routing misses,
  per-record server errors, and node-request failures stay scoped to the
  affected indices.

  The spike currently exposes three narrow public shapes over that
  substrate: full-record reads, header-only reads, and existence probes.
  All of them currently support only the `:timeout` option. Retries
  remain disabled at the public policy surface even though grouped
  execution can now regroup retries internally.
  """

  alias Aerospike.Command.Batch, as: MixedBatch
  alias Aerospike.Command.BatchCommand
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Record

  @type option :: {:timeout, non_neg_integer()}
  @type mode :: :all | :header | :exists

  @type record_item_result ::
          {:ok, Record.t()}
          | {:error, Error.t()}
          | {:error, :no_master | :unknown_node}

  @type exists_item_result ::
          {:ok, boolean()}
          | {:error, Error.t()}
          | {:error, :no_master | :unknown_node}

  @type record_result ::
          {:ok, [record_item_result()]} | {:error, Error.t()} | {:error, :cluster_not_ready}

  @type exists_result ::
          {:ok, [exists_item_result()]} | {:error, Error.t()} | {:error, :cluster_not_ready}

  @spec execute(GenServer.server(), [Key.t()], mode(), [option()]) ::
          record_result() | exists_result()
  def execute(tender, keys, mode, opts \\ [])

  def execute(_tender, [], mode, opts) when mode in [:all, :header, :exists] and is_list(opts) do
    with {:ok, _policy} <- Policy.batch_read(opts) do
      {:ok, []}
    end
  end

  def execute(tender, keys, mode, opts)
      when mode in [:all, :header, :exists] and is_list(keys) and is_list(opts) do
    with :ok <- validate_keys(keys),
         {:ok, policy} <- batch_policy(tender, opts),
         {:ok, results} <- MixedBatch.execute(tender, entries(keys, policy, mode), opts) do
      {:ok, Enum.map(results, &item_result(&1, mode))}
    end
  end

  def execute(_tender, _keys, _bins, _opts) do
    invalid_argument("Aerospike.batch_get/4 supports only :all bins in the spike")
  end

  defp entries(keys, %Policy.BatchRead{} = policy, mode) do
    keys
    |> Enum.with_index()
    |> Enum.map(fn {key, index} ->
      %Entry{
        index: index,
        key: key,
        kind: entry_kind(mode),
        dispatch: {:read, policy.retry.replica_policy, 0},
        payload: nil
      }
    end)
  end

  defp entry_kind(:all), do: :read
  defp entry_kind(:header), do: :read_header
  defp entry_kind(:exists), do: :exists

  defp item_result(%BatchCommand.Result{status: :ok, record: %Record{} = record}, :all),
    do: {:ok, record}

  defp item_result(
         %BatchCommand.Result{
           status: :ok,
           key: key,
           record: %{generation: generation, ttl: ttl}
         },
         :header
       ) do
    {:ok, %Record{key: key, bins: %{}, generation: generation, ttl: ttl}}
  end

  defp item_result(%BatchCommand.Result{status: :ok}, :exists), do: {:ok, true}

  defp item_result(
         %BatchCommand.Result{status: :error, error: %Error{code: :key_not_found}},
         :exists
       ),
       do: {:ok, false}

  defp item_result(%BatchCommand.Result{status: :error, error: %Error{} = error}, _mode),
    do: {:error, error}

  defp item_result(%BatchCommand.Result{status: :error, error: reason}, _mode)
       when is_atom(reason),
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
