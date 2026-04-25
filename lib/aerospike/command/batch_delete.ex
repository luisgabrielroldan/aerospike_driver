defmodule Aerospike.Command.BatchDelete do
  @moduledoc false

  alias Aerospike.BatchResult
  alias Aerospike.Command.Batch, as: MixedBatch
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy

  @type option :: {:timeout, non_neg_integer()}
  @type result :: {:ok, [BatchResult.t()]} | {:error, Error.t()} | {:error, :cluster_not_ready}

  @spec execute(GenServer.server(), [Key.t()], [option()]) :: result()
  def execute(tender, keys, opts \\ [])

  def execute(_tender, [], opts) when is_list(opts) do
    with {:ok, _policy} <- Policy.batch(opts) do
      {:ok, []}
    end
  end

  def execute(tender, keys, opts) when is_list(keys) and is_list(opts) do
    with :ok <- validate_keys(keys),
         {:ok, results} <- MixedBatch.execute(tender, entries(keys), opts) do
      {:ok, MixedBatch.to_public_results(results)}
    end
  end

  defp entries(keys) do
    keys
    |> Enum.with_index()
    |> Enum.map(fn {key, index} ->
      %Entry{
        index: index,
        key: key,
        kind: :delete,
        dispatch: :write,
        payload: nil
      }
    end)
  end

  defp validate_keys(keys) do
    if Enum.all?(keys, &match?(%Key{}, &1)) do
      :ok
    else
      invalid_argument("Aerospike.batch_delete/3 expects a list of %Aerospike.Key{} values")
    end
  end

  defp invalid_argument(message) do
    {:error, Error.from_result_code(:invalid_argument, message: message)}
  end
end
