defmodule Aerospike.Command.BatchUdf do
  @moduledoc false

  alias Aerospike.BatchResult
  alias Aerospike.Command.Batch, as: MixedBatch
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy

  @type option :: {:timeout, non_neg_integer()}
  @type result :: {:ok, [BatchResult.t()]} | {:error, Error.t()} | {:error, :cluster_not_ready}

  @spec execute(GenServer.server(), [Key.t()], String.t(), String.t(), list(), [option()]) ::
          result()
  def execute(tender, keys, package, function, args, opts \\ [])

  def execute(_tender, [], package, function, args, opts)
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    with {:ok, _policy} <- Policy.batch(opts) do
      {:ok, []}
    end
  end

  def execute(tender, keys, package, function, args, opts)
      when is_list(keys) and is_binary(package) and is_binary(function) and is_list(args) and
             is_list(opts) do
    with :ok <- validate_keys(keys),
         {:ok, results} <-
           MixedBatch.execute(tender, entries(keys, package, function, args), opts) do
      {:ok, MixedBatch.to_public_results(results)}
    end
  end

  defp entries(keys, package, function, args) do
    keys
    |> Enum.with_index()
    |> Enum.map(fn {key, index} ->
      %Entry{
        index: index,
        key: key,
        kind: :udf,
        dispatch: :write,
        payload: %{package: package, function: function, args: args}
      }
    end)
  end

  defp validate_keys(keys) do
    if Enum.all?(keys, &match?(%Key{}, &1)) do
      :ok
    else
      invalid_argument("Aerospike.batch_udf/6 expects a list of %Aerospike.Key{} values")
    end
  end

  defp invalid_argument(message) do
    {:error, Error.from_result_code(:invalid_argument, message: message)}
  end
end
