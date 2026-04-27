defmodule Aerospike.Command.BatchGet do
  @moduledoc false

  alias Aerospike.Command.Batch, as: MixedBatch
  alias Aerospike.Command.BatchCommand
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.OperateFlags
  alias Aerospike.Record

  @type option :: {:timeout, non_neg_integer()}
  @type bin_name :: String.t() | atom()
  @type mode :: :all | :header | :exists | [bin_name()]
  @type public_result_mode :: :all | :header | :exists | :bins

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

  def execute(_tender, [], mode, opts) when is_list(opts) do
    with {:ok, _read_spec} <- read_spec(mode),
         {:ok, _policy} <- Policy.batch_read(opts) do
      {:ok, []}
    end
  end

  def execute(tender, keys, mode, opts) when is_list(keys) and is_list(opts) do
    with :ok <- validate_keys(keys),
         {:ok, read_spec} <- read_spec(mode),
         {:ok, policy} <- batch_policy(tender, opts),
         {:ok, results} <- MixedBatch.execute(tender, entries(keys, policy, read_spec), opts) do
      {:ok, to_public_results(results, public_result_mode(read_spec))}
    end
  end

  def execute(_tender, _keys, _bins, _opts) do
    invalid_argument(
      "Aerospike.batch_get/4 expects :all, :header, :exists, or a non-empty list of string or atom bin names"
    )
  end

  @spec execute_operate(GenServer.server(), [Key.t()], [Aerospike.Op.t()], [option()]) ::
          record_result()
  def execute_operate(tender, keys, operations, opts \\ [])

  def execute_operate(_tender, [], operations, opts)
      when is_list(operations) and is_list(opts) do
    with {:ok, _flags} <- validate_operations(operations),
         {:ok, _policy} <- Policy.batch_read(opts) do
      {:ok, []}
    end
  end

  def execute_operate(tender, keys, operations, opts)
      when is_list(keys) and is_list(operations) and is_list(opts) do
    with :ok <- validate_keys(keys, "Aerospike.batch_get_operate/4"),
         {:ok, flags} <- validate_operations(operations),
         {:ok, policy} <- batch_policy(tender, opts),
         {:ok, results} <-
           MixedBatch.execute(tender, operate_entries(keys, policy, operations, flags), opts) do
      {:ok, to_public_results(results, :all)}
    end
  end

  @doc false
  @spec to_public_results([BatchCommand.Result.t()], public_result_mode()) :: [
          record_item_result() | exists_item_result()
        ]
  def to_public_results(results, mode)
      when is_list(results) and mode in [:all, :header, :exists, :bins] do
    do_to_public_results(results, mode, [])
  end

  defp entries(keys, %Policy.BatchRead{} = policy, read_spec) do
    keys
    |> Enum.with_index()
    |> Enum.map(fn {key, index} ->
      %Entry{
        index: index,
        key: key,
        kind: entry_kind(read_spec),
        dispatch: {:read, policy.retry.replica_policy, 0},
        payload: entry_payload(read_spec)
      }
    end)
  end

  defp read_spec(mode) when mode in [:all, :header, :exists], do: {:ok, mode}

  defp read_spec(bins) when is_list(bins) do
    with {:ok, operations} <- read_operations(bins) do
      {:ok, {:bins, operations}}
    end
  end

  defp read_spec(_mode) do
    invalid_argument(
      "Aerospike.batch_get/4 expects :all, :header, :exists, or a non-empty list of string or atom bin names"
    )
  end

  defp entry_kind(:all), do: :read
  defp entry_kind(:header), do: :read_header
  defp entry_kind(:exists), do: :exists
  defp entry_kind({:bins, _operations}), do: :read

  defp entry_payload({:bins, operations}), do: %{operations: operations}
  defp entry_payload(_read_spec), do: nil

  defp public_result_mode({:bins, _operations}), do: :bins
  defp public_result_mode(mode), do: mode

  defp read_operations([]) do
    invalid_argument("Aerospike.batch_get/4 expects at least one named bin")
  end

  defp read_operations(bins) do
    do_read_operations(bins, [])
  end

  defp do_read_operations([], acc), do: {:ok, Enum.reverse(acc)}

  defp do_read_operations([bin | rest], acc) when is_binary(bin) and byte_size(bin) > 0 do
    do_read_operations(rest, [Operation.read(bin) | acc])
  end

  defp do_read_operations([bin | rest], acc) when is_atom(bin) do
    do_read_operations([Atom.to_string(bin) | rest], acc)
  end

  defp do_read_operations([bin | _rest], _acc) do
    invalid_argument(
      "Aerospike.batch_get/4 bin names must be non-empty strings or atoms, got: #{inspect(bin)}"
    )
  end

  defp operate_entries(keys, %Policy.BatchRead{} = policy, operations, flags) do
    keys
    |> Enum.with_index()
    |> Enum.map(fn {key, index} ->
      %Entry{
        index: index,
        key: key,
        kind: :operate,
        dispatch: {:read, policy.retry.replica_policy, 0},
        payload: %{operations: operations, flags: flags}
      }
    end)
  end

  defp do_to_public_results([], _mode, acc), do: Enum.reverse(acc)

  defp do_to_public_results(
         [%BatchCommand.Result{status: :ok, record: %Record{} = record} | rest],
         :all,
         acc
       ) do
    do_to_public_results(rest, :all, [{:ok, record} | acc])
  end

  defp do_to_public_results(
         [%BatchCommand.Result{status: :ok, record: %Record{} = record} | rest],
         :bins,
         acc
       ) do
    do_to_public_results(rest, :bins, [{:ok, record} | acc])
  end

  defp do_to_public_results(
         [
           %BatchCommand.Result{
             status: :ok,
             key: key,
             record: %{generation: generation, ttl: ttl}
           }
           | rest
         ],
         :header,
         acc
       ) do
    record = %Record{key: key, bins: %{}, generation: generation, ttl: ttl}
    do_to_public_results(rest, :header, [{:ok, record} | acc])
  end

  defp do_to_public_results(
         [%BatchCommand.Result{status: :ok} | rest],
         :exists,
         acc
       ) do
    do_to_public_results(rest, :exists, [{:ok, true} | acc])
  end

  defp do_to_public_results(
         [
           %BatchCommand.Result{
             status: :error,
             error: %Error{code: :key_not_found}
           }
           | rest
         ],
         :exists,
         acc
       ) do
    do_to_public_results(rest, :exists, [{:ok, false} | acc])
  end

  defp do_to_public_results(
         [
           %BatchCommand.Result{
             status: :error,
             error: %Error{} = error
           }
           | rest
         ],
         mode,
         acc
       ) do
    do_to_public_results(rest, mode, [{:error, error} | acc])
  end

  defp do_to_public_results(
         [
           %BatchCommand.Result{
             status: :error,
             error: reason
           }
           | rest
         ],
         mode,
         acc
       )
       when is_atom(reason) do
    do_to_public_results(rest, mode, [{:error, reason} | acc])
  end

  defp validate_keys(keys), do: validate_keys(keys, "Aerospike.batch_get/4")

  defp validate_keys(keys, callsite) do
    if Enum.all?(keys, &match?(%Key{}, &1)) do
      :ok
    else
      invalid_argument("#{callsite} expects a list of %Aerospike.Key{} values")
    end
  end

  defp validate_operations([]) do
    invalid_argument("Aerospike.batch_get_operate/4 expects a non-empty operation list")
  end

  defp validate_operations(operations) when is_list(operations) do
    flags = OperateFlags.scan_ops(operations)

    if flags.has_write? do
      invalid_argument("Aerospike.batch_get_operate/4 accepts only read-only operations")
    else
      {:ok, flags}
    end
  end

  defp batch_policy(tender, opts) do
    tender |> Aerospike.Cluster.retry_policy() |> Policy.batch_read(opts)
  end

  defp invalid_argument(message) do
    {:error, Error.from_result_code(:invalid_argument, message: message)}
  end
end
