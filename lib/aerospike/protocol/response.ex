defmodule Aerospike.Protocol.Response do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Batch
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.BatchRead
  alias Aerospike.Protocol.ResultCode
  alias Aerospike.Record

  @spec decode_as_msg(binary()) :: {:ok, AsmMsg.t()} | {:error, Error.t()}
  def decode_as_msg(body) when is_binary(body) do
    case AsmMsg.decode(body) do
      {:ok, _} = ok ->
        ok

      {:error, reason} ->
        {:error,
         Error.from_result_code(:parse_error, message: "failed to decode AS_MSG reply: #{reason}")}
    end
  end

  @doc """
  Parses a successful write or header-only reply into record metadata or an error.
  """
  @spec parse_record_metadata_response(AsmMsg.t()) ::
          {:ok, Record.metadata()} | {:error, Error.t()}
  def parse_record_metadata_response(%AsmMsg{} = msg) do
    case result_atom(msg.result_code) do
      {:ok, :ok} ->
        {:ok, record_metadata(msg)}

      other ->
        error_from_result(other)
    end
  end

  @doc """
  Parses a successful read response into a `%Record{}` or an error.
  """
  @spec parse_record_response(AsmMsg.t(), Key.t()) :: {:ok, Record.t()} | {:error, Error.t()}
  def parse_record_response(%AsmMsg{} = msg, %Key{} = key) do
    case result_atom(msg.result_code) do
      {:ok, :ok} ->
        with {:ok, bins} <- record_bins_from_operations(msg.operations) do
          {:ok,
           %Record{
             key: key,
             bins: bins,
             generation: msg.generation,
             ttl: msg.expiration
           }}
        end

      other ->
        error_from_result(other)
    end
  end

  @doc """
  Parses a multi-record batch read/header reply into per-record indexed results.
  """
  @spec parse_batch_response(binary(), Aerospike.BatchCommand.NodeRequest.t()) ::
          {:ok, Batch.Reply.t()} | {:error, Error.t()}
  def parse_batch_response(body, node_request) when is_binary(body) do
    Batch.parse_response(body, node_request)
  end

  @doc """
  Parses a multi-record batch read/header reply into per-record indexed results.
  """
  @spec parse_batch_read_response(binary(), Aerospike.BatchCommand.NodeRequest.t(), keyword()) ::
          {:ok, BatchRead.Reply.t()} | {:error, Error.t()}
  def parse_batch_read_response(body, node_request, opts \\ [])
      when is_binary(body) and is_list(opts) do
    BatchRead.parse_response(body, node_request, opts)
  end

  @spec record_metadata(AsmMsg.t()) :: Record.metadata()
  def record_metadata(%AsmMsg{} = msg) do
    %{generation: msg.generation, ttl: msg.expiration}
  end

  @doc """
  Extracts the record version from the fields of an AS_MSG response.
  """
  @spec extract_record_version(AsmMsg.t()) :: {:ok, non_neg_integer()} | :none
  def extract_record_version(%AsmMsg{fields: fields}) do
    rv_type = Field.type_record_version()

    case Enum.find(fields, fn field -> field.type == rv_type end) do
      %Field{data: <<version::56-little-unsigned>>} -> {:ok, version}
      nil -> :none
    end
  end

  @doc """
  Extracts the MRT deadline from the fields of an AS_MSG response.
  """
  @spec extract_mrt_deadline(AsmMsg.t()) :: {:ok, integer()} | :none
  def extract_mrt_deadline(%AsmMsg{fields: fields}) do
    dl_type = Field.type_mrt_deadline()

    case Enum.find(fields, fn field -> field.type == dl_type end) do
      %Field{data: <<deadline::32-signed-little>>} -> {:ok, deadline}
      nil -> :none
    end
  end

  defp record_bins_from_operations(operations) do
    bins =
      Enum.reduce(operations, %{bins: %{}, counts: %{}}, fn
        %Operation{bin_name: ""}, acc ->
          acc

        %Operation{bin_name: name} = op, acc ->
          {:ok, value} = Value.decode_value(op.particle_type, op.data)
          put_operation_value(acc, name, value)
      end)

    {:ok, bins.bins}
  end

  defp put_operation_value(%{bins: bins, counts: counts} = acc, name, value) do
    count = Map.get(counts, name, 0) + 1

    bins =
      case count do
        1 ->
          Map.put(bins, name, value)

        2 ->
          first_value = Map.fetch!(bins, name)
          Map.put(bins, name, [first_value, value])

        _ ->
          Map.update!(bins, name, fn values -> values ++ [value] end)
      end

    %{acc | bins: bins, counts: Map.put(counts, name, count)}
  end

  defp result_atom(code) when is_integer(code) do
    ResultCode.from_integer(code)
  end

  defp error_from_result({:ok, error_code}) do
    {:error, Error.from_result_code(error_code)}
  end

  defp error_from_result({:error, unknown}) do
    {:error, Error.from_result_code(:server_error, message: "unknown result code #{unknown}")}
  end
end
