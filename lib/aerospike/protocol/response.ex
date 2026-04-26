defmodule Aerospike.Protocol.Response do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.Batch
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
  Parses a single-record UDF reply into its returned value or a typed error.
  """
  @spec parse_udf_response(AsmMsg.t()) :: {:ok, term()} | {:error, Error.t()}
  def parse_udf_response(%AsmMsg{} = msg) do
    case result_atom(msg.result_code) do
      {:ok, :ok} ->
        with {:ok, bins} <- record_bins_from_operations(msg.operations) do
          extract_udf_value(bins)
        end

      {:ok, :udf_bad_response} ->
        extract_udf_error(msg.operations)

      other ->
        error_from_result(other)
    end
  end

  @doc """
  Parses a multi-record batch read/header reply into per-record indexed results.
  """
  @spec parse_batch_response(binary(), Aerospike.Command.BatchCommand.NodeRequest.t()) ::
          {:ok, Batch.Reply.t()} | {:error, Error.t()}
  def parse_batch_response(body, node_request) when is_binary(body) do
    Batch.parse_response(body, node_request)
  end

  @doc """
  Parses a multi-record batch read/header reply into per-record indexed results.
  """
  @spec parse_batch_read_response(
          binary(),
          Aerospike.Command.BatchCommand.NodeRequest.t(),
          keyword()
        ) ::
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
    {:ok, decode_record_bins(operations, %{}, nil)}
  end

  defp decode_record_bins([], bins, _repeated), do: bins

  defp decode_record_bins([%Operation{bin_name: ""} | rest], bins, repeated) do
    decode_record_bins(rest, bins, repeated)
  end

  defp decode_record_bins([%Operation{bin_name: name} = op | rest], bins, repeated) do
    {:ok, value} = Value.decode_value(op.particle_type, op.data)
    {bins, repeated} = put_operation_value(bins, repeated, name, value)
    decode_record_bins(rest, bins, repeated)
  end

  defp put_operation_value(bins, repeated, name, value) do
    case Map.fetch(bins, name) do
      {:ok, existing} -> put_repeated_operation_value(bins, repeated, name, existing, value)
      :error -> {Map.put(bins, name, value), repeated}
    end
  end

  defp put_repeated_operation_value(bins, repeated, name, existing, value) do
    if repeated != nil and MapSet.member?(repeated, name) do
      {Map.put(bins, name, existing ++ [value]), repeated}
    else
      {Map.put(bins, name, [existing, value]), put_repeated_name(repeated, name)}
    end
  end

  defp put_repeated_name(nil, name), do: MapSet.new([name])
  defp put_repeated_name(repeated, name), do: MapSet.put(repeated, name)

  defp result_atom(code) when is_integer(code) do
    ResultCode.from_integer(code)
  end

  defp extract_udf_value(%{"SUCCESS" => value}), do: {:ok, value}

  defp extract_udf_value(%{"FAILURE" => message}) do
    {:error, Error.from_result_code(:udf_bad_response, message: to_string(message))}
  end

  defp extract_udf_value(bins) when map_size(bins) == 0, do: {:ok, nil}
  defp extract_udf_value(bins), do: {:ok, bins}

  defp extract_udf_error(operations) do
    case record_bins_from_operations(operations) do
      {:ok, %{"FAILURE" => message}} ->
        {:error, Error.from_result_code(:udf_bad_response, message: to_string(message))}

      _ ->
        {:error, Error.from_result_code(:udf_bad_response)}
    end
  end

  defp error_from_result({:ok, error_code}) do
    {:error, Error.from_result_code(error_code)}
  end

  defp error_from_result({:error, unknown}) do
    {:error, Error.from_result_code(:server_error, message: "unknown result code #{unknown}")}
  end
end
