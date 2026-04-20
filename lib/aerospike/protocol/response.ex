defmodule Aerospike.Protocol.Response do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.ResultCode
  alias Aerospike.Record

  @doc """
  Parses a successful read response into a `%Record{}` or an error.
  """
  @spec parse_record_response(AsmMsg.t(), Key.t()) :: {:ok, Record.t()} | {:error, Error.t()}
  def parse_record_response(%AsmMsg{} = msg, %Key{} = key) do
    case result_atom(msg.result_code) do
      {:ok, :ok} ->
        {:ok, bins} = record_bins_from_operations(msg.operations)

        {:ok,
         %Record{
           key: key,
           bins: bins,
           generation: msg.generation,
           ttl: msg.expiration
         }}

      other ->
        error_from_result(other)
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
