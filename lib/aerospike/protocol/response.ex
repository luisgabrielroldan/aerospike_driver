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
        case record_bins_from_operations(msg.operations) do
          {:ok, bins} ->
            {:ok,
             %Record{
               key: key,
               bins: bins,
               generation: msg.generation,
               ttl: msg.expiration
             }}

          {:error, _} = err ->
            err
        end

      other ->
        error_from_result(other)
    end
  end

  defp record_bins_from_operations(operations) do
    Enum.reduce_while(operations, {:ok, %{}}, fn
      %Operation{bin_name: ""}, {:ok, acc} ->
        {:cont, {:ok, acc}}

      %Operation{bin_name: name} = op, {:ok, acc} ->
        case Value.decode_value(op.particle_type, op.data) do
          {:ok, v} -> {:cont, {:ok, Map.put(acc, name, v)}}
          {:error, _} = err -> {:halt, err}
        end
    end)
  end

  @doc """
  Parses put/touch write responses (result code only).
  """
  @spec parse_write_response(AsmMsg.t()) :: :ok | {:error, Error.t()}
  def parse_write_response(%AsmMsg{} = msg) do
    case result_atom(msg.result_code) do
      {:ok, :ok} -> :ok
      other -> error_from_result(other)
    end
  end

  @doc """
  Parses delete responses: `true` if a record was deleted, `false` if key was missing.
  """
  @spec parse_delete_response(AsmMsg.t()) :: {:ok, boolean()} | {:error, Error.t()}
  def parse_delete_response(%AsmMsg{} = msg) do
    case result_atom(msg.result_code) do
      {:ok, :ok} -> {:ok, true}
      {:ok, :key_not_found} -> {:ok, false}
      other -> error_from_result(other)
    end
  end

  @doc """
  Parses exists responses.
  """
  @spec parse_exists_response(AsmMsg.t()) :: {:ok, boolean()} | {:error, Error.t()}
  def parse_exists_response(%AsmMsg{} = msg) do
    case result_atom(msg.result_code) do
      {:ok, :ok} -> {:ok, true}
      {:ok, :key_not_found} -> {:ok, false}
      other -> error_from_result(other)
    end
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
