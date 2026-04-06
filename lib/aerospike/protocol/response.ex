defmodule Aerospike.Protocol.Response do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
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

  @doc """
  Parses a UDF execute response.

  On success, returns `{:ok, value}` where value is the UDF return value extracted
  from the `SUCCESS` bin. Returns `{:error, Error.t()}` on UDF runtime error
  (code 100, `FAILURE` bin) or server error.
  """
  @spec parse_udf_response(AsmMsg.t()) :: {:ok, term()} | {:error, Error.t()}
  def parse_udf_response(%AsmMsg{} = msg) do
    case result_atom(msg.result_code) do
      {:ok, :ok} ->
        case record_bins_from_operations(msg.operations) do
          {:ok, bins} -> extract_udf_value(bins)
          {:error, _} = err -> err
        end

      {:ok, :udf_bad_response} ->
        extract_udf_error(msg.operations)

      other ->
        error_from_result(other)
    end
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

  @doc """
  Extracts the RECORD_VERSION from the fields of an AS_MSG response.

  Returns `{:ok, version}` where version is a non-negative integer (7-byte
  little-endian uint64 from the wire), or `:none` if the field is absent.
  """
  @spec extract_record_version(AsmMsg.t()) :: {:ok, non_neg_integer()} | :none
  def extract_record_version(%AsmMsg{fields: fields}) do
    rv_type = Field.type_record_version()

    case Enum.find(fields, fn f -> f.type == rv_type end) do
      %Field{data: <<v::56-little-unsigned>>} -> {:ok, v}
      nil -> :none
    end
  end

  @doc """
  Extracts the MRT_DEADLINE from the fields of an AS_MSG response.

  Returns `{:ok, deadline}` where deadline is a little-endian signed int32
  returned by the server, or `:none` if the field is absent.
  """
  @spec extract_mrt_deadline(AsmMsg.t()) :: {:ok, integer()} | :none
  def extract_mrt_deadline(%AsmMsg{fields: fields}) do
    dl_type = Field.type_mrt_deadline()

    case Enum.find(fields, fn f -> f.type == dl_type end) do
      %Field{data: <<d::32-signed-little>>} -> {:ok, d}
      nil -> :none
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
