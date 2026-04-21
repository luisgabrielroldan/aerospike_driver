defmodule Aerospike.Put do
  @moduledoc """
  PUT command adapter for the spike.

  This module builds a simple unary write from a key plus bin map and
  delegates routing, checkout, retry, and transport flow to
  `Aerospike.UnaryExecutor`.
  """

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Record
  alias Aerospike.UnaryCommand
  alias Aerospike.UnarySupport

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:ttl, non_neg_integer()}
          | {:generation, non_neg_integer()}

  @type result ::
          {:ok, Record.metadata()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}

  @spec execute(GenServer.server(), Key.t(), Record.bins_input(), [option()]) :: result()
  def execute(tender, %Key{} = key, bins, opts \\ []) when is_list(opts) do
    with {:ok, operations} <- write_operations(bins),
         {:ok, input} <- command_input(key, operations, opts) do
      UnarySupport.run_command(tender, key, opts, command(), input)
    end
  end

  defp command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :write,
      build_request: &encode_write/1,
      parse_response: &parse_metadata_response/2
    )
  end

  defp command_input(key, operations, opts) do
    with {:ok, ttl} <- non_negative_opt(opts, :ttl),
         {:ok, generation} <- non_negative_opt(opts, :generation) do
      {:ok, %{key: key, operations: operations, ttl: ttl, generation: generation}}
    end
  end

  defp write_operations(bins) when is_map(bins) and map_size(bins) > 0 do
    bins
    |> Enum.reduce_while({:ok, []}, fn {bin_name, value}, {:ok, acc} ->
      case Operation.write(normalize_bin_name(bin_name), value) do
        {:ok, operation} -> {:cont, {:ok, [operation | acc]}}
        {:error, %Error{}} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, operations} -> {:ok, Enum.reverse(operations)}
      {:error, %Error{}} = err -> err
    end
  end

  defp write_operations(bins) when is_map(bins) do
    {:error,
     Error.from_result_code(:invalid_argument, message: "PUT requires at least one bin write")}
  end

  defp write_operations(other) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "PUT bins must be a non-empty map, got: #{inspect(other)}"
     )}
  end

  defp normalize_bin_name(bin_name) when is_atom(bin_name), do: Atom.to_string(bin_name)
  defp normalize_bin_name(bin_name), do: bin_name

  defp non_negative_opt(opts, key) do
    case Keyword.get(opts, key, 0) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      value -> invalid_non_negative_opt(key, value)
    end
  end

  defp invalid_non_negative_opt(key, value) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "#{key} must be a non-negative integer, got: #{inspect(value)}"
     )}
  end

  defp encode_write(%{
         key: %Key{} = key,
         operations: operations,
         ttl: ttl,
         generation: generation
       }) do
    key
    |> AsmMsg.key_command(operations,
      write: true,
      send_key: true,
      ttl: ttl,
      generation: generation
    )
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_metadata_response(body, _input) do
    UnarySupport.parse_as_msg(body, &Response.parse_record_metadata_response/1)
  end
end
