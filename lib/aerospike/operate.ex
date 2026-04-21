defmodule Aerospike.Operate do
  @moduledoc """
  OPERATE command adapter for the spike.

  The supported surface stays intentionally narrow:

    * `{:write, bin, value}` for simple bin writes
    * `{:read, bin}` for bin reads that follow those writes

  This command requires at least one write operation so the spike can
  keep routing honest on the master path without introducing per-input
  dispatch selection.
  """

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.UnaryCommand
  alias Aerospike.UnarySupport

  @type simple_operation ::
          {:read, String.t() | atom()}
          | {:write, String.t() | atom(), term()}

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:ttl, non_neg_integer()}
          | {:generation, non_neg_integer()}

  @type result ::
          {:ok, [term()]}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}

  @spec execute(GenServer.server(), Key.t(), [simple_operation()], [option()]) :: result()
  def execute(tender, %Key{} = key, operations, opts \\ []) when is_list(opts) do
    with {:ok, built_operations, has_read?} <- build_operations(operations),
         :ok <- ensure_write_present(built_operations),
         {:ok, ttl} <- non_negative_opt(opts, :ttl),
         {:ok, generation} <- non_negative_opt(opts, :generation) do
      UnarySupport.run_command(
        tender,
        key,
        opts,
        command(),
        %{
          key: key,
          operations: built_operations,
          operation_count: length(built_operations),
          has_read?: has_read?,
          ttl: ttl,
          generation: generation
        }
      )
    end
  end

  defp command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :write,
      build_request: &encode_operate/1,
      parse_response: &parse_operate_response/2
    )
  end

  defp build_operations(operations) when is_list(operations) and operations != [] do
    operations
    |> Enum.reduce_while({:ok, [], false}, fn operation, {:ok, acc, has_read?} ->
      case build_operation(operation) do
        {:ok, built_operation, operation_kind} ->
          {:cont, {:ok, [built_operation | acc], has_read? or operation_kind == :read}}

        {:error, %Error{}} = err ->
          {:halt, err}
      end
    end)
    |> case do
      {:ok, built_operations, has_read?} -> {:ok, Enum.reverse(built_operations), has_read?}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_operations([]) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "operate requires at least one operation"
     )}
  end

  defp build_operations(other) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "operate operations must be a list, got: #{inspect(other)}"
     )}
  end

  defp build_operation({:read, bin_name}) do
    case Operation.from_simple({:read, normalize_bin_name(bin_name)}) do
      {:ok, operation} -> {:ok, operation, :read}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_operation({:write, bin_name, value}) do
    case Operation.from_simple({:write, normalize_bin_name(bin_name), value}) do
      {:ok, operation} -> {:ok, operation, :write}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_operation(other) do
    Operation.from_simple(other)
  end

  defp ensure_write_present(operations) do
    if Enum.any?(operations, &(&1.op_type == Operation.op_write())) do
      :ok
    else
      {:error,
       Error.from_result_code(:invalid_argument,
         message:
           "operate requires at least one write operation in this spike; read-only operate remains out of scope"
       )}
    end
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

  defp encode_operate(%{
         key: %Key{} = key,
         operations: operations,
         has_read?: has_read?,
         ttl: ttl,
         generation: generation
       }) do
    key
    |> AsmMsg.key_command(operations,
      read: has_read?,
      write: true,
      send_key: true,
      respond_all_ops: true,
      ttl: ttl,
      generation: generation
    )
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_operate_response(body, %{operation_count: operation_count}) do
    UnarySupport.parse_as_msg(body, &Response.parse_operate_response(&1, operation_count))
  end
end
