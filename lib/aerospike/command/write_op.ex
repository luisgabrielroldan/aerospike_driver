defmodule Aerospike.Command.WriteOp do
  @moduledoc """
  Unary write adapter for simple add/append/prepend bin mutations.
  """

  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Command.UnarySupport
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Record
  alias Aerospike.Runtime.TxnSupport

  @type kind :: :add | :append | :prepend

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:ttl, non_neg_integer()}
          | {:generation, non_neg_integer()}
          | {:filter, Aerospike.Exp.t() | nil}

  @type result ::
          {:ok, Record.metadata()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}

  @spec execute(GenServer.server(), Key.t(), kind(), Record.bins_input(), [option()]) :: result()
  def execute(tender, %Key{} = key, kind, bins, opts \\ [])
      when kind in [:add, :append, :prepend] and is_list(opts) do
    with {:ok, txn} <- TxnSupport.txn_from_opts(opts),
         {:ok, operations} <- write_operations(kind, bins),
         {:ok, policy} <- UnarySupport.write_policy(tender, opts),
         {:ok, input} <- command_input(tender, key, operations, opts, txn, policy),
         :ok <- TxnSupport.prepare_txn_write(tender, txn, key, opts) do
      result = UnarySupport.run_command(tender, key, policy, command(), input)

      case result do
        {:error, %Error{} = err} ->
          TxnSupport.track_txn_in_doubt(tender, txn, key, err)
          result

        _ ->
          result
      end
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

  defp command_input(tender, key, operations, opts, txn, %Policy.UnaryWrite{} = policy) do
    {:ok,
     %{
       key: key,
       conn: tender,
       txn: txn,
       opts: opts,
       operations: operations,
       ttl: policy.ttl,
       generation: policy.generation,
       filter: policy.filter
     }}
  end

  defp write_operations(kind, bins) when is_map(bins) and map_size(bins) > 0 do
    bins
    |> Enum.reduce_while({:ok, []}, fn {bin_name, value}, {:ok, acc} ->
      case build_operation(kind, normalize_bin_name(bin_name), value) do
        {:ok, operation} -> {:cont, {:ok, [operation | acc]}}
        {:error, %Error{}} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, operations} -> {:ok, Enum.reverse(operations)}
      {:error, %Error{}} = err -> err
    end
  end

  defp write_operations(kind, bins) when is_map(bins) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "#{verb_name(kind)} requires at least one bin mutation"
     )}
  end

  defp write_operations(kind, other) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "#{verb_name(kind)} bins must be a non-empty map, got: #{inspect(other)}"
     )}
  end

  defp build_operation(:add, bin_name, value), do: Operation.add(bin_name, value)
  defp build_operation(:append, bin_name, value), do: Operation.append(bin_name, value)
  defp build_operation(:prepend, bin_name, value), do: Operation.prepend(bin_name, value)

  defp normalize_bin_name(bin_name) when is_atom(bin_name), do: Atom.to_string(bin_name)
  defp normalize_bin_name(bin_name), do: bin_name

  defp verb_name(:add), do: "add"
  defp verb_name(:append), do: "append"
  defp verb_name(:prepend), do: "prepend"

  defp encode_write(%{
         key: %Key{} = key,
         conn: conn,
         opts: opts,
         operations: operations,
         ttl: ttl,
         generation: generation,
         filter: filter
       }) do
    key
    |> AsmMsg.key_command(operations,
      write: true,
      send_key: true,
      ttl: ttl,
      generation: generation
    )
    |> AsmMsg.maybe_add_filter_exp(filter)
    |> TxnSupport.maybe_add_mrt_fields(conn, key, opts, true)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_metadata_response(body, %{key: key, conn: conn, txn: txn}) do
    UnarySupport.parse_as_msg(body, fn msg ->
      case Response.parse_record_metadata_response(msg) do
        {:ok, _} = ok ->
          TxnSupport.track_txn_response(conn, txn, key, :write, msg, ok)
          ok

        {:error, _} = err ->
          err
      end
    end)
  end
end
