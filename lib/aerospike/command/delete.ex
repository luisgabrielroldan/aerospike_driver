defmodule Aerospike.Command.Delete do
  @moduledoc """
  DELETE command adapter for the spike.

  Successful deletes return whether the record existed before removal.
  Missing-record replies stay distinguishable from successful deletes.
  """

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Runtime.TxnSupport
  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Command.UnarySupport

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:generation, non_neg_integer()}
          | {:filter, Aerospike.Exp.t() | nil}

  @type result ::
          {:ok, boolean()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}

  @spec execute(GenServer.server(), Key.t(), [option()]) :: result()
  def execute(tender, %Key{} = key, opts \\ []) when is_list(opts) do
    with {:ok, txn} <- TxnSupport.txn_from_opts(opts),
         {:ok, policy} <- UnarySupport.write_policy(tender, opts),
         :ok <- TxnSupport.prepare_txn_write(tender, txn, key, opts) do
      result =
        UnarySupport.run_command(
          tender,
          key,
          policy,
          command(),
          command_input(tender, key, txn, opts, policy)
        )

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
      build_request: &encode_delete/1,
      parse_response: &parse_delete_response/2
    )
  end

  defp command_input(conn, key, txn, opts, %Policy.UnaryWrite{} = policy) do
    %{
      key: key,
      conn: conn,
      txn: txn,
      opts: opts,
      generation: policy.generation,
      filter: policy.filter
    }
  end

  defp encode_delete(%{
         key: %Key{} = key,
         conn: conn,
         opts: opts,
         generation: generation,
         filter: filter
       }) do
    key
    |> AsmMsg.key_command([],
      write: true,
      delete: true,
      send_key: true,
      generation: generation
    )
    |> AsmMsg.maybe_add_filter_exp(filter)
    |> TxnSupport.maybe_add_mrt_fields(conn, key, opts, true)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_delete_response(body, %{key: key, conn: conn, txn: txn}) do
    UnarySupport.parse_as_msg(body, fn msg ->
      case delete_result(msg) do
        {:ok, _} = ok ->
          TxnSupport.track_txn_response(conn, txn, key, :write, msg, ok)
          ok

        {:error, _} = err ->
          err
      end
    end)
  end

  defp delete_result(msg) do
    case Response.parse_record_metadata_response(msg) do
      {:ok, _metadata} -> {:ok, true}
      {:error, %Error{code: :key_not_found}} -> {:ok, false}
      {:error, %Error{}} = err -> err
    end
  end
end
