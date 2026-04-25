defmodule Aerospike.Command.Touch do
  @moduledoc false

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
      build_request: &encode_touch/1,
      parse_response: &parse_metadata_response/2
    )
  end

  defp command_input(conn, key, txn, opts, %Policy.UnaryWrite{} = policy) do
    %{
      key: key,
      conn: conn,
      txn: txn,
      opts: opts,
      ttl: policy.ttl,
      generation: policy.generation,
      filter: policy.filter
    }
  end

  defp encode_touch(%{
         key: %Key{} = key,
         conn: conn,
         opts: opts,
         ttl: ttl,
         generation: generation,
         filter: filter
       }) do
    key
    |> AsmMsg.key_command([Operation.touch()],
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
