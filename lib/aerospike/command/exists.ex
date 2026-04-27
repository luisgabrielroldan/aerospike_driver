defmodule Aerospike.Command.Exists do
  @moduledoc false

  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Command.UnarySupport
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Runtime.TxnSupport

  @type option ::
          {:timeout, non_neg_integer()}
          | {:socket_timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:replica_policy, :master | :sequence}
          | {:filter, Aerospike.Exp.t() | nil}
          | {:read_mode_ap, :one | :all}
          | {:read_mode_sc, :session | :linearize | :allow_replica | :allow_unavailable}
          | {:read_touch_ttl_percent, -1 | 0..100}
          | {:send_key, boolean()}
          | {:use_compression, boolean()}

  @type result ::
          {:ok, boolean()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}

  @spec execute(GenServer.server(), Key.t(), [option()]) :: result()
  def execute(tender, %Key{} = key, opts \\ []) when is_list(opts) do
    with {:ok, txn} <- TxnSupport.txn_from_opts(opts),
         {:ok, policy} <- UnarySupport.read_policy(tender, opts),
         :ok <- TxnSupport.prepare_txn_read(tender, txn, key) do
      UnarySupport.run_command(
        tender,
        key,
        policy,
        command(),
        %{key: key, conn: tender, txn: txn, opts: opts, policy: policy, filter: policy.filter}
      )
    end
  end

  defp command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :read,
      build_request: &encode_exists/1,
      parse_response: &parse_exists_response/2
    )
  end

  defp encode_exists(%{
         key: %Key{} = key,
         conn: conn,
         opts: opts,
         policy: policy,
         use_compression: use_compression,
         filter: filter
       }) do
    key
    |> AsmMsg.key_command(
      [],
      [read: true, read_header: true] ++ UnarySupport.read_header_opts(policy, use_compression)
    )
    |> AsmMsg.maybe_add_filter_exp(filter)
    |> TxnSupport.maybe_add_mrt_fields(conn, key, opts, false)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_exists_response(body, %{key: key, conn: conn, txn: txn}) do
    UnarySupport.parse_as_msg(body, fn msg ->
      case exists_result(msg) do
        {:ok, _} = ok ->
          TxnSupport.track_txn_response(conn, txn, key, :read, msg, ok)
          ok

        {:error, _} = err ->
          err
      end
    end)
  end

  defp exists_result(msg) do
    case Response.parse_record_metadata_response(msg) do
      {:ok, _metadata} -> {:ok, true}
      {:error, %Error{code: :key_not_found}} -> {:ok, false}
      {:error, %Error{}} = err -> err
    end
  end
end
