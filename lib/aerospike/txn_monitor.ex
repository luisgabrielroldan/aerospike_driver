defmodule Aerospike.TxnMonitor do
  @moduledoc false

  import Bitwise

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.CDT
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Protocol.ResultCode
  alias Aerospike.Router
  alias Aerospike.Txn
  alias Aerospike.TxnOps

  @monitor_set "<ERO~MRT"
  @list_append_op 1

  # List policy: ordered (1) + AddUnique | NoFail | Partial (13)
  @keyds_list_policy %{order: 1, flags: 13}

  @doc false
  @spec monitor_key(Txn.t(), String.t()) :: Key.t()
  def monitor_key(%Txn{} = txn, namespace) when is_binary(namespace) do
    Key.new(namespace, @monitor_set, txn.id)
  end

  @doc false
  @spec register_key(atom(), Txn.t(), Key.t(), keyword()) :: :ok | {:error, Error.t()}
  def register_key(conn_name, %Txn{} = txn, %Key{} = cmd_key, opts \\ []) do
    with :ok <- TxnOps.verify_command(conn_name, txn),
         false <- TxnOps.write_exists?(conn_name, txn, cmd_key),
         :ok <- TxnOps.set_namespace(conn_name, txn, cmd_key.namespace) do
      do_register_key(conn_name, txn, cmd_key, opts)
    else
      true ->
        :ok

      {:error, _} = err ->
        err
    end
  end

  defp do_register_key(conn_name, txn, cmd_key, opts) do
    ns = cmd_key.namespace
    ops = register_ops(conn_name, txn, cmd_key)
    mkey = monitor_key(txn, ns)

    wire =
      encode_monitor_msg(
        ns,
        mkey.digest,
        AsmMsg.info2_write() ||| AsmMsg.info2_respond_all_ops(),
        div(txn.timeout, 1000),
        ops
      )

    case Router.run(conn_name, mkey, wire, opts) do
      {:ok, body, _node} ->
        handle_register_response(conn_name, txn, body)

      {:error, _} = err ->
        err
    end
  end

  defp register_ops(conn_name, txn, cmd_key) do
    append_op =
      CDT.list_modify_op(
        "keyds",
        @list_append_op,
        [cmd_key.digest, @keyds_list_policy.order, @keyds_list_policy.flags]
      )

    if TxnOps.monitor_exists?(conn_name, txn) do
      [append_op]
    else
      put_id_ops = Value.encode_bin_operations(%{"id" => txn.id})
      put_id_ops ++ [append_op]
    end
  end

  defp handle_register_response(conn_name, txn, body) do
    with {:ok, msg} <- AsmMsg.decode(body),
         :ok <- check_result_code(msg) do
      case Response.extract_mrt_deadline(msg) do
        {:ok, deadline} ->
          TxnOps.set_deadline(conn_name, txn, deadline)
          :ok

        :none ->
          :ok
      end
    end
  end

  @doc false
  @spec mark_roll_forward(atom(), Txn.t(), keyword()) :: :ok | {:error, Error.t()}
  def mark_roll_forward(conn_name, %Txn{} = txn, opts \\ []) do
    with {:ok, ns} <- fetch_namespace(conn_name, txn) do
      mkey = monitor_key(txn, ns)
      ops = Value.encode_bin_operations(%{"fwd" => true})

      wire = encode_monitor_msg(ns, mkey.digest, AsmMsg.info2_write(), 0, ops)
      send_and_check(conn_name, mkey, wire, opts)
    end
  end

  @doc false
  @spec close(atom(), Txn.t(), keyword()) :: :ok | {:error, Error.t()}
  def close(conn_name, %Txn{} = txn, opts \\ []) do
    with {:ok, ns} <- fetch_namespace(conn_name, txn) do
      mkey = monitor_key(txn, ns)
      info2 = AsmMsg.info2_write() ||| AsmMsg.info2_delete() ||| AsmMsg.info2_durable_delete()

      wire = encode_monitor_msg(ns, mkey.digest, info2, 0, [])
      send_and_check(conn_name, mkey, wire, opts)
    end
  end

  @spec fetch_namespace(atom(), Txn.t()) :: {:ok, String.t()} | {:error, Error.t()}
  defp fetch_namespace(conn_name, txn) do
    case TxnOps.get_tracking(conn_name, txn) do
      {:ok, %{namespace: ns}} when is_binary(ns) ->
        {:ok, ns}

      {:ok, %{namespace: nil}} ->
        {:error, %Error{code: :parameter_error, message: "transaction has no namespace set"}}

      {:error, :not_found} ->
        {:error, %Error{code: :parameter_error, message: "transaction not initialized"}}
    end
  end

  defp send_and_check(conn_name, key, wire, opts) do
    with {:ok, body, _node} <- Router.run(conn_name, key, wire, opts),
         {:ok, msg} <- AsmMsg.decode(body) do
      check_result_code(msg)
    end
  end

  @doc false
  @spec encode_monitor_msg(String.t(), binary(), non_neg_integer(), non_neg_integer(), [
          Operation.t()
        ]) :: iodata()
  def encode_monitor_msg(namespace, digest, info2, expiration, operations) do
    %AsmMsg{
      info2: info2,
      expiration: expiration,
      fields: [
        Field.namespace(namespace),
        Field.set(@monitor_set),
        Field.digest(digest)
      ],
      operations: operations
    }
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  @doc false
  def check_result_code(%AsmMsg{result_code: 0}), do: :ok

  @doc false
  def check_result_code(%AsmMsg{result_code: code}) do
    case ResultCode.from_integer(code) do
      {:ok, atom} ->
        {:error, Error.from_result_code(atom)}

      {:error, _} ->
        {:error, Error.from_result_code(:server_error, message: "result code #{code}")}
    end
  end
end
