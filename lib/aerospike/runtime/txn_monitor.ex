defmodule Aerospike.Runtime.TxnMonitor do
  @moduledoc false

  import Bitwise

  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Command.UnarySupport
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
  alias Aerospike.Runtime.TxnOps
  alias Aerospike.Txn

  @monitor_set "<ERO~MRT"
  @list_append_op 1
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
      true -> :ok
      {:error, _} = err -> err
    end
  end

  defp do_register_key(conn_name, txn, cmd_key, opts) do
    ns = cmd_key.namespace
    mkey = monitor_key(txn, ns)
    input = monitor_input(:register, conn_name, txn, ns, cmd_key, mkey, opts)

    case run_monitor_command(conn_name, mkey, opts, input) do
      {:ok, _} -> :ok
      {:error, _} = err -> err
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

  @doc false
  @spec mark_roll_forward(atom(), Txn.t(), keyword()) :: :ok | {:error, Error.t()}
  def mark_roll_forward(conn_name, %Txn{} = txn, opts \\ []) do
    with {:ok, ns} <- fetch_namespace(conn_name, txn) do
      mkey = monitor_key(txn, ns)
      input = monitor_input(:roll_forward, conn_name, txn, ns, nil, mkey, opts)

      case run_monitor_command(conn_name, mkey, opts, input) do
        {:ok, _} -> :ok
        {:error, _} = err -> err
      end
    end
  end

  @doc false
  @spec close(atom(), Txn.t(), keyword()) :: :ok | {:error, Error.t()}
  def close(conn_name, %Txn{} = txn, opts \\ []) do
    with {:ok, ns} <- fetch_namespace(conn_name, txn) do
      mkey = monitor_key(txn, ns)
      input = monitor_input(:close, conn_name, txn, ns, nil, mkey, opts)

      case run_monitor_command(conn_name, mkey, opts, input) do
        {:ok, _} -> :ok
        {:error, _} = err -> err
      end
    end
  end

  defp run_monitor_command(conn_name, route_key, opts, input) do
    with {:ok, policy} <- UnarySupport.write_policy(conn_name, opts) do
      UnarySupport.run_command(conn_name, route_key, policy, monitor_command(), input)
    end
  end

  defp monitor_command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :write,
      build_request: &encode_monitor_request/1,
      parse_response: &parse_monitor_response/2
    )
  end

  defp monitor_input(kind, conn_name, txn, namespace, cmd_key, mkey, opts) do
    %{
      kind: kind,
      conn_name: conn_name,
      txn: txn,
      namespace: namespace,
      digest: mkey.digest,
      opts: opts,
      operations: monitor_operations(kind, conn_name, txn, cmd_key),
      info2: monitor_info2(kind),
      expiration: monitor_expiration(kind, txn)
    }
  end

  defp monitor_operations(:register, conn_name, txn, cmd_key) do
    register_ops(conn_name, txn, cmd_key)
  end

  defp monitor_operations(:roll_forward, _conn_name, _txn, _cmd_key) do
    Value.encode_bin_operations(%{"fwd" => true})
  end

  defp monitor_operations(:close, _conn_name, _txn, _cmd_key), do: []

  defp monitor_info2(:register) do
    AsmMsg.info2_write() ||| AsmMsg.info2_respond_all_ops()
  end

  defp monitor_info2(:roll_forward), do: AsmMsg.info2_write()

  defp monitor_info2(:close) do
    AsmMsg.info2_write() ||| AsmMsg.info2_delete() ||| AsmMsg.info2_durable_delete()
  end

  defp monitor_expiration(:register, %Txn{} = txn), do: div(txn.timeout, 1000)
  defp monitor_expiration(_kind, _txn), do: 0

  defp encode_monitor_request(%{
         namespace: namespace,
         digest: digest,
         info2: info2,
         expiration: expiration,
         operations: operations
       }) do
    encode_monitor_msg(namespace, digest, info2, expiration, operations)
  end

  defp parse_monitor_response(body, %{kind: :register, conn_name: conn_name, txn: txn}) do
    UnarySupport.parse_as_msg(body, fn msg ->
      with :ok <- check_result_code(msg) do
        set_monitor_deadline(msg, conn_name, txn)
      end
    end)
  end

  defp parse_monitor_response(body, _input) do
    UnarySupport.parse_as_msg(body, fn msg ->
      case check_result_code(msg) do
        :ok -> {:ok, :ok}
        {:error, _} = err -> err
      end
    end)
  end

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

  defp set_monitor_deadline(msg, conn_name, txn) do
    case Response.extract_mrt_deadline(msg) do
      {:ok, deadline} ->
        TxnOps.set_deadline(conn_name, txn, deadline)
        {:ok, :ok}

      :none ->
        {:ok, :ok}
    end
  end
end
