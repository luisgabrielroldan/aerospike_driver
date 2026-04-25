defmodule Aerospike.Runtime.TxnSupport do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Response
  alias Aerospike.Runtime.TxnMonitor
  alias Aerospike.Runtime.TxnOps
  alias Aerospike.Txn

  @spec txn_from_opts(keyword()) :: {:ok, Txn.t() | nil} | {:error, Error.t()}
  def txn_from_opts(opts) when is_list(opts) do
    case Keyword.get(opts, :txn) do
      nil ->
        {:ok, nil}

      %Txn{} = txn ->
        {:ok, txn}

      other ->
        {:error,
         Error.from_result_code(:invalid_argument,
           message: "txn must be a %Aerospike.Txn{}, got: #{inspect(other)}"
         )}
    end
  end

  @spec maybe_add_mrt_fields(AsmMsg.t(), atom(), Key.t(), keyword(), boolean()) :: AsmMsg.t()
  def maybe_add_mrt_fields(%AsmMsg{} = msg, conn, %Key{} = key, opts, has_write) do
    case txn_from_opts(opts) do
      {:ok, nil} -> msg
      {:ok, %Txn{} = txn} -> do_add_mrt_fields(msg, conn, txn, key, has_write)
      {:error, _} -> msg
    end
  end

  @spec prepare_txn_read(atom(), Txn.t() | nil, Key.t()) :: :ok | {:error, Error.t()}
  def prepare_txn_read(_conn, nil, _key), do: :ok

  def prepare_txn_read(conn, %Txn{} = txn, %Key{} = key) do
    with :ok <- TxnOps.verify_command(conn, txn) do
      TxnOps.set_namespace(conn, txn, key.namespace)
    end
  end

  @spec prepare_txn_write(atom(), Txn.t() | nil, Key.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def prepare_txn_write(_conn, nil, _key, _opts), do: :ok

  def prepare_txn_write(conn, %Txn{} = txn, %Key{} = key, opts) do
    TxnMonitor.register_key(conn, txn, key, opts)
  end

  @spec prepare_txn_for_operate(atom(), Txn.t() | nil, Key.t(), boolean(), keyword()) ::
          :ok | {:error, Error.t()}
  def prepare_txn_for_operate(conn, txn, key, true = _has_write, opts) do
    prepare_txn_write(conn, txn, key, opts)
  end

  def prepare_txn_for_operate(conn, txn, key, false = _has_write, _opts) do
    prepare_txn_read(conn, txn, key)
  end

  @spec track_txn_response(atom(), Txn.t() | nil, Key.t(), :read | :write, AsmMsg.t(), term()) ::
          :ok
  def track_txn_response(_conn, nil, _key, _kind, _msg, _result), do: :ok

  def track_txn_response(conn, %Txn{} = txn, key, :write, %AsmMsg{} = msg, result) do
    version = extract_response_version(msg)
    result_code = result_code_from_result(result)
    TxnOps.track_write(conn, txn, key, version, result_code)
    :ok
  end

  def track_txn_response(conn, %Txn{} = txn, key, :read, %AsmMsg{} = msg, _result) do
    version = extract_response_version(msg)
    TxnOps.track_read(conn, txn, key, version)
    :ok
  end

  @spec track_txn_in_doubt(atom(), Txn.t() | nil, Key.t(), Error.t()) :: :ok
  def track_txn_in_doubt(_conn, nil, _key, _error), do: :ok

  def track_txn_in_doubt(conn, %Txn{} = txn, key, %Error{code: code})
      when code in [:timeout, :network_error] do
    TxnOps.track_write_in_doubt(conn, txn, key)
    :ok
  end

  def track_txn_in_doubt(_conn, _txn, _key, _error), do: :ok

  @spec extract_response_version(AsmMsg.t()) :: term() | nil
  def extract_response_version(%AsmMsg{} = msg) do
    case Response.extract_record_version(msg) do
      {:ok, version} -> version
      :none -> nil
    end
  end

  defp do_add_mrt_fields(msg, conn, %Txn{} = txn, %Key{} = key, has_write) do
    deadline = TxnOps.get_deadline(conn, txn)
    version = TxnOps.get_read_version(conn, txn, key)

    mrt_fields = [Field.mrt_id(txn.id)]

    mrt_fields =
      case version do
        nil -> mrt_fields
        v -> mrt_fields ++ [Field.record_version(v)]
      end

    mrt_fields =
      if has_write and deadline != 0 do
        mrt_fields ++ [Field.mrt_deadline(deadline)]
      else
        mrt_fields
      end

    %{msg | fields: msg.fields ++ mrt_fields}
  end

  defp result_code_from_result({:ok, _}), do: :ok
  defp result_code_from_result({:error, %Error{code: code}}), do: code
end
