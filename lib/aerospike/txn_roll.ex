defmodule Aerospike.TxnRoll do
  @moduledoc false

  import Bitwise

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Txn
  alias Aerospike.TxnMonitor
  alias Aerospike.TxnOps
  alias Aerospike.UnaryCommand
  alias Aerospike.UnarySupport

  @doc false
  @spec commit(atom(), Txn.t(), keyword()) ::
          {:ok, :committed | :already_committed} | {:error, Error.t()}
  def commit(conn_name, %Txn{} = txn, opts \\ []) do
    case TxnOps.get_tracking(conn_name, txn) do
      {:ok, %{state: :committed}} ->
        {:ok, :already_committed}

      {:ok, %{state: :aborted}} ->
        {:error, Error.from_result_code(:txn_already_aborted)}

      {:ok, %{state: :verified}} ->
        handle_roll_forward_mark(conn_name, txn, opts)

      {:ok, _} ->
        do_commit(conn_name, txn, opts)

      {:error, :not_found} ->
        {:error, %Error{code: :parameter_error, message: "transaction not initialized"}}
    end
  end

  @doc false
  @spec abort(atom(), Txn.t(), keyword()) ::
          {:ok, :aborted | :already_aborted} | {:error, Error.t()}
  def abort(conn_name, %Txn{} = txn, opts \\ []) do
    case TxnOps.get_tracking(conn_name, txn) do
      {:ok, %{state: :aborted}} ->
        {:ok, :already_aborted}

      {:ok, %{state: :committed}} ->
        {:error, Error.from_result_code(:txn_already_committed)}

      {:ok, _} ->
        do_abort(conn_name, txn, opts)

      {:error, :not_found} ->
        {:error, %Error{code: :parameter_error, message: "transaction not initialized"}}
    end
  end

  @doc false
  @spec txn_status(atom(), Txn.t()) ::
          {:ok, :open | :verified | :committed | :aborted} | {:error, Error.t()}
  def txn_status(conn_name, %Txn{} = txn) do
    case TxnOps.get_tracking(conn_name, txn) do
      {:ok, %{state: state}} ->
        {:ok, state}

      {:error, :not_found} ->
        {:error, %Error{code: :parameter_error, message: "transaction not initialized"}}
    end
  end

  @doc false
  @spec transaction(atom(), Txn.t() | keyword(), (Txn.t() -> term())) ::
          {:ok, term()} | {:error, Error.t()}
  def transaction(conn_name, txn_or_opts, fun)

  def transaction(conn_name, %Txn{} = txn, fun) when is_function(fun, 1) do
    TxnOps.init_tracking(conn_name, txn)
    run_transaction(conn_name, txn, fun)
  end

  def transaction(conn_name, opts, fun) when is_list(opts) and is_function(fun, 1) do
    txn = Txn.new(opts)
    TxnOps.init_tracking(conn_name, txn)
    run_transaction(conn_name, txn, fun)
  end

  defp run_transaction(conn_name, txn, fun) do
    result =
      try do
        {:ok, fun.(txn)}
      rescue
        e in Aerospike.Error ->
          abort(conn_name, txn, [])
          {:error, e}

        e ->
          abort(conn_name, txn, [])
          {:raise, e, __STACKTRACE__}
      catch
        kind, reason ->
          abort(conn_name, txn, [])
          {:throw_or_exit, kind, reason, __STACKTRACE__}
      end

    case result do
      {:ok, value} ->
        case commit(conn_name, txn, []) do
          {:ok, _} ->
            {:ok, value}

          {:error, _} = err ->
            best_effort_abort(conn_name, txn)
            err
        end

      {:error, _} = err ->
        err

      {:raise, e, stacktrace} ->
        reraise e, stacktrace

      {:throw_or_exit, kind, reason, stacktrace} ->
        :erlang.raise(kind, reason, stacktrace)
    end
  end

  defp do_commit(conn_name, txn, opts) do
    case verify_reads(conn_name, txn, opts) do
      :ok ->
        TxnOps.set_state(conn_name, txn, :verified)
        handle_roll_forward_mark(conn_name, txn, opts)

      {:error, _} = verify_err ->
        TxnOps.set_state(conn_name, txn, :aborted)
        roll_writes(conn_name, txn, opts, :back)
        close_if_needed(conn_name, txn, opts)
        TxnOps.cleanup(conn_name, txn)
        verify_err
    end
  end

  defp handle_roll_forward_mark(conn_name, txn, opts) do
    if TxnOps.monitor_exists?(conn_name, txn) do
      case TxnMonitor.mark_roll_forward(conn_name, txn, opts) do
        :ok ->
          finish_commit(conn_name, txn, opts)

        {:error, %Error{code: :mrt_committed}} ->
          finish_commit(conn_name, txn, opts)

        {:error, %Error{code: :mrt_aborted}} ->
          TxnOps.set_state(conn_name, txn, :aborted)
          {:error, Error.from_result_code(:mrt_aborted)}

        {:error, %Error{} = err} ->
          maybe_set_in_doubt(conn_name, txn, err)
          {:error, err}
      end
    else
      finish_commit(conn_name, txn, opts)
    end
  end

  defp maybe_set_in_doubt(conn_name, txn, %Error{in_doubt: true}),
    do: TxnOps.set_write_in_doubt(conn_name, txn)

  defp maybe_set_in_doubt(_conn_name, _txn, _err), do: :ok

  defp finish_commit(conn_name, txn, opts) do
    TxnOps.set_state(conn_name, txn, :committed)
    roll_writes(conn_name, txn, opts, :forward)
    close_if_needed(conn_name, txn, opts)
    TxnOps.cleanup(conn_name, txn)
    {:ok, :committed}
  end

  defp do_abort(conn_name, txn, opts) do
    TxnOps.set_state(conn_name, txn, :aborted)
    roll_writes(conn_name, txn, opts, :back)
    close_if_needed(conn_name, txn, opts)
    TxnOps.cleanup(conn_name, txn)
    {:ok, :aborted}
  end

  defp verify_reads(conn_name, txn, opts) do
    reads = TxnOps.get_reads(conn_name, txn)

    Enum.reduce_while(reads, :ok, fn {key, version}, :ok ->
      case run_verify_command(conn_name, key, opts, %{key: key, version: version}) do
        {:ok, _} -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp run_verify_command(conn_name, route_key, opts, input) do
    UnarySupport.run_command(conn_name, route_key, opts, verify_command(), input)
  end

  defp roll_writes(conn_name, txn, opts, direction) do
    writes = TxnOps.get_writes(conn_name, txn)

    Enum.each(writes, fn key ->
      _ =
        run_roll_command(conn_name, key, opts, %{key: key, txn_id: txn.id, direction: direction})
    end)
  end

  defp best_effort_abort(conn_name, txn) do
    _ = abort(conn_name, txn, [])
    _ = TxnOps.cleanup(conn_name, txn)
    :ok
  end

  defp close_if_needed(conn_name, txn, opts) do
    if TxnOps.close_monitor?(conn_name, txn) do
      TxnMonitor.close(conn_name, txn, opts)
    end
  end

  defp verify_command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :read,
      build_request: &encode_verify_request/1,
      parse_response: &parse_verify_response/2
    )
  end

  defp roll_command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :write,
      build_request: &encode_roll_request/1,
      parse_response: &parse_roll_response/2
    )
  end

  defp run_roll_command(conn_name, route_key, opts, input) do
    UnarySupport.run_command(conn_name, route_key, opts, roll_command(), input)
  end

  defp encode_verify_request(%{key: %Key{} = key, version: version}) do
    encode_verify_msg(key, version)
  end

  defp encode_roll_request(%{key: %Key{} = key, txn_id: txn_id, direction: direction}) do
    encode_roll_msg(key, txn_id, direction)
  end

  defp parse_verify_response(body, _input) do
    UnarySupport.parse_as_msg(body, fn msg ->
      case Response.parse_record_metadata_response(msg) do
        {:ok, _} -> {:ok, :ok}
        {:error, _} = err -> err
      end
    end)
  end

  defp parse_roll_response(body, _input) do
    UnarySupport.parse_as_msg(body, fn msg ->
      case Response.parse_record_metadata_response(msg) do
        {:ok, _} -> {:ok, :ok}
        {:error, _} = err -> err
      end
    end)
  end

  @doc false
  def encode_verify_msg(%Key{} = key, version) do
    %AsmMsg{
      info1: AsmMsg.info1_read() ||| AsmMsg.info1_nobindata(),
      info3: AsmMsg.info3_sc_read_type(),
      info4: AsmMsg.info4_mrt_verify_read(),
      fields: [
        Field.namespace(key.namespace),
        Field.set(key.set),
        Field.digest(key.digest),
        Field.record_version(version)
      ]
    }
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  @doc false
  def encode_roll_msg(%Key{} = key, txn_id, :forward) do
    encode_roll_msg_with_info4(key, txn_id, AsmMsg.info4_mrt_roll_forward())
  end

  @doc false
  def encode_roll_msg(%Key{} = key, txn_id, :back) do
    encode_roll_msg_with_info4(key, txn_id, AsmMsg.info4_mrt_roll_back())
  end

  defp encode_roll_msg_with_info4(%Key{} = key, txn_id, info4) do
    %AsmMsg{
      info2: AsmMsg.info2_write() ||| AsmMsg.info2_durable_delete(),
      info4: info4,
      fields: [
        Field.namespace(key.namespace),
        Field.set(key.set),
        Field.digest(key.digest),
        Field.mrt_id(txn_id)
      ]
    }
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end
end
