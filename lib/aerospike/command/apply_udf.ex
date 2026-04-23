defmodule Aerospike.Command.ApplyUdf do
  @moduledoc """
  Unary single-record UDF execution adapter for the spike.

  This keeps record UDF apply on the same routed unary execution seam as the
  write family, while isolating UDF-specific request encoding and reply parsing
  from ordinary CRUD responses.
  """

  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Command.UnarySupport
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Protocol.UdfArgs
  alias Aerospike.Runtime.TxnSupport

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:ttl, non_neg_integer()}
          | {:generation, non_neg_integer()}

  @type result ::
          {:ok, term()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}

  @spec execute(GenServer.server(), Key.t(), String.t(), String.t(), list(), [option()]) ::
          result()
  def execute(tender, %Key{} = key, package, function, args, opts \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    with {:ok, txn} <- TxnSupport.txn_from_opts(opts),
         {:ok, policy} <- UnarySupport.write_policy(tender, opts),
         :ok <- TxnSupport.prepare_txn_write(tender, txn, key, opts) do
      input = command_input(tender, key, package, function, args, opts, txn, policy)

      tender
      |> UnarySupport.run_command(key, policy, command(), input)
      |> maybe_track_txn_in_doubt(tender, txn, key)
    end
  end

  defp command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :write,
      retry_transport: false,
      build_request: &encode_udf/1,
      parse_response: &parse_udf_response/2
    )
  end

  defp command_input(
         conn,
         key,
         package,
         function,
         args,
         opts,
         txn,
         %Policy.UnaryWrite{} = policy
       ) do
    %{
      conn: conn,
      key: key,
      package: package,
      function: function,
      args: args,
      opts: opts,
      txn: txn,
      ttl: policy.ttl,
      generation: policy.generation,
      timeout: policy.timeout
    }
  end

  defp maybe_track_txn_in_doubt({:error, %Error{} = err} = result, tender, txn, key) do
    TxnSupport.track_txn_in_doubt(tender, txn, key, err)
    result
  end

  defp maybe_track_txn_in_doubt(result, _tender, _txn, _key), do: result

  defp encode_udf(%{
         conn: conn,
         key: %Key{} = key,
         package: package,
         function: function,
         args: args,
         opts: opts,
         ttl: ttl,
         generation: generation,
         timeout: timeout
       }) do
    %AsmMsg{
      info2: AsmMsg.info2_write(),
      generation: generation,
      expiration: ttl,
      timeout: timeout,
      fields:
        AsmMsg.key_fields(key) ++
          [
            Field.udf_package_name(package),
            Field.udf_function(function),
            Field.udf_arglist(UdfArgs.pack!(args)),
            Field.udf_op(1)
          ]
    }
    |> TxnSupport.maybe_add_mrt_fields(conn, key, opts, true)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_udf_response(body, %{key: key, conn: conn, txn: txn}) do
    UnarySupport.parse_as_msg(body, fn msg ->
      case Response.parse_udf_response(msg) do
        {:ok, _} = ok ->
          TxnSupport.track_txn_response(conn, txn, key, :write, msg, ok)
          ok

        {:error, _} = err ->
          err
      end
    end)
  end
end
