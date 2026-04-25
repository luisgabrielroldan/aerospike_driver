defmodule Aerospike.Command.Get do
  @moduledoc false

  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Command.UnarySupport
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Record
  alias Aerospike.Runtime.TxnSupport

  @type mode :: :all | :header

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:replica_policy, :master | :sequence}
          | {:filter, Aerospike.Exp.t() | nil}

  @doc """
  Reads `key` from the cluster identified by `tender`.

  `tender` is a running `Aerospike.Cluster.Tender` (pid or registered name). The
  spike only supports reading every bin (`mode = :all`) or record metadata only
  (`mode = :header`); other shapes return
  `{:error, %Aerospike.Error{code: :invalid_argument}}` so future tasks can widen
  the API without changing the signature.

  Options:

    * `:timeout` — total op-budget milliseconds shared across the
      initial attempt and every retry. Default `5_000`.
    * `:max_retries` — overrides the cluster-default retry cap. `0`
      disables retry entirely.
    * `:sleep_between_retries_ms` — fixed delay between attempts.
    * `:replica_policy` — `:master` or `:sequence`.
    * `:filter` — non-empty `%Aerospike.Exp{}` server-side filter
      expression, or `nil` for no filter.

  Errors surface as typed `Aerospike.Error` structs or the router's
  `:cluster_not_ready` / `:no_master` atoms for routing failures.
  """
  @spec execute(GenServer.server(), Key.t(), mode() | term(), [option()]) ::
          {:ok, Record.t()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def execute(tender, key, bins, opts \\ [])

  def execute(tender, %Key{} = key, mode, opts) when mode in [:all, :header] do
    with {:ok, txn} <- TxnSupport.txn_from_opts(opts),
         {:ok, policy} <- UnarySupport.read_policy(tender, opts),
         :ok <- TxnSupport.prepare_txn_read(tender, txn, key) do
      UnarySupport.run_command(
        tender,
        key,
        policy,
        command(),
        %{key: key, conn: tender, txn: txn, opts: opts, mode: mode, filter: policy.filter}
      )
    end
  end

  def execute(_tender, %Key{}, _bins, _opts) do
    {:error,
     %Error{
       code: :invalid_argument,
       message: "Aerospike.Command.Get supports only :all and :header read modes in the spike"
     }}
  end

  defp command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :read,
      build_request: &encode_read/1,
      parse_response: &parse_record_response/2
    )
  end

  defp encode_read(%{key: %Key{} = key, conn: conn, opts: opts, mode: mode, filter: filter}) do
    key
    |> AsmMsg.key_command([], read: true, read_all: mode == :all, read_header: mode == :header)
    |> AsmMsg.maybe_add_filter_exp(filter)
    |> TxnSupport.maybe_add_mrt_fields(conn, key, opts, false)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_record_response(body, %{key: key, conn: conn, txn: txn}) do
    UnarySupport.parse_as_msg(body, fn msg ->
      case Response.parse_record_response(msg, key) do
        {:ok, _} = ok ->
          TxnSupport.track_txn_response(conn, txn, key, :read, msg, ok)
          ok

        {:error, _} = err ->
          err
      end
    end)
  end
end
