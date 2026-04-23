defmodule Aerospike.Command.Get do
  @moduledoc """
  GET command adapter for the spike.

  `Aerospike.Runtime.Executor` owns the shared unary control flow:
  routing, node-handle resolution, breaker checks, pool checkout,
  transport dispatch, reply classification, and retry budgeting.

  This module stays intentionally narrow. Its responsibilities are:

    * reject spike-unsupported GET shapes such as named-bin requests
    * build the read request for a `%Aerospike.Key{}`
    * parse the reply body into the GET result surface
    * trigger an asynchronous tend when the shared retry driver sees a
      rebalance response
  """

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Record
  alias Aerospike.Runtime.TxnSupport
  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Command.UnarySupport

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:replica_policy, :master | :sequence}

  @doc """
  Reads all bins for `key` from the cluster identified by `tender`.

  `tender` is a running `Aerospike.Cluster.Tender` (pid or registered name). The
  spike only supports reading every bin (`bins = :all`); other shapes
  return `{:error, %Aerospike.Error{code: :invalid_argument}}` so future
  tasks can widen the API without changing the signature.

  Options:

    * `:timeout` — total op-budget milliseconds shared across the
      initial attempt and every retry. Default `5_000`.
    * `:max_retries` — overrides the cluster-default retry cap. `0`
      disables retry entirely.
    * `:sleep_between_retries_ms` — fixed delay between attempts.
    * `:replica_policy` — `:master` or `:sequence`.

  Errors surface as typed `Aerospike.Error` structs or the router's
  `:cluster_not_ready` / `:no_master` atoms for routing failures.
  """
  @spec execute(GenServer.server(), Key.t(), :all | term(), [option()]) ::
          {:ok, Record.t()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def execute(tender, key, bins, opts \\ [])

  def execute(tender, %Key{} = key, :all, opts) do
    with {:ok, txn} <- TxnSupport.txn_from_opts(opts),
         {:ok, policy} <- UnarySupport.read_policy(tender, opts),
         :ok <- TxnSupport.prepare_txn_read(tender, txn, key) do
      UnarySupport.run_command(
        tender,
        key,
        policy,
        command(),
        %{key: key, conn: tender, txn: txn, opts: opts}
      )
    end
  end

  def execute(_tender, %Key{}, _bins, _opts) do
    {:error,
     %Error{
       code: :invalid_argument,
       message: "Aerospike.Command.Get supports only :all bins in the spike"
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

  defp encode_read(%{key: %Key{} = key, conn: conn, opts: opts}) do
    key.namespace
    |> AsmMsg.read_command(key.set, key.digest)
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
