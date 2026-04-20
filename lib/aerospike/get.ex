defmodule Aerospike.Get do
  @moduledoc """
  End-to-end GET command path for the spike.

  Routes the key through `Aerospike.Router`, resolves the chosen node's
  pool via `Aerospike.Tender.pool_pid/2`, checks out a connection from
  `Aerospike.NodePool`, sends one AS_MSG read with `INFO1_GET_ALL`, and
  parses the reply into an `Aerospike.Record`.

  The Tender owns the per-node pool; the command path only borrows a
  connection for the duration of the round-trip. On command failure the
  checkout returns `:close` so the pool discards the worker and
  initialises a fresh one on the next checkout.
  """

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.NodePool
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Record
  alias Aerospike.Router
  alias Aerospike.Tender

  @default_timeout 5_000

  @type option :: {:timeout, non_neg_integer()}

  @doc """
  Reads all bins for `key` from the cluster identified by `tender`.

  `tender` is a running `Aerospike.Tender` (pid or registered name). The
  spike only supports reading every bin (`bins = :all`); other shapes
  return `{:error, %Aerospike.Error{code: :invalid_argument}}` so future
  tasks can widen the API without changing the signature.

  Options:

    * `:timeout` — milliseconds for the full round-trip, default `5_000`.

  Errors surface as typed `Aerospike.Error` structs or the router's
  `:cluster_not_ready` / `:no_master` atoms for routing failures.
  """
  @spec execute(GenServer.server(), Key.t(), :all | term(), [option()]) ::
          {:ok, Record.t()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def execute(tender, key, bins, opts \\ [])

  def execute(tender, %Key{} = key, :all, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    tables = Tender.tables(tender)

    with {:ok, node_name} <- Router.pick_for_read(tables, key, :master, 0),
         {:ok, pool} <- Tender.pool_pid(tender, node_name) do
      transport = Tender.transport(tender)
      run_via_pool(pool, transport, key, timeout)
    end
  end

  def execute(_tender, %Key{}, _bins, _opts) do
    {:error,
     %Error{
       code: :invalid_argument,
       message: "Aerospike.Get supports only :all bins in the spike"
     }}
  end

  defp run_via_pool(pool, transport, key, timeout) do
    NodePool.checkout!(
      pool,
      fn conn -> do_get(transport, conn, key, timeout) end,
      timeout
    )
  end

  # Returned tuple shape matches `NodePool.checkout!/3`'s `fun` contract:
  # `{result_for_caller, checkin_value}`. `:close` as the checkin value
  # asks the pool to drop the worker; returning the `conn` returns it to
  # the pool for reuse.
  #
  # Tier 1.5 reuses the caller's `:timeout` verbatim as the per-read
  # deadline passed to `NodeTransport.command/3`. GET issues one request
  # and reads one reply, so the whole op budget covers the read — encode,
  # send, decode are effectively free compared to a remote `recv`. Tier 2
  # retry work will pick a smaller per-attempt deadline derived from a
  # monotonic op budget.
  defp do_get(transport, conn, key, deadline_ms) do
    request = encode_read(key)

    case transport.command(conn, request, deadline_ms) do
      {:ok, body} ->
        case decode_as_msg(body) do
          {:ok, msg} ->
            {Response.parse_record_response(msg, key), conn}

          {:error, %Error{}} = err ->
            {err, :close}
        end

      {:error, %Error{}} = err ->
        {err, :close}
    end
  end

  defp encode_read(%Key{} = key) do
    key.namespace
    |> AsmMsg.read_command(key.set, key.digest)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp decode_as_msg(body) do
    case AsmMsg.decode(body) do
      {:ok, _} = ok ->
        ok

      {:error, reason} ->
        {:error, %Error{code: :parse_error, message: "failed to decode AS_MSG reply: #{reason}"}}
    end
  end
end
