defmodule Aerospike.Get do
  @moduledoc """
  End-to-end GET command path for the spike.

  ## Control flow per command

  One `execute/4` call delegates the shared unary execution flow to
  `Aerospike.UnaryExecutor`. That executor owns the retry loop around
  `Aerospike.Router` (partition routing) + `Aerospike.Tender.node_handle/2`
  (pool + counters + breaker thresholds) + `Aerospike.CircuitBreaker.allow?/2`
  + pool checkout + `NodeTransport.command/3`. GET stays responsible for
  building the read request and parsing the reply. The shared loop
  respects a monotonic total-op budget derived from the caller's
  `:timeout` and a configurable attempt cap from the cluster's
  `Aerospike.RetryPolicy`.

  Per attempt:

    1. `Router.pick_for_read/4` chooses a replica. `:sequence` walks the
       replica list by attempt index; `:master` pins every attempt to
       the master.
    2. `Tender.node_handle/2` resolves that replica to a concrete
       `{pool, counters, breaker}` triple — one GenServer hop.
    3. `CircuitBreaker.allow?/2` short-circuits if the node has too many
       recent transport failures or is at its concurrency cap. A
       short-circuit returns `{:error, %Error{code: :circuit_open}}`
       which the driver classifies as transport-class: the next attempt
       re-picks, skipping the refused node on `:sequence`.
    4. `NodePool.checkout!/3` borrows a worker and sends the AS_MSG.

  Classification for retry:

    * `{:ok, record}` — return immediately.
    * rebalance bucket → trigger an asynchronous `Tender.tend_now/1`
      so the partition map catches up, then retry with `attempt + 1`
      if the budget allows. The Router's next pick is deterministic
      once the map updates.
    * transport bucket (`:network_error`, `:timeout`,
      `:connection_error`, `:pool_timeout`, `:invalid_node`,
      `:circuit_open`) → retry with `attempt + 1` if the budget
      allows. `:sequence` replica rotation re-targets the next node
      automatically.
    * routing-refusal and server-fatal buckets → return verbatim. No
      retry.

  On budget exhaustion or the retry cap, the most recent error is
  returned as-is. The monotonic clock is used so system-time adjustments
  mid-command cannot lie about the remaining budget.

  The pool discards a worker with a `:failed` counter bump when the
  command body returns a transport-class error; rebalance errors never
  bump `:failed` because they are a routing cue, not a node-health
  signal.
  """

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Record
  alias Aerospike.RetryPolicy
  alias Aerospike.Tender
  alias Aerospike.UnaryCommand
  alias Aerospike.UnaryExecutor

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:replica_policy, :master | :sequence}

  @doc """
  Reads all bins for `key` from the cluster identified by `tender`.

  `tender` is a running `Aerospike.Tender` (pid or registered name). The
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
    tables = Tender.tables(tender)
    transport = Tender.transport(tender)

    executor =
      UnaryExecutor.new!(
        base_policy: RetryPolicy.load(tables.meta),
        command_opts: opts,
        on_rebalance: fn -> trigger_tend_async(tender) end
      )

    command =
      UnaryCommand.new!(
        name: __MODULE__,
        build_request: &encode_read/1,
        parse_response: &parse_record_response/2
      )

    UnaryExecutor.run_command(executor, command, %{
      tender: tender,
      tables: tables,
      transport: transport,
      route_key: key,
      command_input: key
    })
  end

  def execute(_tender, %Key{}, _bins, _opts) do
    {:error,
     %Error{
       code: :invalid_argument,
       message: "Aerospike.Get supports only :all bins in the spike"
     }}
  end

  # Fire a tend cycle without blocking the command path. Retaining the
  # returned task pid is unnecessary — if the tend fails, the existing
  # map is still the best we have, and the next retry will either hit
  # the same rebalance (and the budget burns out) or see the updated
  # map. Spawn with `:transient` (no link) so a transient failure cannot
  # tear down the caller.
  defp trigger_tend_async(tender) do
    _ =
      spawn(fn ->
        try do
          Tender.tend_now(tender)
        catch
          :exit, _ -> :ok
        end
      end)

    :ok
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

  defp parse_record_response(body, key) do
    case decode_as_msg(body) do
      {:ok, msg} ->
        Response.parse_record_response(msg, key)

      {:error, %Error{}} = err ->
        err
    end
  end
end
