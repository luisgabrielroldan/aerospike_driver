defmodule Aerospike.Get do
  @moduledoc """
  End-to-end GET command path for the spike.

  ## Control flow per command

  One `execute/4` call drives a retry loop that wraps `Aerospike.Router`
  (partition routing) + `Aerospike.Tender.node_handle/2` (pool + counters
  + breaker thresholds) + `Aerospike.CircuitBreaker.allow?/2` + pool
  checkout + `NodeTransport.command/3`. The loop respects a monotonic
  total-op budget derived from the caller's `:timeout` and a
  configurable attempt cap from the cluster's
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
    * `{:error, %Error{} = e}` with `RetryPolicy.rebalance?/1` → trigger
      an asynchronous `Tender.tend_now/1` so the partition map catches
      up, then retry with `attempt + 1` if the budget allows. The
      Router's next pick is deterministic once the map updates.
    * `{:error, %Error{code: c}}` with `c` in the transport class
      (`:network_error`, `:timeout`, `:connection_error`,
      `:pool_timeout`, `:invalid_node`, `:circuit_open`) → retry with
      `attempt + 1` if the budget allows. `:sequence` replicas rotation
      re-targets the next node automatically.
    * Any other error (server logical errors, routing atoms) → return
      verbatim. No retry.

  On budget exhaustion or the retry cap, the most recent error is
  returned as-is. The monotonic clock is used so system-time adjustments
  mid-command cannot lie about the remaining budget.

  The pool discards a worker with a `:failed` counter bump when the
  command body returns a transport-class error; rebalance errors never
  bump `:failed` because they are a routing cue, not a node-health
  signal.
  """

  alias Aerospike.CircuitBreaker
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.NodePool
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Record
  alias Aerospike.RetryPolicy
  alias Aerospike.Router
  alias Aerospike.Tender

  @default_timeout 5_000

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
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    tables = Tender.tables(tender)
    transport = Tender.transport(tender)
    retry_policy = RetryPolicy.merge(RetryPolicy.load(tables.meta), opts)

    ctx = %{
      tender: tender,
      tables: tables,
      transport: transport,
      policy: retry_policy,
      key: key,
      deadline: monotonic_now() + timeout
    }

    attempt_loop(ctx, _attempt = 0, _last_error = nil)
  end

  def execute(_tender, %Key{}, _bins, _opts) do
    {:error,
     %Error{
       code: :invalid_argument,
       message: "Aerospike.Get supports only :all bins in the spike"
     }}
  end

  defp attempt_loop(ctx, attempt, last_error) do
    cond do
      attempt > ctx.policy.max_retries ->
        exhausted(last_error, :max_retries)

      budget_exhausted?(ctx.deadline) ->
        exhausted(last_error, :deadline)

      true ->
        run_attempt(ctx, attempt, last_error)
    end
  end

  defp run_attempt(ctx, attempt, last_error) do
    case Router.pick_for_read(ctx.tables, ctx.key, ctx.policy.replica_policy, attempt) do
      {:ok, node_name} ->
        dispatch(ctx, node_name, attempt)

      {:error, _reason} = routing_error ->
        # Routing atoms (`:cluster_not_ready`, `:no_master`) are fatal for
        # the retry driver: we have no replica to target, so re-picking
        # would burn attempts against the same empty state. The caller
        # gets the last concrete error if we have one, else the routing
        # atom itself.
        fatal(last_error, routing_error)
    end
  end

  defp dispatch(ctx, node_name, attempt) do
    case Tender.node_handle(ctx.tender, node_name) do
      {:ok, handle} ->
        check_breaker(ctx, handle, attempt)

      {:error, :unknown_node} = err ->
        # The node disappeared between pick and handle — treat as a
        # transport-class miss so the next attempt re-picks.
        retry_after_error(ctx, attempt, err)
    end
  end

  defp check_breaker(ctx, handle, attempt) do
    case CircuitBreaker.allow?(handle.counters, handle.breaker) do
      :ok ->
        remaining = remaining_budget(ctx.deadline)
        command_opts = [use_compression: handle.use_compression]

        result =
          NodePool.checkout!(
            handle.pool,
            fn conn -> do_get(ctx.transport, conn, ctx.key, remaining, command_opts) end,
            remaining
          )

        classify(ctx, attempt, result)

      {:error, %Error{code: :circuit_open}} = err ->
        # Breaker refusal is a node-health cue. Treat as transport class
        # so `:sequence` re-routes to a different replica on the next
        # attempt without paying a checkout.
        retry_after_error(ctx, attempt, err)
    end
  end

  defp classify(ctx, attempt, result) do
    cond do
      match?({:ok, _}, result) ->
        result

      RetryPolicy.rebalance?(result) ->
        trigger_tend_async(ctx.tender)
        retry_after_error(ctx, attempt, result)

      RetryPolicy.transport?(result) ->
        retry_after_error(ctx, attempt, result)

      true ->
        # Fatal: return verbatim. No retry for server logical errors.
        result
    end
  end

  defp retry_after_error(ctx, attempt, err) do
    maybe_sleep(ctx.policy)
    attempt_loop(ctx, attempt + 1, err)
  end

  defp maybe_sleep(%{sleep_between_retries_ms: 0}), do: :ok

  defp maybe_sleep(%{sleep_between_retries_ms: ms}) when is_integer(ms) and ms > 0 do
    Process.sleep(ms)
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

  defp exhausted(nil, reason) do
    {:error,
     %Error{
       code: :timeout,
       message: "Aerospike.Get: retry budget exhausted (#{reason}) with no attempts succeeding"
     }}
  end

  defp exhausted(last_error, _reason), do: last_error

  defp fatal(_last_error, routing_error) do
    # Routing atoms (`:cluster_not_ready`, `:no_master`) surface directly.
    # A prior transport error is not substituted because the router's
    # verdict supersedes it: if we no longer have any replica to target,
    # the last transport error is irrelevant.
    routing_error
  end

  defp budget_exhausted?(deadline), do: remaining_budget(deadline) <= 0

  defp remaining_budget(deadline), do: deadline - monotonic_now()

  defp monotonic_now, do: System.monotonic_time(:millisecond)

  # Returned tuple shape matches `NodePool.checkout!/3`'s `fun` contract:
  # `{result_for_caller, checkin_value}`.
  #
  #   * `conn` — keep the worker (normal return).
  #   * `:close` — drop the worker without counting a failure.
  #   * `{:close, :failure}` — drop the worker *and* bump the node's
  #     `:failed` counter. Used for transport-level errors and parse
  #     errors so the Task 6 circuit breaker sees the rate.
  defp do_get(transport, conn, key, deadline_ms, command_opts) do
    request = encode_read(key)

    case transport.command(conn, request, deadline_ms, command_opts) do
      {:ok, body} ->
        case decode_as_msg(body) do
          {:ok, msg} ->
            {Response.parse_record_response(msg, key), conn}

          {:error, %Error{}} = err ->
            {err, {:close, :failure}}
        end

      {:error, %Error{}} = err ->
        {err, {:close, :failure}}
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
