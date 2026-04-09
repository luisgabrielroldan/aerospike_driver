defmodule Aerospike.CircuitBreaker do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Tables

  @type conn_name :: atom()
  @type node_name :: String.t()

  @increment_event [:aerospike, :circuit_breaker, :increment]
  @reject_event [:aerospike, :circuit_breaker, :reject]
  @reset_event [:aerospike, :circuit_breaker, :reset]

  @doc """
  Returns whether the circuit breaker is enabled for the connection.

  Breaker state is driven by the `:breaker_config` row in the meta ETS table:
  `%{max_error_rate: non_neg_integer, error_rate_window: pos_integer}`.

  Missing or malformed config intentionally disables breaker logic so request paths
  never crash due to partial startup state or transient table inconsistencies.
  """
  @spec enabled?(conn_name()) :: boolean()
  def enabled?(conn) when is_atom(conn) do
    case breaker_config(conn) do
      {:ok, %{max_error_rate: max_error_rate}} -> max_error_rate > 0
      :error -> false
    end
  end

  @doc """
  Performs the request-path threshold check for a node.

  Rejection boundary is `count >= max_error_rate` (inclusive), so a node is
  rejected as soon as it reaches the configured threshold. This matches the
  fixed-threshold semantics used by the cluster tend window.

  Emits `[:aerospike, :circuit_breaker, :reject]` on rejection with:
  - measurements: `%{count: count, max_error_rate: threshold}`
  - metadata: `%{conn: conn, node: node_name}`
  """
  @spec allow_request?(conn_name(), node_name()) :: :ok | {:error, Error.t()}
  def allow_request?(conn, node_name) when is_atom(conn) and is_binary(node_name) do
    if enabled?(conn) do
      {:ok, %{max_error_rate: max_error_rate}} = breaker_config(conn)
      count = error_count(conn, node_name)
      maybe_reject(conn, node_name, count, max_error_rate)
    else
      :ok
    end
  end

  @doc """
  Atomically increments the per-node error counter for breaker tracking.

  Uses `:ets.update_counter/4` with row shape `{node_name, count}` so increments
  remain lock-free and safe across concurrent request processes.

  The counter is additive within the current tend window and is only cleared by
  `maybe_reset_window/3`. It is never decremented in request paths.

  Emits `[:aerospike, :circuit_breaker, :increment]` with:
  - measurements: `%{count: updated_count}`
  - metadata: `%{conn: conn, node: node_name, reason: reason}`

  The `reason` metadata is part of the maintainer-facing contract used to
  differentiate transport and server-overload signals in telemetry handlers.
  """
  @spec record_error(conn_name(), node_name(), atom()) :: :ok
  def record_error(conn, node_name, reason)
      when is_atom(conn) and is_binary(node_name) and is_atom(reason) do
    if enabled?(conn) do
      count = :ets.update_counter(Tables.breaker(conn), node_name, {2, 1}, {node_name, 0})
      emit_increment(conn, node_name, reason, count)
    end

    :ok
  end

  @doc """
  Resets all node counters at the configured tend-window boundary.

  Reset happens when `rem(tend_tick, error_rate_window) == 0`, matching the
  cluster tend cadence so windows are consistent across all request paths.

  Emits `[:aerospike, :circuit_breaker, :reset]` for each node whose
  `count_before_reset` is greater than zero.
  """
  @spec maybe_reset_window(conn_name(), non_neg_integer(), pos_integer()) :: :ok
  def maybe_reset_window(conn, tend_tick, error_rate_window)
      when is_atom(conn) and is_integer(tend_tick) and tend_tick >= 0 and
             is_integer(error_rate_window) and error_rate_window > 0 do
    if enabled?(conn) and rem(tend_tick, error_rate_window) == 0 do
      reset_window(conn)
    end

    :ok
  end

  defp maybe_reject(conn, node_name, count, max_error_rate) when count >= max_error_rate do
    emit_reject(conn, node_name, count, max_error_rate)
    {:error, Error.from_result_code(:max_error_rate, node: node_name)}
  end

  defp maybe_reject(_conn, _node_name, _count, _max_error_rate), do: :ok

  # Config reads are intentionally total: malformed or missing rows return :error
  # so public request-path functions can safely degrade to breaker-disabled mode.
  defp breaker_config(conn) do
    case :ets.lookup(Tables.meta(conn), :breaker_config) do
      [{:breaker_config, %{max_error_rate: max_error_rate, error_rate_window: error_rate_window}}]
      when is_integer(max_error_rate) and max_error_rate >= 0 and is_integer(error_rate_window) and
             error_rate_window > 0 ->
        {:ok, %{max_error_rate: max_error_rate, error_rate_window: error_rate_window}}

      _ ->
        :error
    end
  end

  # Unknown rows are treated as count 0 to avoid negative coupling with node
  # discovery order and to keep pre-checks idempotent during topology churn.
  defp error_count(conn, node_name) do
    case :ets.lookup(Tables.breaker(conn), node_name) do
      [{^node_name, count}] when is_integer(count) and count >= 0 -> count
      _ -> 0
    end
  end

  # Emit reset events before deleting rows so observers receive the final
  # window totals for each node.
  defp reset_window(conn) do
    conn
    |> Tables.breaker()
    |> :ets.tab2list()
    |> Enum.each(&emit_reset(conn, &1))

    :ets.delete_all_objects(Tables.breaker(conn))
  end

  defp emit_increment(conn, node_name, reason, count) do
    :telemetry.execute(@increment_event, %{count: count}, %{
      conn: conn,
      node: node_name,
      reason: reason
    })
  end

  defp emit_reject(conn, node_name, count, max_error_rate) do
    :telemetry.execute(
      @reject_event,
      %{count: count, max_error_rate: max_error_rate},
      %{conn: conn, node: node_name}
    )
  end

  defp emit_reset(conn, {node_name, count})
       when is_binary(node_name) and is_integer(count) and count > 0 do
    :telemetry.execute(
      @reset_event,
      %{count_before_reset: count},
      %{conn: conn, node: node_name}
    )
  end

  defp emit_reset(_conn, _entry), do: :ok
end
