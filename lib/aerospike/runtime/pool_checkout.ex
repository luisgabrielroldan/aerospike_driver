defmodule Aerospike.Runtime.PoolCheckout do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Telemetry

  @spec run(String.t() | nil, NimblePool.pool(), (-> result), timeout) ::
          result | {:error, Error.t()}
        when result: var
  def run(node_name, pool, checkout_fun, timeout)
      when is_function(checkout_fun, 0) and is_integer(timeout) do
    metadata = %{node_name: node_name, pool_pid: pool}
    emit_span(metadata, fn -> checkout_result(checkout_fun) end)
  end

  defp checkout_result(checkout_fun) do
    checkout_fun.()
  catch
    :exit, {:timeout, {NimblePool, :checkout, _}} ->
      {:error, Error.from_result_code(:pool_timeout)}

    :exit, {:noproc, {NimblePool, :checkout, _}} ->
      {:error,
       %Error{
         code: :invalid_node,
         message: "Aerospike.Cluster.NodePool: pool not available"
       }}

    :exit, reason ->
      {:error,
       %Error{
         code: :network_error,
         message: "Aerospike.Cluster.NodePool: checkout exited: #{inspect(reason)}"
       }}
  end

  defp emit_span(metadata, fun) do
    start_time = System.monotonic_time()
    span_metadata = Map.put(metadata, :telemetry_span_context, make_ref())

    emit_start(start_time, span_metadata)

    try do
      result = fun.()
      emit_stop(start_time, span_metadata)
      result
    rescue
      exception ->
        emit_exception(start_time, span_metadata, :error, exception, __STACKTRACE__)
        reraise(exception, __STACKTRACE__)
    catch
      kind, reason ->
        emit_exception(start_time, span_metadata, kind, reason, __STACKTRACE__)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp emit_start(start_time, metadata) do
    :telemetry.execute(
      Telemetry.pool_checkout_span() ++ [:start],
      %{system_time: System.system_time(), monotonic_time: start_time},
      metadata
    )
  end

  defp emit_stop(start_time, metadata) do
    :telemetry.execute(
      Telemetry.pool_checkout_span() ++ [:stop],
      %{duration: System.monotonic_time() - start_time},
      metadata
    )
  end

  defp emit_exception(start_time, metadata, kind, reason, stacktrace) do
    :telemetry.execute(
      Telemetry.pool_checkout_span() ++ [:exception],
      %{duration: System.monotonic_time() - start_time},
      Map.merge(metadata, %{kind: kind, reason: reason, stacktrace: stacktrace})
    )
  end
end
