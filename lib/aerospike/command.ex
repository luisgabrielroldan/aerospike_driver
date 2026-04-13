defmodule Aerospike.Command do
  @moduledoc false

  # Single entry point for `[:aerospike, :command]` telemetry and inline
  # `RuntimeMetrics.record/5`: use `run/2` for synchronous commands (span
  # shape) and `emit_start/1` plus `emit_stop/3` / `emit_exception/4` for
  # streaming scans where `:start` and `:stop` run in different processes. New
  # command paths must not call `:telemetry.span` or `:telemetry.execute` on
  # this event outside this module. `:telemetry.span/3` does not merge start
  # metadata into `:stop`; `build_stop_meta/3` merges explicitly.

  alias Aerospike.Error
  alias Aerospike.RuntimeMetrics

  @type start_meta :: %{
          required(:command) => atom(),
          required(:conn) => atom(),
          optional(atom()) => term()
        }

  @type ctx :: %{t0: integer(), meta: start_meta()}

  @spec run(start_meta(), (-> {term(), map()})) :: term()
  def run(%{command: command, conn: conn} = start_meta, fun) when is_function(fun, 0) do
    :telemetry.span([:aerospike, :command], start_meta, fn ->
      start_mono = System.monotonic_time()
      {result, stop_meta} = fun.()
      node = Map.get(stop_meta, :node)
      RuntimeMetrics.record(conn, command, start_mono, result, node)
      {result, build_stop_meta(start_meta, stop_meta, result)}
    end)
  end

  @spec emit_start(start_meta()) :: ctx()
  def emit_start(%{command: _, conn: _} = start_meta) do
    t0 = System.monotonic_time()

    :telemetry.execute(
      [:aerospike, :command, :start],
      %{monotonic_time: t0},
      start_meta
    )

    %{t0: t0, meta: start_meta}
  end

  @spec emit_stop(ctx(), map(), term()) :: :ok
  def emit_stop(%{t0: t0, meta: %{command: command, conn: conn} = start_meta}, stop_meta, result)
      when is_map(stop_meta) do
    node = Map.get(stop_meta, :node)
    RuntimeMetrics.record(conn, command, t0, result, node)

    now = System.monotonic_time()
    duration = now - t0

    :telemetry.execute(
      [:aerospike, :command, :stop],
      %{duration: duration, monotonic_time: now},
      build_stop_meta(start_meta, stop_meta, result)
    )

    :ok
  end

  @spec emit_exception(ctx(), Exception.kind(), term(), Exception.stacktrace()) :: :ok
  def emit_exception(%{t0: t0, meta: start_meta}, kind, _reason, _stacktrace) do
    now = System.monotonic_time()
    duration = now - t0

    :telemetry.execute(
      [:aerospike, :command, :exception],
      %{duration: duration, monotonic_time: now},
      Map.merge(start_meta, %{kind: kind})
    )

    :ok
  end

  defp build_stop_meta(start_meta, stop_meta, result) do
    Map.merge(start_meta, Map.put(stop_meta, :result, result_tag(result)))
  end

  defp result_tag(:ok), do: :ok
  defp result_tag({:ok, _}), do: :ok
  defp result_tag({:error, %Error{code: code}}), do: {:error, code}
end
