defmodule Demo.Examples.RuntimeMetrics do
  @moduledoc """
  Demonstrates runtime metrics counters and explicit pool warm-up.
  """

  require Logger

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_runtime_metrics"
  @key "metrics_key"

  def run do
    key = Aerospike.key(@namespace, @set, @key)

    try do
      reset_metrics()
      run_command_sequence(key)
      warm_up_pool()
      disable_metrics()
    after
      @repo.disable_metrics()
      @repo.delete(key)
    end
  end

  defp reset_metrics do
    :ok = @repo.disable_metrics()

    unless @repo.metrics_enabled?() == false do
      raise "Expected runtime metrics to start disabled"
    end

    :ok = @repo.enable_metrics(reset: true)

    unless @repo.metrics_enabled?() == true do
      raise "Expected runtime metrics to be enabled"
    end

    Logger.info("  Runtime metrics enabled with counters reset")
  end

  defp run_command_sequence(key) do
    before = @repo.stats()

    :ok = @repo.put!(key, %{"name" => "metrics", "count" => 1})
    {:ok, record} = @repo.get(key)
    {:ok, true} = @repo.delete(key)

    unless record.bins["count"] == 1 do
      raise "Expected record count bin to round-trip through metrics example"
    end

    after_stats = @repo.stats()
    assert_commands_increased(before, after_stats)

    Logger.info("  Commands observed: #{before.commands_total} -> #{after_stats.commands_total}")
  end

  defp warm_up_pool do
    {:ok, result} = @repo.warm_up(count: 1)

    unless result.status in [:ok, :partial] and result.total_warmed > 0 do
      raise "Expected warm_up(count: 1) to check out at least one connection"
    end

    Logger.info(
      "  Warm-up: status=#{result.status} warmed=#{result.total_warmed}/#{result.total_requested}"
    )
  end

  defp disable_metrics do
    :ok = @repo.disable_metrics()

    unless @repo.metrics_enabled?() == false do
      raise "Expected runtime metrics to be disabled"
    end

    Logger.info("  Runtime metrics disabled")
  end

  defp assert_commands_increased(before, after_stats) do
    unless after_stats.metrics_enabled == true do
      raise "Expected stats snapshot to report enabled metrics"
    end

    unless after_stats.commands_total > before.commands_total do
      raise "Expected commands_total to increase after put/get/delete"
    end

    unless after_stats.commands_ok >= before.commands_ok do
      raise "Expected commands_ok not to decrease"
    end
  end
end
