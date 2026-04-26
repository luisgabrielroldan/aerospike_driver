Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.Micro.RuntimeObservability do
  @moduledoc false

  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Runtime.PoolCheckout
  alias Aerospike.RuntimeMetrics
  alias Aerospike.Telemetry

  @iterations 512

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config, %{iterations_per_sample: @iterations})

    Runtime.print_metadata(metadata, %{workload: :runtime_observability_micro})

    Benchee.run(
      jobs(),
      Aerospike.Bench.benchee_options(
        config,
        title: "L1 runtime observability overhead",
        inputs: inputs(),
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp jobs do
    %{
      "RO-001 RuntimeMetrics disabled record_command x#{@iterations}" => fn %{
                                                                              disabled_cluster:
                                                                                cluster
                                                                            } ->
        repeat(fn ->
          RuntimeMetrics.record_command(cluster, Aerospike.Command.Get, now(), :ok)
        end)
      end,
      "RO-002 RuntimeMetrics enabled record_command x#{@iterations}" => fn %{
                                                                             enabled_cluster:
                                                                               cluster
                                                                           } ->
        repeat(fn ->
          RuntimeMetrics.record_command(cluster, Aerospike.Command.Get, now(), :ok)
        end)
      end,
      "RO-003 PoolCheckout telemetry no handlers x#{@iterations}" => fn %{pool: pool} ->
        repeat(fn -> PoolCheckout.run("bench-node", pool, fn -> :ok end, 1_000) end)
      end,
      "RO-004 PoolCheckout telemetry handlers x#{@iterations}" => fn %{pool: pool} ->
        with_handlers(fn ->
          repeat(fn -> PoolCheckout.run("bench-node", pool, fn -> :ok end, 1_000) end)
        end)
      end,
      "RO-005 Retry telemetry no handlers x#{@iterations}" => fn _input ->
        repeat(fn -> Telemetry.emit_retry_attempt("bench-node", 1, :transport, 1_000) end)
      end,
      "RO-006 Retry telemetry handlers x#{@iterations}" => fn _input ->
        with_handlers(fn ->
          repeat(fn -> Telemetry.emit_retry_attempt("bench-node", 1, :transport, 1_000) end)
        end)
      end
    }
  end

  defp inputs do
    disabled_cluster = start_cluster!(:runtime_observability_disabled)
    enabled_cluster = start_cluster!(:runtime_observability_enabled)
    pool = self()

    RuntimeMetrics.init(disabled_cluster)
    RuntimeMetrics.init(enabled_cluster)
    RuntimeMetrics.enable(enabled_cluster)

    %{
      "local ETS/meta tables" => %{
        disabled_cluster: disabled_cluster,
        enabled_cluster: enabled_cluster,
        pool: pool
      }
    }
  end

  defp start_cluster!(prefix) do
    name = :"#{prefix}_#{System.unique_integer([:positive])}"
    {:ok, _owner} = TableOwner.start_link(name: name)
    name
  end

  defp repeat(fun) do
    Enum.reduce(1..@iterations, nil, fn _index, _acc -> fun.() end)
  end

  defp now, do: System.monotonic_time()

  defp with_handlers(fun) do
    handler_id = {__MODULE__, self(), System.unique_integer([:positive])}

    :ok =
      :telemetry.attach_many(
        handler_id,
        Telemetry.handler_events(),
        &__MODULE__.handle_event/4,
        nil
      )

    try do
      fun.()
    after
      :telemetry.detach(handler_id)
    end
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok
end

Aerospike.Bench.Micro.RuntimeObservability.run()
