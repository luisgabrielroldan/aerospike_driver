Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.Fanout.BatchOperate do
  @moduledoc false

  import Aerospike.Op

  alias Aerospike.Batch
  alias Aerospike.BatchResult
  alias Aerospike.Bench.Support.E2EHelpers
  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Record

  @conn_name :bench_fanout_operate
  @default_batch_size 100
  @cluster_ready_timeout_ms 5_000

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config)
    target = Runtime.connection_target("bench_fanout_operate")

    batch_sizes = configured_batch_sizes(config)
    payload_profiles = Map.fetch!(config, :payload_profiles)
    payload = E2EHelpers.payload(payload_profiles, :small)

    state = build_state(target.namespace, target.set, payload, batch_sizes)

    E2EHelpers.with_started_client(
      @conn_name,
      target.host,
      target.port,
      @cluster_ready_timeout_ms,
      fn ->
        bootstrap_dataset!(@conn_name, state)
        Runtime.print_metadata(metadata, %{workload: :fo_010})

        try do
          Enum.each(Map.fetch!(config, :concurrency), fn concurrency ->
            Enum.each(state.batch_workloads, fn workload ->
              run_for_concurrency(config, concurrency, state.payload_bin, workload)
            end)
          end)
        after
          teardown_dataset(@conn_name, state)
        end
      end
    )
  end

  defp configured_batch_sizes(config) do
    (Map.get(config, :fanout_batch_sizes, [@default_batch_size]) ++
       Map.get(config, :fanout_large_batch_sizes, []))
    |> Enum.uniq()
  end

  defp run_for_concurrency(config, concurrency, payload_bin, workload) do
    Benchee.run(
      %{
        "FO-010 Batch Operate Read-Heavy #{workload.batch_size} keys" => fn ->
          {:ok, results} = Aerospike.batch_operate(@conn_name, workload.read_ops)
          true = length(results) == workload.batch_size
          true = Enum.all?(results, &read_result?(&1, payload_bin))
        end,
        "FO-011 Batch Operate Mixed #{workload.batch_size} keys" => fn ->
          {:ok, results} = Aerospike.batch_operate(@conn_name, workload.mixed_ops)
          true = length(results) == workload.batch_size
          true = Enum.all?(results, &mixed_result?(&1, payload_bin))
        end
      },
      Aerospike.Bench.benchee_options(
        config,
        title: "L4 fan-out batch operate (concurrency=#{concurrency}, batch_size=#{workload.batch_size})",
        parallel: concurrency,
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp build_state(namespace, set, payload, batch_sizes) do
    {payload_bin, payload_value} = single_payload_bin!(payload)

    batch_workloads =
      Enum.map(batch_sizes, fn batch_size ->
        keys = E2EHelpers.key_ring(namespace, set, "fo:batch:operate:#{batch_size}", batch_size)

        %{
          batch_size: batch_size,
          keys: keys,
          read_ops: Enum.map(keys, &Batch.read(&1, bins: ["counter", payload_bin])),
          mixed_ops: Enum.map(keys, &Batch.operate(&1, mixed_recipe(payload_bin, payload_value)))
        }
      end)

    %{
      payload: Map.put(payload, "counter", 0),
      payload_bin: payload_bin,
      batch_workloads: batch_workloads
    }
  end

  defp mixed_recipe(payload_bin, payload_value) do
    [
      put(payload_bin, payload_value),
      add("counter", 1),
      get("counter"),
      get(payload_bin)
    ]
  end

  defp read_result?(%BatchResult{status: :ok, record: %Record{bins: bins}}, payload_bin) do
    is_integer(Map.get(bins, "counter")) and is_binary(Map.get(bins, payload_bin))
  end

  defp read_result?(_result, _payload_bin), do: false

  defp mixed_result?(%BatchResult{status: :ok, record: %Record{bins: bins}}, payload_bin) do
    is_integer(Map.get(bins, "counter")) and is_binary(Map.get(bins, payload_bin))
  end

  defp mixed_result?(_result, _payload_bin), do: false

  defp single_payload_bin!(payload) when is_map(payload) do
    case Map.to_list(payload) do
      [{payload_bin, payload_value}] when is_binary(payload_bin) and is_binary(payload_value) ->
        {payload_bin, payload_value}

      _ ->
        raise ArgumentError, "expected single-bin payload map, got: #{inspect(payload)}"
    end
  end

  defp bootstrap_dataset!(conn_name, state) do
    keys =
      state.batch_workloads
      |> Enum.flat_map(& &1.keys)
      |> Enum.uniq()

    E2EHelpers.bootstrap_keys!(conn_name, keys, state.payload)
  end

  defp teardown_dataset(conn_name, state) do
    keys =
      state.batch_workloads
      |> Enum.flat_map(& &1.keys)
      |> Enum.uniq()

    E2EHelpers.teardown_keys(conn_name, keys)
  end
end

Aerospike.Bench.Fanout.BatchOperate.run()
