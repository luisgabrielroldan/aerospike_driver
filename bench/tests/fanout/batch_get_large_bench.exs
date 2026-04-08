Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.Fanout.BatchGetLarge do
  @moduledoc false

  alias Aerospike.Bench.Support.E2EHelpers
  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Record

  @conn_name :bench_fanout_large
  @default_batch_size 1000
  @cluster_ready_timeout_ms 5_000

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config)
    target = Runtime.connection_target("bench_fanout_large")

    batch_sizes = Map.get(config, :fanout_large_batch_sizes, [@default_batch_size])
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
        Runtime.print_metadata(metadata, %{workload: :fo_002})

        try do
          Enum.each(Map.fetch!(config, :concurrency), fn concurrency ->
            Enum.each(state.batch_keysets, fn {batch_size, keys} ->
              run_for_concurrency(config, concurrency, batch_size, keys)
            end)
          end)
        after
          teardown_dataset(@conn_name, state)
        end
      end
    )
  end

  defp run_for_concurrency(config, concurrency, batch_size, keys) do
    Benchee.run(
      %{
        "FO-002 Batch Get #{batch_size} keys" => fn ->
          {:ok, records} = Aerospike.batch_get(@conn_name, keys)
          true = length(records) == batch_size
          true = Enum.all?(records, &match?(%Record{bins: bins} when map_size(bins) > 0, &1))
        end
      },
      Aerospike.Bench.benchee_options(
        config,
        title: "L4 fan-out large baseline (concurrency=#{concurrency}, batch_size=#{batch_size})",
        parallel: concurrency,
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp build_state(namespace, set, payload, batch_sizes) do
    batch_keysets =
      Enum.map(batch_sizes, fn batch_size ->
        keys = E2EHelpers.key_ring(namespace, set, "fo:batch:get:large:#{batch_size}", batch_size)
        {batch_size, keys}
      end)

    %{
      payload: payload,
      batch_keysets: batch_keysets
    }
  end

  defp bootstrap_dataset!(conn_name, state) do
    keys =
      state.batch_keysets
      |> Enum.flat_map(fn {_batch_size, keyset} -> keyset end)
      |> Enum.uniq()

    E2EHelpers.bootstrap_keys!(conn_name, keys, state.payload)
  end

  defp teardown_dataset(conn_name, state) do
    keys =
      state.batch_keysets
      |> Enum.flat_map(fn {_batch_size, keyset} -> keyset end)
      |> Enum.uniq()

    E2EHelpers.teardown_keys(conn_name, keys)
  end
end

Aerospike.Bench.Fanout.BatchGetLarge.run()
