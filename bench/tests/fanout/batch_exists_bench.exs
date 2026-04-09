Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.Fanout.BatchExists do
  @moduledoc false

  alias Aerospike.Bench.Support.E2EHelpers
  alias Aerospike.Bench.Support.Runtime

  @conn_name :bench_fanout_exists
  @default_batch_size 100
  @cluster_ready_timeout_ms 5_000

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config)
    target = Runtime.connection_target("bench_fanout_exists")

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
        Runtime.print_metadata(metadata, %{workload: :fo_009})

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

  defp configured_batch_sizes(config) do
    (Map.get(config, :fanout_batch_sizes, [@default_batch_size]) ++
       Map.get(config, :fanout_large_batch_sizes, []))
    |> Enum.uniq()
  end

  defp run_for_concurrency(config, concurrency, batch_size, keys) do
    Benchee.run(
      %{
        "FO-009 Batch Exists #{batch_size} keys" => fn ->
          {:ok, exists_flags} = Aerospike.batch_exists(@conn_name, keys)
          true = length(exists_flags) == batch_size
          true = Enum.all?(exists_flags, &(&1 == true))
        end
      },
      Aerospike.Bench.benchee_options(
        config,
        title: "L4 fan-out exists (concurrency=#{concurrency}, batch_size=#{batch_size})",
        parallel: concurrency,
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp build_state(namespace, set, payload, batch_sizes) do
    batch_keysets =
      Enum.map(batch_sizes, fn batch_size ->
        keys = E2EHelpers.key_ring(namespace, set, "fo:batch:exists:#{batch_size}", batch_size)
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

Aerospike.Bench.Fanout.BatchExists.run()
