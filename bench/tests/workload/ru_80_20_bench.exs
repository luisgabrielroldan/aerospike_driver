Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.Workload.RU8020 do
  @moduledoc false

  alias Aerospike.Bench.Support.E2EHelpers
  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Record

  @conn_name :bench_workload
  @ring_size 512
  @mix_size 1000
  @seed 8020
  @read_ratio_env "BENCH_READ_RATIO"
  @write_ratio_env "BENCH_WRITE_RATIO"
  @cluster_ready_timeout_ms 5_000

  def run do
    config = Aerospike.Bench.load_config()
    target = Runtime.connection_target("bench_workload")
    read_ratio = Runtime.env_positive_integer(@read_ratio_env, 80)
    write_ratio = Runtime.env_positive_integer(@write_ratio_env, 20)

    payload_profiles = Map.fetch!(config, :payload_profiles)
    payload = E2EHelpers.payload(payload_profiles, :small)

    operation_ring =
      read_ratio
      |> Aerospike.Bench.deterministic_mix(write_ratio, @mix_size, seed: @seed)
      |> Aerospike.Bench.prepare_ring()

    metadata = Aerospike.Bench.run_metadata(config)
    state = build_state(target.namespace, target.set, payload, operation_ring)

    E2EHelpers.with_started_client(
      @conn_name,
      target.host,
      target.port,
      @cluster_ready_timeout_ms,
      fn ->
        bootstrap_dataset!(@conn_name, state)

        Runtime.print_metadata(metadata, %{
          workload: :wl_001,
          read_ratio: read_ratio,
          write_ratio: write_ratio,
          operation_seed: @seed
        })

        try do
          Enum.each(Map.fetch!(config, :concurrency), fn concurrency ->
            run_for_concurrency(config, concurrency, read_ratio, write_ratio, state)
          end)
        after
          teardown_dataset(@conn_name, state)
        end
      end
    )
  end

  defp run_for_concurrency(config, concurrency, read_ratio, write_ratio, state) do
    operation_counter = :atomics.new(1, [])

    Benchee.run(
      %{
        "WL-001 RU #{read_ratio}/#{write_ratio}" => fn ->
          execute_next_operation(state, operation_counter)
        end
      },
      Aerospike.Bench.benchee_options(
        config,
        title:
          "L3 mixed-workload baseline (concurrency=#{concurrency}, mix=RU #{read_ratio}/#{write_ratio})",
        parallel: concurrency,
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp execute_next_operation(state, operation_counter) do
    case Aerospike.Bench.ring_next(state.operation_ring, operation_counter) do
      :read ->
        {:ok, %Record{bins: bins}} = Aerospike.get(@conn_name, state.read_key)
        true = map_size(bins) > 0

      :write ->
        :ok =
          Aerospike.put(
            @conn_name,
            Aerospike.Bench.ring_next(state.write_ring, state.write_counter),
            state.payload
          )
    end
  end

  defp build_state(namespace, set, payload, operation_ring) do
    %{
      payload: payload,
      read_key: Aerospike.key(namespace, set, "wl:read"),
      write_ring:
        namespace
        |> E2EHelpers.key_ring(set, "wl:write", @ring_size)
        |> Aerospike.Bench.prepare_ring(),
      write_counter: :atomics.new(1, []),
      operation_ring: operation_ring
    }
  end

  defp bootstrap_dataset!(conn_name, state) do
    :ok = Aerospike.put(conn_name, state.read_key, state.payload)

    {write_ring, _write_ring_size} = state.write_ring
    E2EHelpers.bootstrap_keys!(conn_name, Tuple.to_list(write_ring), state.payload)
  end

  defp teardown_dataset(conn_name, state) do
    {write_ring, _write_ring_size} = state.write_ring
    E2EHelpers.teardown_keys(conn_name, [state.read_key | Tuple.to_list(write_ring)])
  end
end

Aerospike.Bench.Workload.RU8020.run()
