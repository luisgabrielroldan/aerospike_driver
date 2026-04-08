Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.E2E.OperateMultiOp do
  @moduledoc false

  import Aerospike.Op

  alias Aerospike.Bench.Support.E2EHelpers
  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Record

  @conn_name :bench_e2e_operate
  @ring_size 256
  @cluster_ready_timeout_ms 5_000

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config)
    target = Runtime.connection_target("bench_e2e_operate")

    payload_profiles = Map.fetch!(config, :payload_profiles)
    small_payload = E2EHelpers.payload(payload_profiles, :small)
    large_payload = E2EHelpers.payload(payload_profiles, :large)

    state =
      build_state(
        target.namespace,
        target.set,
        small_payload,
        large_payload
      )

    E2EHelpers.with_started_client(
      @conn_name,
      target.host,
      target.port,
      @cluster_ready_timeout_ms,
      fn ->
        bootstrap_dataset!(@conn_name, state)
        Runtime.print_metadata(metadata, %{workload: :e2e_007})

        try do
          Enum.each(Map.fetch!(config, :concurrency), fn concurrency ->
            run_for_concurrency(config, concurrency, state)
          end)
        after
          teardown_dataset(@conn_name, state)
        end
      end
    )
  end

  defp run_for_concurrency(config, concurrency, state) do
    jobs = %{
      "E2E-007 Operate Small Multi-Op" => fn ->
        {:ok, %Record{bins: bins}} =
          Aerospike.operate(
            @conn_name,
            Aerospike.Bench.ring_next(state.small_ring, state.small_counter),
            state.small_operations
          )

        true = map_size(bins) > 0
        true = is_integer(Map.get(bins, "counter"))
        true = is_binary(Map.get(bins, state.small_payload_bin))
      end,
      "E2E-007 Operate Large Multi-Op" => fn ->
        {:ok, %Record{bins: bins}} =
          Aerospike.operate(
            @conn_name,
            Aerospike.Bench.ring_next(state.large_ring, state.large_counter),
            state.large_operations
          )

        true = map_size(bins) > 0
        true = is_integer(Map.get(bins, "counter"))
        true = is_binary(Map.get(bins, state.large_payload_bin))
      end
    }

    Benchee.run(
      jobs,
      Aerospike.Bench.benchee_options(
        config,
        title: "L2 multi-op E2E operate baseline (concurrency=#{concurrency})",
        parallel: concurrency,
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp build_state(namespace, set, small_payload, large_payload) do
    {small_payload_bin, small_payload_value} = single_payload_bin!(small_payload)
    {large_payload_bin, large_payload_value} = single_payload_bin!(large_payload)

    %{
      small_seed_payload: Map.put(small_payload, "counter", 0),
      large_seed_payload: Map.put(large_payload, "counter", 0),
      small_payload_bin: small_payload_bin,
      large_payload_bin: large_payload_bin,
      small_read_key: Aerospike.key(namespace, set, "e2e:operate:read:small"),
      large_read_key: Aerospike.key(namespace, set, "e2e:operate:read:large"),
      small_ring:
        namespace
        |> E2EHelpers.key_ring(set, "e2e:operate:small", @ring_size)
        |> Aerospike.Bench.prepare_ring(),
      large_ring:
        namespace
        |> E2EHelpers.key_ring(set, "e2e:operate:large", @ring_size)
        |> Aerospike.Bench.prepare_ring(),
      small_counter: :atomics.new(1, []),
      large_counter: :atomics.new(1, []),
      small_operations: operation_bundle(small_payload_bin, small_payload_value),
      large_operations: operation_bundle(large_payload_bin, large_payload_value)
    }
  end

  defp operation_bundle(payload_bin, payload_value)
       when is_binary(payload_bin) and is_binary(payload_value) do
    [
      put(payload_bin, payload_value),
      add("counter", 1),
      get("counter"),
      get(payload_bin)
    ]
  end

  defp single_payload_bin!(payload) when is_map(payload) do
    case Map.to_list(payload) do
      [{payload_bin, payload_value}] when is_binary(payload_bin) and is_binary(payload_value) ->
        {payload_bin, payload_value}

      _ ->
        raise ArgumentError, "expected single-bin payload map, got: #{inspect(payload)}"
    end
  end

  defp bootstrap_dataset!(conn_name, state) do
    :ok = Aerospike.put(conn_name, state.small_read_key, state.small_seed_payload)
    :ok = Aerospike.put(conn_name, state.large_read_key, state.large_seed_payload)

    small_ring_keys = E2EHelpers.ring_keys(state.small_ring)
    large_ring_keys = E2EHelpers.ring_keys(state.large_ring)

    E2EHelpers.bootstrap_keys!(conn_name, small_ring_keys, state.small_seed_payload)
    E2EHelpers.bootstrap_keys!(conn_name, large_ring_keys, state.large_seed_payload)
  end

  defp teardown_dataset(conn_name, state) do
    keys =
      [state.small_read_key, state.large_read_key]
      |> Kernel.++(E2EHelpers.ring_keys(state.small_ring))
      |> Kernel.++(E2EHelpers.ring_keys(state.large_ring))

    E2EHelpers.teardown_keys(conn_name, keys)
  end
end

Aerospike.Bench.E2E.OperateMultiOp.run()
