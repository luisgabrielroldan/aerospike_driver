Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.E2E.CrudBaseline do
  @moduledoc false

  alias Aerospike.Bench.Support.E2EHelpers
  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Key
  alias Aerospike.Record

  @conn_name :bench_e2e
  @ring_size 256

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config)
    target = Runtime.connection_target("bench_e2e")

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
      target.namespace,
      fn ->
        bootstrap_dataset!(@conn_name, state)
        Runtime.print_metadata(metadata, %{workload: :crud_baseline})

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
      "E2E-001 Put Small" => fn ->
        {:ok, _metadata} =
          Aerospike.put(
            @conn_name,
            Aerospike.Bench.ring_next(state.small_ring, state.small_counter),
            state.small_payload
          )
      end,
      "E2E-002 Get Small" => fn ->
        {:ok, %Record{bins: bins}} = Aerospike.get(@conn_name, state.small_read_key)
        true = map_size(bins) > 0
      end,
      "E2E-003 Put Large" => fn ->
        {:ok, _metadata} =
          Aerospike.put(
            @conn_name,
            Aerospike.Bench.ring_next(state.large_ring, state.large_counter),
            state.large_payload
          )
      end,
      "E2E-004 Get Large" => fn ->
        {:ok, %Record{bins: bins}} = Aerospike.get(@conn_name, state.large_read_key)
        true = map_size(bins) > 0
      end,
      "E2E-005 Exists" => fn ->
        {:ok, true} = Aerospike.exists(@conn_name, state.small_read_key)
      end,
      "E2E-006 Delete" => fn ->
        {:ok, _deleted?} =
          Aerospike.delete(
            @conn_name,
            Aerospike.Bench.ring_next(state.delete_ring, state.delete_counter)
          )
      end,
      "E2E-007 Touch" => fn ->
        {:ok, _metadata} =
          Aerospike.touch(
            @conn_name,
            Aerospike.Bench.ring_next(state.touch_ring, state.touch_counter)
          )
      end
    }

    Benchee.run(
      jobs,
      Aerospike.Bench.benchee_options(
        config,
        title: "L2 single-op E2E baseline (concurrency=#{concurrency})",
        parallel: concurrency,
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp build_state(namespace, set, small_payload, large_payload) do
    %{
      small_payload: small_payload,
      large_payload: large_payload,
      small_read_key: Key.new(namespace, set, "e2e:read:small"),
      large_read_key: Key.new(namespace, set, "e2e:read:large"),
      small_ring:
        namespace
        |> E2EHelpers.key_ring(set, "e2e:put:small", @ring_size)
        |> Aerospike.Bench.prepare_ring(),
      large_ring:
        namespace
        |> E2EHelpers.key_ring(set, "e2e:put:large", @ring_size)
        |> Aerospike.Bench.prepare_ring(),
      delete_ring:
        namespace
        |> E2EHelpers.key_ring(set, "e2e:delete", @ring_size)
        |> Aerospike.Bench.prepare_ring(),
      touch_ring:
        namespace
        |> E2EHelpers.key_ring(set, "e2e:touch", @ring_size)
        |> Aerospike.Bench.prepare_ring(),
      small_counter: :atomics.new(1, []),
      large_counter: :atomics.new(1, []),
      delete_counter: :atomics.new(1, []),
      touch_counter: :atomics.new(1, [])
    }
  end

  defp bootstrap_dataset!(conn_name, state) do
    {:ok, _metadata} = Aerospike.put(conn_name, state.small_read_key, state.small_payload)
    {:ok, _metadata} = Aerospike.put(conn_name, state.large_read_key, state.large_payload)

    {small_ring, _small_ring_size} = state.small_ring
    {large_ring, _large_ring_size} = state.large_ring
    {delete_ring, _delete_ring_size} = state.delete_ring
    {touch_ring, _touch_ring_size} = state.touch_ring

    E2EHelpers.bootstrap_keys!(conn_name, Tuple.to_list(small_ring), state.small_payload)
    E2EHelpers.bootstrap_keys!(conn_name, Tuple.to_list(large_ring), state.large_payload)
    E2EHelpers.bootstrap_keys!(conn_name, Tuple.to_list(delete_ring), state.small_payload)
    E2EHelpers.bootstrap_keys!(conn_name, Tuple.to_list(touch_ring), state.small_payload)
  end

  defp teardown_dataset(conn_name, state) do
    {small_ring, _small_ring_size} = state.small_ring
    {large_ring, _large_ring_size} = state.large_ring
    {delete_ring, _delete_ring_size} = state.delete_ring
    {touch_ring, _touch_ring_size} = state.touch_ring

    keys =
      [state.small_read_key, state.large_read_key]
      |> Kernel.++(Tuple.to_list(small_ring))
      |> Kernel.++(Tuple.to_list(large_ring))
      |> Kernel.++(Tuple.to_list(delete_ring))
      |> Kernel.++(Tuple.to_list(touch_ring))

    E2EHelpers.teardown_keys(conn_name, keys)
  end
end

Aerospike.Bench.E2E.CrudBaseline.run()
