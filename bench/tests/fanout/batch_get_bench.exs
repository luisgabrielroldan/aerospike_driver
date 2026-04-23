Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.Fanout.BatchGet do
  @moduledoc false

  alias Aerospike.Bench.Support.E2EHelpers
  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Cluster.Supervisor, as: ClusterSupervisor
  alias Aerospike.Cluster.Tender
  alias Aerospike.Key
  alias Aerospike.Record

  @conn_name :bench_fanout
  @namespace "test"
  @hosts ["localhost:3000", "localhost:3010", "localhost:3020"]

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config)
    set_name = System.get_env("BENCH_SET", "bench_fanout")
    batch_sizes = Map.get(config, :fanout_batch_sizes, [100])
    payload = E2EHelpers.payload(Map.fetch!(config, :payload_profiles), :small)

    verify_cluster_available!()
    stop_client()

    {:ok, _pid} =
      Aerospike.start_link(
        name: @conn_name,
        transport: Aerospike.Transport.Tcp,
        hosts: @hosts,
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 16
      )

    :ok = Tender.tend_now(@conn_name)
    true = Tender.ready?(@conn_name)

    state = build_state(set_name, payload, batch_sizes)

    Runtime.print_metadata(metadata, %{workload: :batch_get_fanout})

    try do
      bootstrap_dataset!(@conn_name, state)

      Enum.each(Map.fetch!(config, :concurrency), fn concurrency ->
        Enum.each(state.batch_keysets, fn {batch_size, keys} ->
          run_for_concurrency(config, concurrency, batch_size, keys)
        end)
      end)
    after
      teardown_dataset(@conn_name, state)
      stop_client()
    end
  end

  defp run_for_concurrency(config, concurrency, batch_size, keys) do
    Benchee.run(
      %{
        "FO-001 Batch Get #{batch_size} keys" => fn ->
          {:ok, records} = Aerospike.batch_get(@conn_name, keys)
          true = length(records) == batch_size

          true =
            Enum.all?(records, fn
              {:ok, %Record{bins: bins}} when map_size(bins) > 0 -> true
              _ -> false
            end)
        end
      },
      Aerospike.Bench.benchee_options(
        config,
        title: "L4 fan-out baseline (concurrency=#{concurrency}, batch_size=#{batch_size})",
        parallel: concurrency,
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp build_state(set_name, payload, batch_sizes) do
    batch_keysets =
      Enum.map(batch_sizes, fn batch_size ->
        keys =
          Enum.map(1..batch_size, fn index ->
            Key.new(@namespace, set_name, "fo:batch:get:#{batch_size}:#{index}")
          end)

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

  defp verify_cluster_available! do
    Enum.each(@hosts, fn host_port ->
      [host, port] = String.split(host_port, ":", parts: 2)
      E2EHelpers.verify_server_available!(host, String.to_integer(port))
    end)
  end

  defp stop_client do
    Supervisor.stop(ClusterSupervisor.sup_name(@conn_name))
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end
end

Aerospike.Bench.Fanout.BatchGet.run()
