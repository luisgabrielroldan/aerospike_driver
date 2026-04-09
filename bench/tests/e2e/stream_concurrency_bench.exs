Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.E2E.StreamConcurrency do
  @moduledoc false

  alias Aerospike.Bench.Support.E2EHelpers
  alias Aerospike.Bench.Support.Runtime

  @conn_name :bench_e2e_stream_concurrency
  @cluster_ready_timeout_ms 5_000
  @benchmark_title "L2 stream multi-node concurrency"
  @summary_sidecar_suffix "summary"
  @default_stream_record_count 25_000
  @default_ttfr_trials 12
  @default_max_concurrent_nodes [1, 2, 0]

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config)
    target = Runtime.connection_target("bench_e2e_stream_concurrency")

    payload_profiles = Map.fetch!(config, :payload_profiles)
    payload = E2EHelpers.payload(payload_profiles, :small)
    run_id = Aerospike.Bench.Reporting.run_id()
    benchmark_set = benchmark_set_name(target.set, run_id)
    state = build_state(config, target.namespace, benchmark_set, payload)

    E2EHelpers.with_started_client(
      @conn_name,
      target.host,
      target.port,
      @cluster_ready_timeout_ms,
      fn ->
        node_count = fetch_node_count!()

        {concurrency_settings, skipped_settings} =
          effective_max_concurrent_nodes(
            Map.get(config, :stream_max_concurrent_nodes, @default_max_concurrent_nodes),
            node_count
          )

        maybe_print_skipped_settings(skipped_settings, node_count)

        bootstrap_dataset!(@conn_name, state)

        Runtime.print_metadata(metadata, %{
          workload: :e2e_stream_concurrency,
          node_count: node_count,
          benchmark_set: benchmark_set,
          stream_record_count: state.stream_record_count,
          ttfr_trials: state.ttfr_trials,
          max_concurrent_nodes: concurrency_settings
        })

        try do
          ttfr_metrics =
            ttfr_metrics(
              concurrency_settings,
              state.ttfr_trials,
              state.scan,
              state.stream_record_count
            )

          suite =
            throughput_benchmark(
              config,
              concurrency_settings,
              state.scan,
              state.stream_record_count
            )

          throughput_metrics =
            throughput_metrics(suite, concurrency_settings, state.stream_record_count)

          write_summary_artifact!(%{
            run_id: run_id,
            generated_at_utc:
              DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601(),
            benchmark: @benchmark_title,
            metadata:
              Map.merge(metadata, %{
                node_count: node_count,
                benchmark_set: benchmark_set,
                stream_record_count: state.stream_record_count,
                ttfr_trials: state.ttfr_trials,
                max_concurrent_nodes: concurrency_settings
              }),
            ttfr: ttfr_metrics,
            throughput: throughput_metrics
          })
        after
          teardown_dataset(@conn_name, state)
        end
      end
    )
  end

  defp throughput_benchmark(config, concurrency_settings, scan, expected_record_count) do
    jobs =
      Enum.into(concurrency_settings, %{}, fn max_concurrent_nodes ->
        {
          throughput_job_name(max_concurrent_nodes),
          fn ->
            count = stream_record_count(scan, max_concurrent_nodes)
            true = count == expected_record_count
            count
          end
        }
      end)

    Benchee.run(
      jobs,
      Aerospike.Bench.benchee_options(
        config,
        title: @benchmark_title,
        parallel: 1,
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp ttfr_metrics(concurrency_settings, ttfr_trials, scan, expected_record_count) do
    Enum.map(concurrency_settings, fn max_concurrent_nodes ->
      samples_us =
        Enum.map(1..ttfr_trials, fn _trial ->
          ttfr_us = sample_ttfr_us(scan, max_concurrent_nodes)
          true = ttfr_us >= 0

          # Verify each TTFR trial actually observed a record.
          true = expected_record_count > 0
          ttfr_us
        end)

      %{
        max_concurrent_nodes: max_concurrent_nodes,
        sample_size: length(samples_us),
        microseconds: numeric_summary(samples_us),
        milliseconds: numeric_summary(Enum.map(samples_us, &to_ms/1))
      }
    end)
  end

  defp throughput_metrics(suite, concurrency_settings, expected_record_count) do
    Enum.map(concurrency_settings, fn max_concurrent_nodes ->
      scenario = find_scenario!(suite, throughput_job_name(max_concurrent_nodes))
      run_time_data = Map.get(scenario, :run_time_data) || %{}
      run_time_samples = Map.get(run_time_data, :samples) || []
      statistics = Map.get(run_time_data, :statistics) || %{}

      records_per_second_samples =
        Enum.map(run_time_samples, fn sample_ns ->
          records_per_second(sample_ns, expected_record_count)
        end)

      %{
        max_concurrent_nodes: max_concurrent_nodes,
        sample_size: length(run_time_samples),
        benchee_ips: Map.get(statistics, :ips),
        records_per_second: numeric_summary(records_per_second_samples)
      }
    end)
  end

  defp sample_ttfr_us(scan, max_concurrent_nodes) do
    started_at_us = System.monotonic_time(:microsecond)

    first_record =
      Aerospike.stream!(@conn_name, scan, max_concurrent_nodes: max_concurrent_nodes)
      |> Enum.take(1)

    elapsed_us = System.monotonic_time(:microsecond) - started_at_us

    case first_record do
      [_record] -> elapsed_us
      [] -> raise "stream produced no records while measuring TTFR"
    end
  end

  defp stream_record_count(scan, max_concurrent_nodes) do
    Aerospike.stream!(@conn_name, scan, max_concurrent_nodes: max_concurrent_nodes)
    |> Enum.reduce(0, fn _record, count -> count + 1 end)
  end

  defp build_state(config, namespace, set, payload) do
    stream_record_count = Map.get(config, :stream_record_count, @default_stream_record_count)
    ttfr_trials = Map.get(config, :stream_ttfr_trials, @default_ttfr_trials)
    keys = E2EHelpers.key_ring(namespace, set, "e2e:stream:concurrency", stream_record_count)

    %{
      payload: payload,
      keys: keys,
      scan: Aerospike.Scan.new(namespace, set),
      stream_record_count: stream_record_count,
      ttfr_trials: ttfr_trials
    }
  end

  defp fetch_node_count! do
    case Aerospike.nodes(@conn_name) do
      {:ok, nodes} ->
        node_count = length(nodes)

        if node_count < 2 do
          raise """
          stream concurrency benchmark requires a multi-node cluster, got #{node_count} node(s).
          Configure at least two Aerospike nodes before running this benchmark.
          """
        end

        node_count

      {:error, reason} ->
        raise "failed to fetch cluster node list: #{inspect(reason)}"
    end
  end

  defp effective_max_concurrent_nodes(settings, node_count) when is_list(settings) do
    (settings ++ [1, 0])
    |> Enum.uniq()
    |> Enum.reduce({[], []}, fn
      0, {kept, skipped} ->
        {[0 | kept], skipped}

      setting, {kept, skipped}
      when is_integer(setting) and setting > 0 and setting <= node_count ->
        {[setting | kept], skipped}

      setting, {kept, skipped} ->
        {kept, [setting | skipped]}
    end)
    |> then(fn {kept, skipped} -> {Enum.reverse(kept), Enum.reverse(skipped)} end)
  end

  defp maybe_print_skipped_settings([], _node_count), do: :ok

  defp maybe_print_skipped_settings(skipped_settings, node_count) do
    rendered =
      skipped_settings
      |> Enum.map(&inspect/1)
      |> Enum.join(", ")

    IO.puts(
      "Skipping max_concurrent_nodes settings not valid for #{node_count} cluster node(s): #{rendered}"
    )
  end

  defp write_summary_artifact!(payload) do
    case Aerospike.Bench.Reporting.write_json_sidecar(
           @benchmark_title,
           @summary_sidecar_suffix,
           payload
         ) do
      {:ok, _path} ->
        :ok

      {:error, reason} ->
        raise reason
    end
  end

  defp throughput_job_name(max_concurrent_nodes) do
    "E2E-STREAM Throughput max_concurrent_nodes=#{max_concurrent_nodes}"
  end

  defp find_scenario!(suite, job_name) do
    scenarios = Map.get(suite, :scenarios, [])

    case Enum.find(scenarios, fn scenario -> Map.get(scenario, :job_name) == job_name end) do
      nil -> raise "missing Benchee scenario for job #{job_name}"
      scenario -> scenario
    end
  end

  defp records_per_second(sample_ns, expected_record_count)
       when is_integer(sample_ns) and sample_ns > 0 and is_integer(expected_record_count) and
              expected_record_count > 0 do
    expected_record_count * 1_000_000_000 / sample_ns
  end

  defp numeric_summary([]), do: %{min: nil, median: nil, p95: nil}

  defp numeric_summary(samples) when is_list(samples) do
    sorted = Enum.sort(samples)

    %{
      min: Enum.at(sorted, 0),
      median: median(sorted),
      p95: percentile(sorted, 95)
    }
  end

  defp median(sorted_samples) do
    size = length(sorted_samples)
    midpoint = div(size, 2)

    if rem(size, 2) == 1 do
      Enum.at(sorted_samples, midpoint)
    else
      (Enum.at(sorted_samples, midpoint - 1) + Enum.at(sorted_samples, midpoint)) / 2
    end
  end

  defp percentile(sorted_samples, p) when is_integer(p) and p >= 0 and p <= 100 do
    size = length(sorted_samples)
    index = max(1, ceil(size * p / 100)) - 1
    Enum.at(sorted_samples, index)
  end

  defp benchmark_set_name(base_set, run_id) when is_binary(base_set) and is_binary(run_id) do
    suffix =
      run_id
      |> String.downcase()
      |> String.replace(~r/[^a-z0-9]/u, "")
      |> String.slice(-8, 8)
      |> case do
        "" -> "run"
        value -> value
      end

    "#{base_set}_#{suffix}"
  end

  defp to_ms(value) when is_integer(value), do: value / 1_000
  defp to_ms(value) when is_float(value), do: value / 1_000

  defp bootstrap_dataset!(conn_name, state) do
    E2EHelpers.bootstrap_keys!(conn_name, state.keys, state.payload)
  end

  defp teardown_dataset(conn_name, state) do
    E2EHelpers.teardown_keys(conn_name, state.keys)
  end
end

Aerospike.Bench.E2E.StreamConcurrency.run()
