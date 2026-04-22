defmodule Aerospike.Bench.Reporting do
  @moduledoc false

  @behaviour Benchee.Formatter

  @run_id_env "BENCH_RUN_ID"
  @default_benchmark_title "benchmark"
  @default_filename "results"
  @results_dir Path.expand("results", __DIR__)

  def formatter_options(metadata) when is_map(metadata) do
    %{
      run_id: run_id(),
      metadata: metadata,
      output_dir: @results_dir
    }
  end

  def run_id do
    persistent_run_id()
  end

  @impl Benchee.Formatter
  def format(suite, options) do
    metadata =
      options
      |> Map.get(:metadata, %{})
      |> with_fallback_metadata(suite)

    %{
      run_id: Map.fetch!(options, :run_id),
      generated_at_utc: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601(),
      benchmark: benchmark_info(suite),
      metadata: metadata,
      scenarios: scenario_reports(suite)
    }
  end

  @impl Benchee.Formatter
  def write(report, options) do
    output_dir = Map.get(options, :output_dir, @results_dir)
    output_path = report_path(output_dir, report)

    with :ok <- File.mkdir_p(Path.dirname(output_path)),
         :ok <- File.write(output_path, encode_json(report) <> "\n") do
      IO.puts("Wrote benchmark artifact: #{output_path}")
      :ok
    else
      {:error, reason} ->
        {:error, "failed to write benchmark artifact #{output_path}: #{inspect(reason)}"}
    end
  end

  defp persistent_run_id do
    key = {__MODULE__, :run_id}

    case :persistent_term.get(key, nil) do
      nil ->
        id = System.get_env(@run_id_env, default_run_id())
        :persistent_term.put(key, id)
        id

      id ->
        id
    end
  end

  defp default_run_id do
    DateTime.utc_now()
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601(:basic)
    |> String.replace(":", "")
  end

  defp with_fallback_metadata(%{} = metadata, suite) do
    Map.merge(
      %{
        benchee_system: system_info(suite),
        benchee_configuration: configuration_info(suite)
      },
      metadata
    )
  end

  defp benchmark_info(%{configuration: %{title: title}} = suite) do
    %{
      title: normalize_title(title),
      input_names: input_names(suite)
    }
  end

  defp benchmark_info(suite) do
    %{
      title: @default_benchmark_title,
      input_names: input_names(suite)
    }
  end

  defp normalize_title(nil), do: @default_benchmark_title
  defp normalize_title(""), do: @default_benchmark_title
  defp normalize_title(title), do: title

  defp configuration_info(%{configuration: nil}), do: %{}

  defp configuration_info(%{configuration: configuration}) do
    %{
      parallel: Map.get(configuration, :parallel),
      time_ns: Map.get(configuration, :time),
      warmup_ns: Map.get(configuration, :warmup),
      pre_check: Map.get(configuration, :pre_check),
      unit_scaling: Map.get(configuration, :unit_scaling)
    }
  end

  defp system_info(%{system: nil}), do: %{}

  defp system_info(%{system: system}) do
    %{
      os: Map.get(system, :os),
      elixir: Map.get(system, :elixir),
      erlang: Map.get(system, :erlang),
      jit_enabled: Map.get(system, :jit_enabled?),
      num_cores: Map.get(system, :num_cores),
      cpu_speed: Map.get(system, :cpu_speed),
      available_memory: Map.get(system, :available_memory)
    }
  end

  defp input_names(%{configuration: %{input_names: input_names}}) when is_list(input_names),
    do: input_names

  defp input_names(_suite), do: []

  defp scenario_reports(%{scenarios: scenarios}) when is_list(scenarios) do
    Enum.map(scenarios, &scenario_report/1)
  end

  defp scenario_reports(_suite), do: []

  defp scenario_report(scenario) do
    %{
      scenario_id: scenario_identifier(scenario),
      job_name: Map.get(scenario, :job_name),
      input_name: Map.get(scenario, :input_name),
      run_time: collection_data_report(Map.get(scenario, :run_time_data)),
      memory_usage: collection_data_report(Map.get(scenario, :memory_usage_data)),
      reductions: collection_data_report(Map.get(scenario, :reductions_data))
    }
  end

  defp scenario_identifier(scenario) do
    job_name = Map.get(scenario, :job_name, @default_filename)
    input_name = Map.get(scenario, :input_name)

    case input_name do
      nil -> job_name
      _ -> "#{job_name}::#{input_name}"
    end
  end

  defp collection_data_report(nil), do: %{}

  defp collection_data_report(collection_data) do
    samples = Map.get(collection_data, :samples, [])
    statistics = Map.get(collection_data, :statistics)

    %{
      sample_size: length(samples),
      statistics: statistics_report(statistics)
    }
  end

  defp statistics_report(nil), do: %{}

  defp statistics_report(statistics) do
    statistics
    |> Map.from_struct()
    |> Enum.into(%{}, fn {key, value} -> {to_string(key), normalize_value(value)} end)
  end

  defp report_path(output_dir, report) do
    benchmark_title =
      report
      |> Map.get(:benchmark, %{})
      |> Map.get(:title, @default_filename)
      |> slugify()

    run_id = report |> Map.get(:run_id, @default_filename) |> slugify()
    Path.join([output_dir, run_id, "#{benchmark_title}.json"])
  end

  defp slugify(value) when is_binary(value) do
    value
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9]+/u, "-")
    |> String.trim("-")
    |> case do
      "" -> @default_filename
      slug -> slug
    end
  end

  defp slugify(value), do: value |> to_string() |> slugify()

  defp encode_json(data), do: encode_json_value(normalize_value(data))

  defp normalize_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp normalize_value(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)

  defp normalize_value(%{} = value),
    do: Enum.into(value, %{}, fn {k, v} -> {normalize_key(k), normalize_value(v)} end)

  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)

  defp normalize_value(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&normalize_value/1)

  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp normalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_key(key), do: to_string(key)

  defp encode_json_value(value) when is_map(value) do
    entries =
      value
      |> Enum.sort_by(fn {key, _value} -> key end)
      |> Enum.map(fn {key, entry_value} ->
        encode_json_string(key) <> ":" <> encode_json_value(entry_value)
      end)

    "{" <> Enum.join(entries, ",") <> "}"
  end

  defp encode_json_value(value) when is_list(value) do
    "[" <> (value |> Enum.map(&encode_json_value/1) |> Enum.join(",")) <> "]"
  end

  defp encode_json_value(value) when is_binary(value), do: encode_json_string(value)
  defp encode_json_value(value) when is_integer(value), do: Integer.to_string(value)

  defp encode_json_value(value) when is_float(value),
    do: :erlang.float_to_binary(value, [:compact, decimals: 16])

  defp encode_json_value(true), do: "true"
  defp encode_json_value(false), do: "false"
  defp encode_json_value(nil), do: "null"

  defp encode_json_string(value) do
    escaped =
      value
      |> String.replace("\\", "\\\\")
      |> String.replace("\"", "\\\"")
      |> String.replace("\b", "\\b")
      |> String.replace("\f", "\\f")
      |> String.replace("\n", "\\n")
      |> String.replace("\r", "\\r")
      |> String.replace("\t", "\\t")

    "\"" <> escaped <> "\""
  end
end
