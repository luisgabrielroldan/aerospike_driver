Code.require_file("scenarios/default.exs", __DIR__)
Code.require_file("config.exs", __DIR__)
Code.require_file("reporting.exs", __DIR__)
Code.require_file("support/e2e_helpers.exs", __DIR__)
Code.require_file("support/fixtures.exs", __DIR__)
Code.require_file("support/runtime.exs", __DIR__)

defmodule Aerospike.Bench do
  @moduledoc false

  @server_version_env "AEROSPIKE_SERVER_VERSION"
  @default_benchee_options [formatters: [{Benchee.Formatters.Console, extended_statistics: true}]]

  def load_config(overrides \\ %{}), do: Aerospike.Bench.Config.load(overrides)

  def run_metadata(config, overrides \\ %{}) when is_map(config) and is_map(overrides) do
    %{
      git_sha: git_sha(),
      elixir_version: System.version(),
      otp_release: System.otp_release(),
      aerospike_server_version: server_version(overrides),
      benchmark_profile: Map.fetch!(config, :profile),
      scenario_config: config,
      host_summary: host_summary()
    }
  end

  def benchee_options(config, overrides \\ [])
      when is_map(config) and is_list(overrides) do
    report_options = Aerospike.Bench.Reporting.formatter_options(run_metadata(config))

    [time: Map.fetch!(config, :duration_s), warmup: Map.fetch!(config, :warmup_s)]
    |> Keyword.merge(@default_benchee_options)
    |> Keyword.update(
      :formatters,
      [{Aerospike.Bench.Reporting, report_options}],
      fn formatters -> formatters ++ [{Aerospike.Bench.Reporting, report_options}] end
    )
    |> Keyword.merge(overrides)
  end

  def deterministic_mix(read_ratio, write_ratio, size, opts \\ [])
      when is_integer(read_ratio) and is_integer(write_ratio) and is_integer(size) and size > 0 do
    ensure_ratio!(read_ratio, write_ratio)

    total_ratio = read_ratio + write_ratio
    read_count = div(size * read_ratio, total_ratio)
    write_count = size - read_count
    seed = Keyword.get(opts, :seed, 8020)

    List.duplicate(:read, read_count)
    |> Kernel.++(List.duplicate(:write, write_count))
    |> Enum.with_index()
    |> Enum.sort_by(fn {operation, index} -> :erlang.phash2({seed, index, operation}) end)
    |> Enum.map(&elem(&1, 0))
  end

  def prepare_ring(items) when is_list(items) and items != [] do
    {List.to_tuple(items), length(items)}
  end

  def ring_next({items, size}, counter) when is_tuple(items) and is_integer(size) and size > 0 do
    index = :atomics.add_get(counter, 1, 1)
    :erlang.element(rem(index - 1, size) + 1, items)
  end

  def primitive_encoding_inputs do
    %{
      "int/64b" => 42,
      "float/64b" => 3.141_592_653_589_793,
      "string/128b" => String.duplicate("s", 128),
      "bytes/256b" => {:bytes, :binary.copy(<<170>>, 256)},
      "bool/true" => true,
      "nil" => nil
    }
  end

  defp git_sha do
    case System.cmd("git", ["rev-parse", "HEAD"], stderr_to_stdout: true) do
      {sha, 0} -> String.trim(sha)
      _ -> "unknown"
    end
  end

  defp server_version(overrides) do
    case Map.fetch(overrides, :aerospike_server_version) do
      {:ok, value} -> value
      :error -> System.get_env(@server_version_env, "unknown")
    end
  end

  defp host_summary do
    %{
      architecture: :erlang.system_info(:system_architecture) |> List.to_string(),
      logical_processors: logical_processors(),
      memory_bytes: memory_bytes()
    }
  end

  defp logical_processors do
    :erlang.system_info(:logical_processors_available)
    |> normalize_processor_value()
  end

  defp normalize_processor_value(:unknown), do: :erlang.system_info(:logical_processors)
  defp normalize_processor_value(value), do: value

  defp memory_bytes do
    case :os.type() do
      {:unix, :darwin} -> read_sysctl_value("hw.memsize")
      {:unix, :linux} -> read_meminfo_value()
      _ -> "unknown"
    end
  end

  defp read_sysctl_value(key) do
    case System.cmd("sysctl", ["-n", key], stderr_to_stdout: true) do
      {value, 0} ->
        value
        |> String.trim()
        |> parse_integer_or_unknown()

      _ ->
        "unknown"
    end
  end

  defp read_meminfo_value do
    case File.read("/proc/meminfo") do
      {:ok, contents} ->
        parse_meminfo(contents)

      {:error, _reason} ->
        "unknown"
    end
  end

  defp parse_meminfo(contents) do
    case Regex.run(~r/^MemTotal:\s+(\d+)\s+kB$/m, contents) do
      [_, kilobytes] ->
        with {value, ""} <- Integer.parse(kilobytes) do
          value * 1024
        else
          _ -> "unknown"
        end

      _ ->
        "unknown"
    end
  end

  defp parse_integer_or_unknown(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _ -> "unknown"
    end
  end

  defp ensure_ratio!(read_ratio, write_ratio) when read_ratio > 0 and write_ratio > 0, do: :ok

  defp ensure_ratio!(read_ratio, write_ratio) do
    raise ArgumentError,
          "read/write ratios must be positive integers, got read=#{inspect(read_ratio)} write=#{inspect(write_ratio)}"
  end
end
