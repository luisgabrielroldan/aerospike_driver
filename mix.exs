defmodule Aerospike.MixProject do
  use Mix.Project

  @bench_profile_env "BENCH_PROFILE"
  @bench_profile_flags %{
    "--quick" => "quick",
    "--default" => "default",
    "--full" => "full"
  }
  @version "0.1.0"
  @description "Aerospike driver for Elixir with an OTP-native cluster runtime"
  @ce_integration_files [
    "test/integration/compression_test.exs",
    "test/integration/get_pool_test.exs",
    "test/integration/get_test.exs",
    "test/integration/index_query_test.exs",
    "test/integration/node_kill_test.exs",
    "test/integration/operate_cdt_test.exs",
    "test/integration/stream_transport_test.exs",
    "test/integration/udf_apply_test.exs",
    "test/integration/udf_lifecycle_test.exs",
    "test/integration/write_family_test.exs"
  ]
  @cluster_integration_files [
    "test/integration/batch_get_test.exs",
    "test/integration/scan_test.exs"
  ]
  @enterprise_integration_files [
    "test/integration/auth_test.exs",
    "test/integration/operator_surface_smoke_test.exs",
    "test/integration/security_admin_test.exs",
    "test/integration/tls_test.exs",
    "test/integration/txn_test.exs"
  ]

  def project do
    [
      app: :aerospike_driver,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: @description,
      package: package(),
      docs: docs(),
      preferred_cli_env: preferred_cli_env(),
      aliases: aliases(),
      test_coverage: [summary: [threshold: 78]],
      dialyzer: [
        plt_add_apps: [:ex_unit, :ssl, :public_key],
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"}
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :ssl, :public_key]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      name: "aerospike_driver",
      files: ~w(lib priv .formatter.exs mix.exs mix.lock README.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"]
    ]
  end

  defp preferred_cli_env do
    [
      bench: :dev,
      docs: :dev,
      "test.unit": :test,
      "test.coverage": :test,
      "test.coverage.live": :test,
      "test.coverage.all": :test,
      "test.integration.ce": :test,
      "test.integration.cluster": :test,
      "test.integration.enterprise": :test,
      "test.integration.all": :test,
      "test.live": :test,
      validate: :test
    ]
  end

  defp aliases do
    [
      bench: &bench/1,
      "bench.clean": &bench_clean/1,
      "test.unit": "test --seed 0",
      "test.coverage": "test --cover --seed 0",
      "test.coverage.live": "test --cover --include integration --include cluster --seed 0",
      "test.coverage.all":
        "test --cover --include integration --include cluster --include enterprise --seed 0",
      "test.integration.ce":
        integration_alias(@ce_integration_files, "--include integration --seed 0"),
      "test.integration.cluster":
        integration_alias(
          @cluster_integration_files,
          "--include integration --include cluster --seed 0"
        ),
      "test.integration.enterprise":
        integration_alias(
          @enterprise_integration_files,
          "--include integration --include enterprise --seed 0"
        ),
      "test.integration.all": [
        "test.integration.ce",
        "test.integration.cluster",
        "test.integration.enterprise"
      ],
      "test.live": [
        "test.integration.ce",
        "test.integration.cluster"
      ],
      validate: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "credo --strict",
        "test.unit",
        "test.coverage"
      ]
    ]
  end

  defp integration_alias(files, opts) do
    Enum.join(["test", opts | files], " ")
  end

  defp deps do
    [
      {:nimble_pool, "~> 1.0"},
      {:telemetry, "~> 1.3"},
      {:bcrypt_elixir, "~> 3.3"},
      {:telemetry_metrics, "~> 1.0", only: :test},
      {:benchee, "~> 1.3", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp bench(args) do
    {options, scripts} = split_bench_args(args)
    maybe_set_bench_profile(options)

    scripts_to_run =
      case scripts do
        [] ->
          Path.wildcard("bench/tests/**/*_bench.exs")
          |> Enum.sort()

        _ ->
          scripts
      end

    run_bench_scripts(scripts_to_run)
  end

  defp run_bench_scripts([]) do
    Mix.shell().info("No benchmark scripts found under bench/tests/.")
  end

  defp run_bench_scripts(paths) do
    scripts =
      paths
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.uniq()

    Enum.each(scripts, &validate_bench_script!/1)

    Mix.Task.reenable("run")
    Mix.Task.run("run", scripts)
  end

  defp validate_bench_script!(path) do
    cond do
      not String.ends_with?(path, "_bench.exs") ->
        Mix.raise("Benchmark script must match *_bench.exs: #{path}")

      Path.extname(path) != ".exs" ->
        Mix.raise("Benchmark script must be an .exs file: #{path}")

      not File.regular?(path) ->
        Mix.raise("Benchmark script not found: #{path}")

      true ->
        :ok
    end
  end

  defp split_bench_args(args) do
    {options, scripts} = Enum.split_with(args, &String.starts_with?(&1, "--"))
    known_flags = Map.keys(@bench_profile_flags)
    invalid_flags = options -- known_flags

    case invalid_flags do
      [] ->
        {options, scripts}

      _ ->
        Mix.raise(
          "Unknown bench option(s): #{Enum.join(invalid_flags, ", ")}\nUsage: mix bench [--quick|--default|--full] [bench/tests/..._bench.exs ...]"
        )
    end
  end

  defp maybe_set_bench_profile(options) do
    profile_flags = Enum.filter(options, &Map.has_key?(@bench_profile_flags, &1))

    case profile_flags do
      [] ->
        :ok

      [flag] ->
        profile = Map.fetch!(@bench_profile_flags, flag)
        System.put_env(@bench_profile_env, profile)
        Mix.shell().info("Using benchmark profile: #{profile}")

      _multiple ->
        Mix.raise("Use only one profile option: --quick, --default, or --full")
    end
  end

  defp bench_clean(args) do
    results_dir = Path.join("bench", "results")
    run_dirs = result_run_dirs(results_dir)

    case {args, run_dirs} do
      {_, []} ->
        Mix.shell().info("No benchmark result directories found under #{results_dir}/.")

      {["--all"], _dirs} ->
        Enum.each(run_dirs, &File.rm_rf!/1)
        Mix.shell().info("Removed #{length(run_dirs)} benchmark result directories.")

      {[], [_only]} ->
        Mix.shell().info("Only one benchmark result directory exists; nothing to clean.")

      {[], dirs} ->
        [latest | older] = sort_dirs_by_mtime_desc(dirs)
        Enum.each(older, &File.rm_rf!/1)

        Mix.shell().info(
          "Removed #{length(older)} benchmark result directories; kept latest: #{Path.basename(latest)}"
        )

      _ ->
        Mix.raise("Usage: mix bench.clean [--all]")
    end
  end

  defp result_run_dirs(results_dir) do
    case File.ls(results_dir) do
      {:ok, entries} ->
        entries
        |> Enum.map(&Path.join(results_dir, &1))
        |> Enum.filter(&File.dir?/1)

      {:error, :enoent} ->
        []

      {:error, reason} ->
        Mix.raise("Failed to list benchmark results in #{results_dir}: #{inspect(reason)}")
    end
  end

  defp sort_dirs_by_mtime_desc(dirs) do
    Enum.sort_by(dirs, &directory_mtime/1, :desc)
  end

  defp directory_mtime(path) do
    case File.stat(path, time: :posix) do
      {:ok, %{mtime: mtime}} ->
        mtime

      {:error, _reason} ->
        0
    end
  end
end
