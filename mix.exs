defmodule Aerospike.MixProject do
  use Mix.Project

  @bench_profile_env "BENCH_PROFILE"
  @bench_profile_flags %{
    "--quick" => "quick",
    "--default" => "default",
    "--full" => "full"
  }

  @version "0.2.0"
  @source_url "https://github.com/luisgabrielroldan/aerospike_driver"
  @description "Idiomatic Elixir client for the Aerospike database"

  def project do
    [
      app: :aerospike_driver,
      version: @version,
      elixir: "~> 1.15",
      aliases: aliases(),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),

      # Hex
      description: @description,
      package: package(),

      # Docs
      name: "Aerospike Driver",
      source_url: @source_url,
      docs: docs(),

      # Dialyzer
      dialyzer: [
        plt_add_apps: [:ex_unit],
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"}
      ],
      preferred_cli_env: [
        bench: :dev,
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.lcov": :test,
        "coveralls.json": :test,
        "test.all": :test
      ],
      test_coverage: [tool: ExCoveralls]
    ]
  end

  defp package do
    [
      name: "aerospike_driver",
      maintainers: ["Gabriel Roldan"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"
      },
      files: ~w(lib guides .formatter.exs mix.exs README.md CHANGELOG.md LICENSE)
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto, :ssl, :telemetry]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:jason, "~> 1.4"},
      {:nimble_options, "~> 1.0"},
      {:nimble_pool, "~> 1.0"},
      {:telemetry, "~> 1.0"},
      # Dev/test only
      {:stream_data, "~> 1.0", only: [:dev, :test]},
      {:benchee, "~> 1.3", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp aliases do
    [
      bench: &bench/1,
      "bench.clean": &bench_clean/1,
      coveralls: "coveralls --include integration",
      "coveralls.detail": "coveralls.detail --include integration",
      "coveralls.html": "coveralls.html --include integration",
      "coveralls.json": "coveralls.json --include integration",
      "coveralls.lcov": "coveralls.lcov --include integration",
      "test.coverage": "coveralls.html --include integration",
      "test.all":
        "test --include integration --include property --include cluster --include enterprise --include tls_stack",
      "test.enterprise": "test --include enterprise"
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

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      source_ref: "v#{@version}",
      homepage_url: @source_url,
      formatters: ["html"],
      extras: [
        {"README.md", [title: "Overview"]},
        {"CHANGELOG.md", [title: "Changelog"]},
        {"LICENSE", [title: "License"]},
        "guides/getting-started.md",
        "guides/operate-and-cdt.md",
        "guides/map-patterns.md",
        "guides/list-patterns.md",
        "guides/nested-operations.md",
        "guides/batch-operations.md",
        "guides/queries-and-scanning.md",
        "guides/expressions.md",
        "guides/policies.md",
        "guides/transactions.md",
        "guides/secondary-indexes.md",
        "guides/udfs.md"
      ],
      groups_for_extras: [
        Tutorials: ~r/guides\/.*/
      ],
      groups_for_modules: [
        "Client API": [
          Aerospike,
          Aerospike.Repo
        ],
        "Data Types": [
          Aerospike.Geo,
          Aerospike.Geo.Circle,
          Aerospike.Geo.Point,
          Aerospike.Geo.Polygon,
          Aerospike.Key,
          Aerospike.Record,
          Aerospike.Error,
          Aerospike.Page,
          Aerospike.Cursor
        ],
        "Scan & Query": [
          Aerospike.PartitionFilter,
          Aerospike.Query,
          Aerospike.Scan
        ],
        "Expressions & filters": [
          Aerospike.Exp,
          Aerospike.Filter
        ],
        "Operations & CDT": [
          Aerospike.Ctx,
          Aerospike.Op,
          Aerospike.Op.Bit,
          Aerospike.Op.Exp,
          Aerospike.Op.HLL,
          Aerospike.Op.List,
          Aerospike.Op.Map
        ],
        Batch: [
          Aerospike.Batch,
          Aerospike.Batch.Read,
          Aerospike.Batch.Put,
          Aerospike.Batch.Delete,
          Aerospike.Batch.Operate,
          Aerospike.Batch.UDF,
          Aerospike.BatchResult
        ],
        Transactions: [
          Aerospike.Txn
        ],
        "Async Tasks": [
          Aerospike.AsyncTask,
          Aerospike.IndexTask,
          Aerospike.RegisterTask
        ]
      ],
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"],
      before_closing_head_tag: &before_closing_head_tag/1
    ]
  end

  defp before_closing_head_tag(:html) do
    """
    <style>
      .content-inner { max-width: 900px; }
    </style>
    """
  end

  defp before_closing_head_tag(_), do: ""
end
