defmodule Aerospike.MixProject do
  use Mix.Project

  @version "0.1.1"
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
      {:nimble_options, "~> 1.0"},
      {:nimble_pool, "~> 1.0"},
      {:telemetry, "~> 1.0"},
      # Dev/test only
      {:stream_data, "~> 1.0", only: [:dev, :test]},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp aliases do
    [
      coveralls: "coveralls --include integration",
      "coveralls.detail": "coveralls.detail --include integration",
      "coveralls.html": "coveralls.html --include integration",
      "coveralls.json": "coveralls.json --include integration",
      "coveralls.lcov": "coveralls.lcov --include integration",
      "test.coverage": "coveralls.html --include integration",
      "test.all":
        "test --include integration --include property --include cluster --include enterprise",
      "test.enterprise": "test --include enterprise"
    ]
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
        "Data Types": [
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
