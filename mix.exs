defmodule Aerospike.MixProject do
  use Mix.Project

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
    "test/integration/write_family_test.exs"
  ]
  @cluster_integration_files [
    "test/integration/batch_get_test.exs",
    "test/integration/scan_test.exs"
  ]
  @enterprise_integration_files [
    "test/integration/auth_test.exs",
    "test/integration/operator_surface_smoke_test.exs",
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
      test_coverage: [summary: [threshold: 85]],
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
      docs: :dev,
      "test.unit": :test,
      "test.coverage": :test,
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
      "test.unit": "test --seed 0",
      "test.coverage": "test --cover --seed 0",
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
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
