defmodule Aerospike.MixProject do
  use Mix.Project

  @version "0.1.0"
  @description "Aerospike Elixir spike focused on architecture and operator-surface validation"

  def project do
    [
      app: :aerospike_spike,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: @description,
      package: package(),
      docs: docs(),
      preferred_cli_env: [docs: :dev],
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
      name: "aerospike_spike",
      files: ~w(lib priv .formatter.exs mix.exs mix.lock README.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"]
    ]
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
