defmodule AeroMarketLive.MixProject do
  use Mix.Project

  def project do
    [
      app: :aero_market_live,
      version: "0.1.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {AeroMarketLive.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      {:phoenix, "~> 1.7.21"},
      {:phoenix_html, "~> 4.1"},
      {:phoenix_live_reload, "~> 1.2", only: :dev},
      {:phoenix_live_view, "~> 1.0"},
      {:floki, ">= 0.30.0", only: :test},
      {:telemetry_metrics, "~> 1.0"},
      {:telemetry_poller, "~> 1.0"},
      {:jason, "~> 1.2"},
      {:dns_cluster, "~> 0.1.1"},
      {:bandit, "~> 1.5"},
      {:aerospike_driver, path: "../../"}
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to install project dependencies and perform other setup tasks, run:
  #
  #     $ mix setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      setup: ["deps.get", "assets.setup"],
      "assets.setup": &vendor_liveview_js/1
    ]
  end

  defp vendor_liveview_js(_) do
    dest = Path.join(["priv", "static", "assets"])
    File.mkdir_p!(dest)

    sources = [
      {"deps/phoenix/priv/static/phoenix.js", "phoenix.js"},
      {"deps/phoenix_live_view/priv/static/phoenix_live_view.js", "phoenix_live_view.js"}
    ]

    Enum.each(sources, fn {src, name} ->
      unless File.exists?(src) do
        Mix.raise("assets.setup: missing #{src} — run `mix deps.get` first")
      end

      File.cp!(src, Path.join(dest, name))
    end)

    :ok
  end
end
