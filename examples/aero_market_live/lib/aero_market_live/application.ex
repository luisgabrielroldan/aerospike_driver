defmodule AeroMarketLive.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      AeroMarketLiveWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:aero_market_live, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: AeroMarketLive.PubSub},
      AeroMarketLive.Aero,
      {AeroMarketLive.Generator, []},
      AeroMarketLiveWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: AeroMarketLive.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    AeroMarketLiveWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
