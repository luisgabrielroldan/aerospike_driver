# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

config :aero_market_live,
  generators: [timestamp_type: :utc_datetime]

config :aero_market_live, AeroMarketLive.Aero,
  transport: Aerospike.Transport.Tcp,
  hosts: ["localhost:3000"],
  namespaces: ["test"],
  pool_size: 2

# Configures the endpoint
config :aero_market_live, AeroMarketLiveWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: AeroMarketLiveWeb.ErrorHTML, json: AeroMarketLiveWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: AeroMarketLive.PubSub,
  live_view: [signing_salt: "Vckawi51"]

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
