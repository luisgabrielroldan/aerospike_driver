defmodule AeroMarketLiveWeb.Router do
  use AeroMarketLiveWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {AeroMarketLiveWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", AeroMarketLiveWeb do
    pipe_through :browser

    live "/", DashboardLive
  end

  # Other scopes may use custom stacks.
  # scope "/api", AeroMarketLiveWeb do
  #   pipe_through :api
  # end
end
