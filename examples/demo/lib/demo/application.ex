defmodule Demo.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    host = System.get_env("AEROSPIKE_HOST", "localhost:3000")

    children = [
      {Aerospike, name: :aero, hosts: [host], pool_size: 4}
    ]

    opts = [strategy: :one_for_one, name: Demo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
