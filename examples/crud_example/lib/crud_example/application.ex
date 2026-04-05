defmodule CrudExample.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Aerospike, name: :aero, hosts: ["localhost:3000"], pool_size: 4}
    ]

    opts = [strategy: :one_for_one, name: CrudExample.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
