defmodule Demo.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    host = default_host()

    children = [
      {Aerospike, name: :aero, hosts: [host], pool_size: 4}
    ]

    opts = [strategy: :one_for_one, name: Demo.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp default_host do
    if tcp_open?("127.0.0.1", 3100) do
      "localhost:3100"
    else
      "localhost:3000"
    end
  end

  defp tcp_open?(host, port) do
    case :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false], 250) do
      {:ok, socket} ->
        :ok = :gen_tcp.close(socket)
        true

      _ ->
        false
    end
  end
end
