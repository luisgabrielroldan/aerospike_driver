defmodule Demo.Examples.TlsSecureConnection do
  @moduledoc """
  Demonstrates TLS-secured connections to Aerospike.

  Defaults:
  - host: `localhost:4333`
  - TLS verify: `:verify_peer` when fixture CA exists, otherwise `:verify_none`

  Optional override:
  - `AEROSPIKE_TLS_HOST` to point at a different TLS endpoint.
  """

  require Logger

  alias Aerospike.Transport.Tls

  def run do
    {host, port} = tls_host_port()

    with {:ok, conn} <- Tls.connect(host, port, default_connect_opts()),
         {:ok, info_map} <- Tls.info(conn, ["status", "build"]) do
      _ = Tls.close(conn)
      Logger.info("  TLS connected to #{host}:#{port}")
      Logger.info("    status=#{Map.get(info_map, "status")} build=#{Map.get(info_map, "build")}")
      :ok
    else
      {:error, _reason} ->
        Logger.warning(
          "  TlsSecureConnection: skipped — TLS endpoint not reachable on localhost:4333"
        )

        :skipped
    end
  end

  defp tls_host_port do
    host_port = System.get_env("AEROSPIKE_TLS_HOST", "localhost:4333")

    case String.split(host_port, ":", parts: 2) do
      [host, port_s] ->
        {port, _rest} = Integer.parse(port_s)
        {host, port}

      [host] ->
        {host, 4333}
    end
  end

  defp default_connect_opts do
    ca_path = Path.join(fixtures_dir(), "ca.crt")

    if File.exists?(ca_path) do
      [connect_timeout_ms: 5_000, tls_name: "localhost", tls_cacertfile: ca_path]
    else
      [connect_timeout_ms: 5_000, tls_name: "localhost", tls_verify: :verify_none]
    end
  end

  defp fixtures_dir do
    Path.expand("../../test/support/fixtures/tls", File.cwd!())
  end
end
