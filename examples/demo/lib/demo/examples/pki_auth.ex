defmodule Demo.Examples.PkiAuth do
  @moduledoc """
  Demonstrates certificate-authenticated TLS (mTLS) against localhost.

  Defaults:
  - host: `localhost:4334` (mTLS service)
  - certs: `../../test/support/fixtures/tls`
  """

  require Logger

  alias Aerospike.Transport.Tls

  def run do
    host = "localhost"
    port = 4334

    if fixtures_present?() do
      with {:ok, conn} <- Tls.connect(host, port, connect_opts()),
           {:ok, info_map} <- Tls.info(conn, ["status"]) do
        _ = Tls.close(conn)
        Logger.info("  PKI/mTLS connected to #{host}:#{port}")
        Logger.info("    status=#{Map.get(info_map, "status")}")
        :ok
      else
        {:error, _reason} ->
          Logger.warning("  PkiAuth: skipped — mTLS endpoint not reachable on localhost:4334")
          :skipped
      end
    else
      Logger.warning("  PkiAuth: skipped — missing TLS fixtures (run make tls-fixtures)")
      :skipped
    end
  end

  defp connect_opts do
    dir = fixtures_dir()

    [
      connect_timeout_ms: 5_000,
      tls_name: "localhost",
      tls_cacertfile: Path.join(dir, "ca.crt"),
      tls_certfile: Path.join(dir, "client.crt"),
      tls_keyfile: Path.join(dir, "client.key")
    ]
  end

  defp fixtures_present? do
    dir = fixtures_dir()

    File.exists?(Path.join(dir, "ca.crt")) and
      File.exists?(Path.join(dir, "client.crt")) and
      File.exists?(Path.join(dir, "client.key"))
  end

  defp fixtures_dir do
    Path.expand("../../test/support/fixtures/tls", File.cwd!())
  end
end
