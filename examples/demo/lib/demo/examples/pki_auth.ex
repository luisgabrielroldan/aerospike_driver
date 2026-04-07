defmodule Demo.Examples.PkiAuth do
  @moduledoc """
  Demonstrates certificate-authenticated TLS (mTLS) against localhost.

  Defaults:
  - host: `localhost:4334` (mTLS service)
  - certs: `../../test/support/fixtures/tls`
  """

  require Logger

  def run do
    host = "localhost"
    port = 4334

    tls_opts = [
      verify: :verify_peer,
      cacertfile: to_charlist(Path.join(fixtures_dir(), "ca.crt")),
      certfile: to_charlist(Path.join(fixtures_dir(), "client.crt")),
      keyfile: to_charlist(Path.join(fixtures_dir(), "client.key"))
    ]

    if fixtures_present?() do
      with {:ok, conn} <-
             Aerospike.Connection.connect(
               host: host,
               port: port,
               timeout: 5_000,
               tls: true,
               tls_opts: tls_opts
             ),
           {:ok, _conn2, info_map} <- Aerospike.Connection.request_info(conn, ["status"]) do
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
