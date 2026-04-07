defmodule Aerospike.Test.TlsFixtures do
  @moduledoc false

  @required_files ~w(ca.crt server.crt server.key client.crt client.key)

  def load do
    dir = fixtures_dir()

    if Enum.all?(@required_files, &File.exists?(Path.join(dir, &1))) do
      {:ok,
       %{
         server_ssl_opts: server_ssl_opts(dir),
         client_ssl_opts: client_ssl_opts(dir)
       }}
    else
      {:error, :fixtures_missing}
    end
  end

  defp fixtures_dir do
    System.get_env(
      "AEROSPIKE_TLS_FIXTURES_DIR",
      Path.expand("fixtures/tls", __DIR__)
    )
  end

  defp server_ssl_opts(dir) do
    [
      certfile: to_charlist(Path.join(dir, "server.crt")),
      keyfile: to_charlist(Path.join(dir, "server.key")),
      verify: :verify_none
    ]
  end

  defp client_ssl_opts(dir) do
    [
      verify: :verify_peer,
      cacertfile: to_charlist(Path.join(dir, "ca.crt"))
    ]
  end
end
