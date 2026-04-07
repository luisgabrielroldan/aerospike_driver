defmodule Aerospike.Integration.TlsPkiParityTest do
  use ExUnit.Case, async: false

  alias Aerospike.Tables

  @moduletag :tls_stack

  @namespace "test"
  @set "tls_parity_itest"

  test "TLS secure connection can request info on demo TLS endpoint" do
    ensure_endpoint_reachable!("127.0.0.1", 4333, "TLS endpoint (run make demo-stack-up)")

    assert {:ok, conn} =
             Aerospike.Connection.connect(
               host: "localhost",
               port: 4333,
               timeout: 5_000,
               tls: true,
               tls_opts: default_tls_opts()
             )

    assert {:ok, _conn2, info_map} = Aerospike.Connection.request_info(conn, ["status", "build"])
    assert is_binary(info_map["status"])
    assert is_binary(info_map["build"])

    :ok = Aerospike.Connection.close(conn)
  end

  test "PKI/mTLS connection can request info on demo mTLS endpoint" do
    ensure_endpoint_reachable!("127.0.0.1", 4334, "mTLS endpoint (run make demo-stack-up)")
    assert :ok = ensure_client_fixtures_present()

    assert {:ok, conn} =
             Aerospike.Connection.connect(
               host: "localhost",
               port: 4334,
               timeout: 5_000,
               tls: true,
               tls_opts: mtls_client_opts()
             )

    assert {:ok, _conn2, info_map} = Aerospike.Connection.request_info(conn, ["status"])
    assert is_binary(info_map["status"])

    :ok = Aerospike.Connection.close(conn)
  end

  test "Aerospike.start_link over TLS establishes client connectivity" do
    ensure_endpoint_reachable!("127.0.0.1", 4333, "TLS endpoint (run make demo-stack-up)")

    conn = :"tls_parity_#{System.unique_integer([:positive])}"

    opts = [
      name: conn,
      hosts: ["localhost:4333"],
      tls: true,
      tls_opts: default_tls_opts(),
      pool_size: 1,
      connect_timeout: 5_000,
      tend_interval: 1_000
    ]

    assert {:ok, _sup} = start_supervised({Aerospike, opts})
    Process.sleep(1_500)

    key = Aerospike.key(@namespace, @set, "tls_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      if :ets.whereis(Tables.meta(conn)) != :undefined do
        _ = Aerospike.delete(conn, key)
        _ = Aerospike.close(conn)
      end
    end)

    case Aerospike.put(conn, key, %{"v" => 1}) do
      :ok ->
        assert {:ok, rec} = Aerospike.get(conn, key)
        assert rec.bins["v"] == 1
        assert {:ok, true} = Aerospike.delete(conn, key)

      {:error, %Aerospike.Error{code: :invalid_cluster_partition_map}} ->
        assert {:ok, build} = Aerospike.info(conn, "build")
        assert is_binary(build)
    end
  end

  defp ensure_endpoint_reachable!(host, port, hint) do
    case :gen_tcp.connect(String.to_charlist(host), port, [], 1_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      _ ->
        flunk("Endpoint #{host}:#{port} is not reachable: #{hint}")
    end
  end

  defp default_tls_opts do
    ca_path = Path.join(fixtures_dir(), "ca.crt")

    if File.exists?(ca_path) do
      [verify: :verify_peer, cacertfile: to_charlist(ca_path)]
    else
      [verify: :verify_none]
    end
  end

  defp mtls_client_opts do
    dir = fixtures_dir()

    [
      verify: :verify_peer,
      cacertfile: to_charlist(Path.join(dir, "ca.crt")),
      certfile: to_charlist(Path.join(dir, "client.crt")),
      keyfile: to_charlist(Path.join(dir, "client.key"))
    ]
  end

  defp ensure_client_fixtures_present do
    dir = fixtures_dir()

    required = ["ca.crt", "client.crt", "client.key"]

    if Enum.all?(required, &File.exists?(Path.join(dir, &1))) do
      :ok
    else
      flunk("Missing TLS fixtures in #{dir}. Run make tls-fixtures")
    end
  end

  defp fixtures_dir do
    Path.expand("support/fixtures/tls", File.cwd!() <> "/test")
  end
end
