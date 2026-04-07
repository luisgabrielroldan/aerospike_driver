defmodule Aerospike.ConnectionTlsTest do
  use ExUnit.Case, async: true

  alias Aerospike.Connection
  alias Aerospike.Protocol.Info
  alias Aerospike.Test.MockTcpServer
  alias Aerospike.Test.MockTlsServer
  alias Aerospike.Test.TlsFixtures

  setup_all do
    case TlsFixtures.load() do
      {:ok, %{server_ssl_opts: server_conf, client_ssl_opts: client_tls_opts}} ->
        %{server_conf: server_conf, client_tls_opts: client_tls_opts}

      {:error, :fixtures_missing} ->
        server_conf = MockTlsServer.generate_test_certs()
        %{server_conf: server_conf, client_tls_opts: [verify: :verify_none]}
    end
  end

  describe "TLS connect" do
    test "with IP host returns :ssl transport", %{
      server_conf: server_conf,
      client_tls_opts: client_tls_opts
    } do
      {:ok, lsock, port} = MockTlsServer.start()

      server =
        Task.async(fn ->
          MockTlsServer.accept_once_tls(lsock, server_conf, fn _ssl -> :ok end)
        end)

      assert {:ok, conn} =
               Connection.connect(
                 host: "127.0.0.1",
                 port: port,
                 timeout: 5_000,
                 tls: true,
                 tls_opts: client_tls_opts
               )

      assert {:ssl, _} = conn.transport
      Connection.close(conn)
      Task.await(server)
    end

    test "with hostname triggers SNI code path", %{
      server_conf: server_conf,
      client_tls_opts: client_tls_opts
    } do
      {:ok, lsock, port} = MockTlsServer.start()

      server =
        Task.async(fn ->
          MockTlsServer.accept_once_tls(lsock, server_conf, fn _ssl -> :ok end)
        end)

      assert {:ok, conn} =
               Connection.connect(
                 host: "localhost",
                 port: port,
                 timeout: 5_000,
                 tls: true,
                 tls_opts: client_tls_opts
               )

      assert {:ssl, _} = conn.transport
      assert conn.host == "localhost"
      Connection.close(conn)
      Task.await(server)
    end

    test "preserves existing server_name_indication in tls_opts", %{
      server_conf: server_conf,
      client_tls_opts: client_tls_opts
    } do
      {:ok, lsock, port} = MockTlsServer.start()

      server =
        Task.async(fn ->
          MockTlsServer.accept_once_tls(lsock, server_conf, fn _ssl -> :ok end)
        end)

      tls_opts =
        client_tls_opts
        |> Keyword.put(:verify, :verify_none)
        |> Keyword.put(:server_name_indication, ~c"custom.example")

      assert {:ok, conn} =
               Connection.connect(
                 host: "localhost",
                 port: port,
                 timeout: 5_000,
                 tls: true,
                 tls_opts: tls_opts
               )

      assert {:ssl, _} = conn.transport
      Connection.close(conn)
      Task.await(server)
    end

    @tag :capture_log
    test "failure returns error and cleans up TCP socket" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            :gen_tcp.send(client, "NOT A TLS HANDSHAKE\n")
          end)
        end)

      assert {:error, _} =
               Connection.connect(
                 host: "127.0.0.1",
                 port: port,
                 timeout: 2_000,
                 tls: true,
                 tls_opts: [verify: :verify_none]
               )

      Task.await(server)
    end
  end

  describe "TLS close/1" do
    test "returns :ok for a TLS connection", %{
      server_conf: server_conf,
      client_tls_opts: client_tls_opts
    } do
      {:ok, lsock, port} = MockTlsServer.start()

      server =
        Task.async(fn ->
          MockTlsServer.accept_once_tls(lsock, server_conf, fn _ssl ->
            Process.sleep(200)
          end)
        end)

      {:ok, conn} =
        Connection.connect(
          host: "127.0.0.1",
          port: port,
          timeout: 5_000,
          tls: true,
          tls_opts: client_tls_opts
        )

      assert :ok = Connection.close(conn)
      Task.await(server)
    end
  end

  describe "TLS transport_peername/1" do
    test "returns {:ok, {ip, port}} for TLS connection", %{
      server_conf: server_conf,
      client_tls_opts: client_tls_opts
    } do
      {:ok, lsock, port} = MockTlsServer.start()

      server =
        Task.async(fn ->
          MockTlsServer.accept_once_tls(lsock, server_conf, fn _ssl ->
            Process.sleep(200)
          end)
        end)

      {:ok, conn} =
        Connection.connect(
          host: "127.0.0.1",
          port: port,
          timeout: 5_000,
          tls: true,
          tls_opts: client_tls_opts
        )

      assert {:ok, {_ip, _port}} = Connection.transport_peername(conn)
      Connection.close(conn)
      Task.await(server)
    end
  end

  describe "TLS request/recv_message" do
    test "request_info round-trip through TLS", %{
      server_conf: server_conf,
      client_tls_opts: client_tls_opts
    } do
      {:ok, lsock, port} = MockTlsServer.start()

      server =
        Task.async(fn ->
          MockTlsServer.accept_once_tls(lsock, server_conf, fn ssl_client ->
            MockTlsServer.recv_message(ssl_client)
            MockTlsServer.send_info_response(ssl_client, "status\tok\n")
          end)
        end)

      {:ok, conn} =
        Connection.connect(
          host: "127.0.0.1",
          port: port,
          timeout: 5_000,
          tls: true,
          tls_opts: client_tls_opts
        )

      assert {:ok, _conn2, %{"status" => "ok"}} = Connection.request_info(conn, ["status"])
      Connection.close(conn)
      Task.await(server)
    end

    test "handles zero-length body over TLS", %{
      server_conf: server_conf,
      client_tls_opts: client_tls_opts
    } do
      {:ok, lsock, port} = MockTlsServer.start()

      server =
        Task.async(fn ->
          MockTlsServer.accept_once_tls(lsock, server_conf, fn ssl_client ->
            MockTlsServer.recv_message(ssl_client)
            MockTlsServer.send_empty_info_response(ssl_client)
          end)
        end)

      {:ok, conn} =
        Connection.connect(
          host: "127.0.0.1",
          port: port,
          timeout: 5_000,
          tls: true,
          tls_opts: client_tls_opts
        )

      data = Info.encode_request(["status"])
      assert {:ok, _conn2, _version, _type, <<>>} = Connection.request(conn, data)
      Connection.close(conn)
      Task.await(server)
    end
  end

  describe "mTLS with client certificate" do
    test "connect succeeds when server requires peer cert from fixtures" do
      if mtls_fixtures_present?() do
        dir = mtls_fixtures_dir()

        server_opts = [
          certfile: to_charlist(Path.join(dir, "server.crt")),
          keyfile: to_charlist(Path.join(dir, "server.key")),
          cacertfile: to_charlist(Path.join(dir, "ca.crt")),
          verify: :verify_peer,
          fail_if_no_peer_cert: true
        ]

        client_opts = [
          verify: :verify_none,
          certfile: to_charlist(Path.join(dir, "client.crt")),
          keyfile: to_charlist(Path.join(dir, "client.key"))
        ]

        {:ok, lsock, port} = MockTlsServer.start()

        server =
          Task.async(fn ->
            MockTlsServer.accept_once_tls(lsock, server_opts, fn _ssl ->
              :ok
            end)
          end)

        assert {:ok, conn} =
                 Connection.connect(
                   host: "127.0.0.1",
                   port: port,
                   timeout: 5_000,
                   tls: true,
                   tls_opts: client_opts
                 )

        assert {:ssl, _} = conn.transport
        Connection.close(conn)
        Task.await(server)
      end
    end
  end

  defp mtls_fixtures_present? do
    dir = mtls_fixtures_dir()

    Enum.all?(
      ~w(ca.crt server.crt server.key client.crt client.key),
      &File.exists?(Path.join(dir, &1))
    )
  end

  defp mtls_fixtures_dir do
    Path.expand("../support/fixtures/tls", __DIR__)
  end
end
