defmodule Aerospike.ConnectionTlsTest do
  use ExUnit.Case, async: true

  alias Aerospike.Connection
  alias Aerospike.Protocol.Info
  alias Aerospike.Test.MockTcpServer
  alias Aerospike.Test.MockTlsServer

  setup_all do
    server_conf = MockTlsServer.generate_test_certs()
    %{server_conf: server_conf}
  end

  describe "TLS connect" do
    test "with IP host returns :ssl transport", %{server_conf: server_conf} do
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
                 tls_opts: [verify: :verify_none]
               )

      assert {:ssl, _} = conn.transport
      Connection.close(conn)
      Task.await(server)
    end

    test "with hostname triggers SNI code path", %{server_conf: server_conf} do
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
                 tls_opts: [verify: :verify_none]
               )

      assert {:ssl, _} = conn.transport
      assert conn.host == "localhost"
      Connection.close(conn)
      Task.await(server)
    end

    test "preserves existing server_name_indication in tls_opts", %{server_conf: server_conf} do
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
                 tls_opts: [verify: :verify_none, server_name_indication: ~c"custom.example"]
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
    test "returns :ok for a TLS connection", %{server_conf: server_conf} do
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
          tls_opts: [verify: :verify_none]
        )

      assert :ok = Connection.close(conn)
      Task.await(server)
    end
  end

  describe "TLS transport_peername/1" do
    test "returns {:ok, {ip, port}} for TLS connection", %{server_conf: server_conf} do
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
          tls_opts: [verify: :verify_none]
        )

      assert {:ok, {_ip, _port}} = Connection.transport_peername(conn)
      Connection.close(conn)
      Task.await(server)
    end
  end

  describe "TLS request/recv_message" do
    test "request_info round-trip through TLS", %{server_conf: server_conf} do
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
          tls_opts: [verify: :verify_none]
        )

      assert {:ok, _conn2, %{"status" => "ok"}} = Connection.request_info(conn, ["status"])
      Connection.close(conn)
      Task.await(server)
    end

    test "handles zero-length body over TLS", %{server_conf: server_conf} do
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
          tls_opts: [verify: :verify_none]
        )

      data = Info.encode_request(["status"])
      assert {:ok, _conn2, _version, _type, <<>>} = Connection.request(conn, data)
      Connection.close(conn)
      Task.await(server)
    end
  end
end
