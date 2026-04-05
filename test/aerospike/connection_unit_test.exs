defmodule Aerospike.ConnectionUnitTest do
  use ExUnit.Case, async: false

  alias Aerospike.Connection
  alias Aerospike.Protocol.Info
  alias Aerospike.Protocol.Message
  alias Aerospike.Test.MockTcpServer

  describe "struct fields" do
    test "struct can be constructed with expected field types" do
      conn = %Connection{
        transport: {:gen_tcp, nil},
        host: "127.0.0.1",
        port: 3000,
        idle_deadline: 0,
        idle_timeout: 55_000,
        recv_timeout: 5_000
      }

      assert conn.idle_timeout == 55_000
      assert conn.recv_timeout == 5_000
      assert conn.host == "127.0.0.1"
      assert conn.port == 3000
      assert {:gen_tcp, nil} = conn.transport
    end
  end

  describe "idle?/1 and refresh_idle/1" do
    test "idle? is false immediately after refresh_idle" do
      conn = %Connection{
        transport: {:gen_tcp, nil},
        host: "h",
        port: 3000,
        idle_deadline: 0,
        idle_timeout: 60_000,
        recv_timeout: 5_000
      }

      c2 = Connection.refresh_idle(conn)
      refute Connection.idle?(c2)
    end

    test "idle? is true when deadline is in the past" do
      past = :erlang.monotonic_time(:millisecond) - 1

      conn = %Connection{
        transport: {:gen_tcp, nil},
        host: "h",
        port: 3000,
        idle_deadline: past,
        idle_timeout: 1,
        recv_timeout: 5_000
      }

      assert Connection.idle?(conn)
    end

    test "refresh_idle resets deadline for a 1 ms idle window" do
      conn = %Connection{
        transport: {:gen_tcp, nil},
        host: "h",
        port: 3000,
        idle_deadline: 0,
        idle_timeout: 1,
        recv_timeout: 5_000
      }

      c2 = Connection.refresh_idle(conn)
      refute Connection.idle?(c2)
      Process.sleep(2)
      assert Connection.idle?(c2)
    end
  end

  describe "close/1" do
    test "close(nil) returns :ok" do
      assert :ok = Connection.close(nil)
    end

    test "close on a raw :gen_tcp socket port returns :ok" do
      {:ok, socket} = :gen_tcp.listen(0, [:binary])
      assert :ok = Connection.close(socket)
    end
  end

  describe "connect/1 transport" do
    test "without tls uses :gen_tcp transport tuple" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn _client -> :ok end)
        end)

      assert {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      assert {:gen_tcp, sock} = conn.transport
      assert is_port(sock)
      Connection.close(conn)
      Task.await(server)
    end
  end

  describe "admin_message_type/0" do
    test "returns the admin message type constant" do
      assert Connection.admin_message_type() == 2
    end
  end

  describe "request_info/2 via mock server" do
    test "returns {:error, :unexpected_message_type} for non-info response" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            MockTcpServer.recv_message(client)
            MockTcpServer.send_typed_response(client, 2, 3, <<>>)
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      assert {:error, :unexpected_message_type} = Connection.request_info(conn, ["node"])
      Connection.close(conn)
      Task.await(server)
    end

    test "propagates network error on closed socket" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            :gen_tcp.close(client)
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      Process.sleep(50)
      assert {:error, _} = Connection.request_info(conn, ["node"])
      Connection.close(conn)
      Task.await(server)
    end
  end

  describe "login/2 via mock server" do
    test "returns :unexpected_message_type when server sends non-admin type" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            MockTcpServer.recv_message(client)
            MockTcpServer.send_typed_response(client, 2, 99, <<>>)
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)

      assert {:error, :unexpected_message_type} =
               Connection.login(conn, user: "u", credential: "cred")

      Connection.close(conn)
      Task.await(server)
    end

    test "returns error for non-ok result code" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            MockTcpServer.recv_message(client)
            body = <<0, 65, 0, 0>> <> :binary.copy(<<0>>, 12)
            MockTcpServer.send_admin_response(client, body)
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)

      assert {:error, %{code: :invalid_credential}} =
               Connection.login(conn, user: "u", credential: "c")

      Connection.close(conn)
      Task.await(server)
    end

    test "returns ok for admin :ok result code" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            MockTcpServer.recv_message(client)
            body = :binary.copy(<<0>>, 16)
            MockTcpServer.send_admin_response(client, body)
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      assert {:ok, _} = Connection.login(conn, user: "u", credential: "c")
      Connection.close(conn)
      Task.await(server)
    end

    test "propagates network error on login request" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            :gen_tcp.close(client)
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      Process.sleep(50)
      assert {:error, _} = Connection.login(conn, user: "u", credential: "c")
      Connection.close(conn)
      Task.await(server)
    end
  end

  describe "recv_message/1 via mock server" do
    test "handles zero-length body" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            MockTcpServer.recv_message(client)
            MockTcpServer.send_empty_info_response(client)
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      data = Info.encode_request(["status"])

      assert {:ok, conn2, _version, _type, body} = Connection.request(conn, data)
      assert body == <<>>
      refute Connection.idle?(conn2)

      Connection.close(conn)
      Task.await(server)
    end

    test "returns error when body recv fails (partial response)" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            MockTcpServer.recv_message(client)
            header = Message.encode_header(2, 1, 1000)
            :gen_tcp.send(client, header)
            Process.sleep(10)
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      data = Info.encode_request(["status"])

      assert {:error, :closed} = Connection.request(conn, data)
      Connection.close(conn)
      Task.await(server)
    end
  end

  describe "login/2 with malformed admin body" do
    test "returns error when admin body decode fails (short body)" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            MockTcpServer.recv_message(client)
            MockTcpServer.send_admin_response(client, <<1>>)
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      assert {:error, :short_body} = Connection.login(conn, user: "u", credential: "c")
      Connection.close(conn)
      Task.await(server)
    end
  end
end
