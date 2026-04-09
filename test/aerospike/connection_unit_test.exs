defmodule Aerospike.ConnectionUnitTest do
  use ExUnit.Case, async: false

  alias Aerospike.Connection
  alias Aerospike.Protocol.AsmMsg
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

  describe "send_command/2 via mock server" do
    test "sends data without reading; server receives full message" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            assert {:ok, _header, body} = MockTcpServer.recv_message(client)
            assert body == <<1, 2, 3>>
          end)
        end)

      Process.sleep(50)
      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      req = Message.encode(Message.proto_version(), Message.type_info(), <<1, 2, 3>>)
      assert {:ok, conn2} = Connection.send_command(conn, req)
      refute Connection.idle?(conn2)
      Connection.close(conn2)
      Task.await(server)
    end

    test "returns error when client socket is already closed" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn _client -> :ok end)
        end)

      Process.sleep(50)
      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      Connection.close(conn)
      assert {:error, _} = Connection.send_command(conn, <<>>)
      Task.await(server)
    end
  end

  describe "recv_frame/1 via mock server" do
    test "reads one frame and sets last? from INFO3_LAST bit" do
      {:ok, lsock, port} = MockTcpServer.start()

      not_last_body = <<22, 0, 0, 0>>
      last_body = <<22, 0, 0, 1>>

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            {:ok, _, _} = MockTcpServer.recv_message(client)
            MockTcpServer.send_typed_response(client, 2, Message.type_as_msg(), not_last_body)
            MockTcpServer.send_typed_response(client, 2, Message.type_as_msg(), last_body)
          end)
        end)

      Process.sleep(50)
      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      req = Message.encode(Message.proto_version(), Message.type_info(), <<>>)

      assert {:ok, conn2} = Connection.send_command(conn, req)
      assert {:ok, conn3, body1, false} = Connection.recv_frame(conn2)
      assert body1 == not_last_body
      assert {:ok, conn4, body2, true} = Connection.recv_frame(conn3)
      assert body2 == last_body
      refute Connection.idle?(conn4)

      Connection.close(conn4)
      Task.await(server)
    end

    test "returns error when body recv fails (partial frame)" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            {:ok, _, _} = MockTcpServer.recv_message(client)
            header = Message.encode_header(2, Message.type_as_msg(), 1000)
            :gen_tcp.send(client, header)
            Process.sleep(10)
          end)
        end)

      Process.sleep(50)
      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      req = Message.encode(Message.proto_version(), Message.type_info(), <<>>)
      assert {:ok, conn2} = Connection.send_command(conn, req)
      assert {:error, :closed} = Connection.recv_frame(conn2)
      Connection.close(conn2)
      Task.await(server)
    end

    test "returns error when server closes before any frame" do
      {:ok, lsock, port} = MockTcpServer.start()

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            {:ok, _, _} = MockTcpServer.recv_message(client)
            :gen_tcp.close(client)
          end)
        end)

      Process.sleep(50)
      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      req = Message.encode(Message.proto_version(), Message.type_info(), <<>>)
      assert {:ok, conn2} = Connection.send_command(conn, req)
      assert {:error, _} = Connection.recv_frame(conn2)
      Connection.close(conn2)
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

  describe "recv_stream edge cases" do
    test "request_stream skips zero-length frames and keeps reading" do
      {:ok, lsock, port} = MockTcpServer.start()

      terminal_body =
        IO.iodata_to_binary(
          AsmMsg.encode(%AsmMsg{
            info3: AsmMsg.info3_last(),
            result_code: 0,
            fields: [],
            operations: []
          })
        )

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            {:ok, _, _} = MockTcpServer.recv_message(client)
            :ok = :gen_tcp.send(client, Message.encode_header(2, Message.type_as_msg(), 0))
            :ok = :gen_tcp.send(client, Message.encode(2, Message.type_as_msg(), terminal_body))
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      request = Message.encode(Message.proto_version(), Message.type_info(), <<0>>)

      assert {:ok, conn2, body} = Connection.request_stream(conn, request)
      assert body == terminal_body
      Connection.close(conn2)
      Task.await(server)
    end

    test "request_stream handles multiple frames sent in one tcp write" do
      {:ok, lsock, port} = MockTcpServer.start()

      body1 =
        IO.iodata_to_binary(AsmMsg.encode(%AsmMsg{result_code: 0, fields: [], operations: []}))

      body2 =
        IO.iodata_to_binary(
          AsmMsg.encode(%AsmMsg{
            info3: AsmMsg.info3_last(),
            result_code: 0,
            fields: [],
            operations: []
          })
        )

      frame1 = Message.encode(2, Message.type_as_msg(), body1)
      frame2 = Message.encode(2, Message.type_as_msg(), body2)

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            {:ok, _, _} = MockTcpServer.recv_message(client)
            :ok = :gen_tcp.send(client, frame1 <> frame2)
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      request = Message.encode(Message.proto_version(), Message.type_info(), <<1>>)

      assert {:ok, conn2, combined} = Connection.request_stream(conn, request)
      assert combined == body1 <> body2
      Connection.close(conn2)
      Task.await(server)
    end

    test "request_stream returns closed when server closes mid-frame" do
      {:ok, lsock, port} = MockTcpServer.start()
      partial = :binary.copy(<<0>>, 16)

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            {:ok, _, _} = MockTcpServer.recv_message(client)
            :ok = :gen_tcp.send(client, Message.encode_header(2, Message.type_as_msg(), 100))
            :ok = :gen_tcp.send(client, partial)
          end)
        end)

      {:ok, conn} = Connection.connect(host: "127.0.0.1", port: port, timeout: 2_000)
      request = Message.encode(Message.proto_version(), Message.type_info(), <<2>>)

      assert {:error, :closed} = Connection.request_stream(conn, request)
      Connection.close(conn)
      Task.await(server)
    end

    test "request_stream reads a 1MB frame body without crashing" do
      {:ok, lsock, port} = MockTcpServer.start()
      large_body = :binary.copy(<<170>>, 1_048_576)

      server =
        Task.async(fn ->
          MockTcpServer.accept_once(lsock, fn client ->
            {:ok, _, _} = MockTcpServer.recv_message(client)
            :ok = :gen_tcp.send(client, Message.encode(2, Message.type_as_msg(), large_body))
          end)
        end)

      {:ok, conn} =
        Connection.connect(host: "127.0.0.1", port: port, timeout: 5_000, recv_timeout: 5_000)

      request = Message.encode(Message.proto_version(), Message.type_info(), <<3>>)

      assert {:ok, conn2, body} = Connection.request_stream(conn, request)
      assert body == large_body
      Connection.close(conn2)
      Task.await(server)
    end
  end
end
