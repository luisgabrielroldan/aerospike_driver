defmodule Aerospike.Transport.TcpCommandStreamTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Transport.Tcp

  setup do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])
    {:ok, port} = :inet.port(listener)

    on_exit(fn ->
      _ = :gen_tcp.close(listener)
    end)

    %{listener: listener, port: port}
  end

  describe "command_stream/4" do
    test "collects all frame bodies through the terminal marker", %{
      listener: listener,
      port: port
    } do
      first_body = as_msg_body(0, <<1, 2>>)
      second_body = as_msg_body(0, <<3>>)
      terminal_body = as_msg_body(AsmMsg.info3_last())

      compressed_first_frame =
        Message.encode_compressed_payload(Message.encode_as_msg(first_body))

      second_frame = Message.encode_as_msg(second_body)
      terminal_frame = Message.encode_as_msg(terminal_body)
      <<part1::binary-size(5), part2::binary>> = compressed_first_frame

      server =
        spawn_server(listener, fn client ->
          {:ok, _} = :gen_tcp.recv(client, 0, 1_000)
          :ok = :gen_tcp.send(client, part1)
          Process.sleep(20)
          :ok = :gen_tcp.send(client, part2)
          :ok = :gen_tcp.send(client, second_frame)
          :ok = :gen_tcp.send(client, terminal_frame)
          :ok = :gen_tcp.close(client)
        end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:ok, body} = Tcp.command_stream(conn, <<"req">>, 500, [])
      assert body == IO.iodata_to_binary([first_body, second_body, terminal_body])

      :ok = Tcp.close(conn)
      wait_for(server)
    end

    test "returns a network error when the server closes before the terminal marker", %{
      listener: listener,
      port: port
    } do
      first_body = as_msg_body(0, <<9>>)
      first_frame = Message.encode_as_msg(first_body)

      server =
        spawn_server(listener, fn client ->
          {:ok, _} = :gen_tcp.recv(client, 0, 1_000)
          :ok = :gen_tcp.send(client, first_frame)
          :ok = :gen_tcp.close(client)
        end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:error, %Error{code: :network_error}} = Tcp.command_stream(conn, <<"req">>, 500, [])

      :ok = Tcp.close(conn)
      wait_for(server)
    end

    test "treats an admin error frame as terminal without waiting for query_end", %{
      listener: listener,
      port: port
    } do
      invalid_role_frame = Message.encode(2, 2, <<0, 74, 16, 0, 0::96>>)

      server =
        spawn_server(listener, fn client ->
          {:ok, _} = :gen_tcp.recv(client, 0, 1_000)
          :ok = :gen_tcp.send(client, invalid_role_frame)
          :ok = :gen_tcp.close(client)
        end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:ok, ^invalid_role_frame} =
               Tcp.command_stream(conn, <<"req">>, 500, message_type: :admin)

      :ok = Tcp.close(conn)
      wait_for(server)
    end
  end

  defp as_msg_body(info3, payload \\ <<>>) do
    IO.iodata_to_binary(AsmMsg.encode(%AsmMsg{info3: info3})) <> payload
  end

  defp spawn_server(listener, handler) do
    parent = self()

    spawn_link(fn ->
      case :gen_tcp.accept(listener, 2_000) do
        {:ok, client} ->
          send(parent, {:stream_server, self()})
          handler.(client)

        {:error, _} ->
          :ok
      end
    end)
  end

  defp wait_for(pid) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    after
      1_000 -> :ok
    end
  end
end
