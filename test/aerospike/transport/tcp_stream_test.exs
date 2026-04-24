defmodule Aerospike.Transport.TcpStreamTest do
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

  describe "stream_open/4 and stream_read/2" do
    test "delivers multiple frames, inflates compressed frames, and ends on the terminal marker",
         %{
           listener: listener,
           port: port
         } do
      first_frame = as_msg_frame(0, <<1, 2>>)
      second_frame = as_msg_frame(0, <<3>>)
      terminal_frame = as_msg_frame(0x01)
      compressed_first_frame = Message.encode_compressed_payload(first_frame)
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
      assert {:ok, stream} = Tcp.stream_open(conn, <<"req">>, 1_000, [])

      assert {:ok, ^first_frame} = Tcp.stream_read(stream, 500)
      assert {:ok, ^second_frame} = Tcp.stream_read(stream, 500)
      assert {:ok, ^terminal_frame} = Tcp.stream_read(stream, 500)
      assert :done = Tcp.stream_read(stream, 500)
      assert :ok = Tcp.stream_close(stream)

      wait_for(server)
    end

    test "returns a network error when the server closes before the terminal marker", %{
      listener: listener,
      port: port
    } do
      first_frame = as_msg_frame(0, <<9>>)

      server =
        spawn_server(listener, fn client ->
          {:ok, _} = :gen_tcp.recv(client, 0, 1_000)
          :ok = :gen_tcp.send(client, first_frame)
          :ok = :gen_tcp.close(client)
        end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)
      assert {:ok, stream} = Tcp.stream_open(conn, <<"req">>, 1_000, [])

      assert {:ok, ^first_frame} = Tcp.stream_read(stream, 500)
      assert {:error, %Error{code: :network_error}} = Tcp.stream_read(stream, 500)
      assert :ok = Tcp.stream_close(stream)

      wait_for(server)
    end
  end

  defp as_msg_frame(info3, payload \\ <<>>) do
    body = IO.iodata_to_binary(AsmMsg.encode(%AsmMsg{info3: info3})) <> payload
    Message.encode_as_msg(body)
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
