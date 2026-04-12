defmodule Aerospike.Test.MockTcpServer do
  @moduledoc false

  alias Aerospike.Protocol.Message

  @default_timeout 60_000

  @doc """
  Starts a TCP listener on a random port. Returns `{:ok, listen_socket, port}`.

  The caller must spawn an acceptor via `accept_once/2` or `accept_loop/2`.
  """
  def start do
    {:ok, lsock} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, true}])
    {:ok, {_, port}} = :inet.sockname(lsock)
    {:ok, lsock, port}
  end

  @doc """
  Accepts one connection and calls `handler.(client_socket)`, then closes everything.
  """
  def accept_once(lsock, handler) do
    {:ok, client} = :gen_tcp.accept(lsock, @default_timeout)
    handler.(client)
    :gen_tcp.close(client)
    :gen_tcp.close(lsock)
  end

  @doc """
  Reads one full Aerospike wire message from the socket (8-byte header + body).
  """
  def recv_message(socket) do
    {:ok, header} = :gen_tcp.recv(socket, 8, @default_timeout)
    {:ok, {_version, _type, length}} = Message.decode_header(header)

    body =
      if length > 0 do
        {:ok, b} = :gen_tcp.recv(socket, length, @default_timeout)
        b
      else
        <<>>
      end

    {:ok, header, body}
  end

  @doc """
  Sends a framed Aerospike info response (type 1) with the given body.
  """
  def send_info_response(socket, body) do
    frame = Message.encode(Message.proto_version(), Message.type_info(), body)
    :gen_tcp.send(socket, frame)
  end

  @doc """
  Sends a framed Aerospike admin response (type 2) with the given body.
  """
  def send_admin_response(socket, body) do
    frame = Message.encode(2, 2, body)
    :gen_tcp.send(socket, frame)
  end

  @doc """
  Sends a framed response with a custom type.
  """
  def send_typed_response(socket, version, type, body) do
    frame = Message.encode(version, type, body)
    :gen_tcp.send(socket, frame)
  end

  @doc """
  Sends a zero-length info response (header only, empty body).
  """
  def send_empty_info_response(socket) do
    send_info_response(socket, <<>>)
  end
end
