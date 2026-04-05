defmodule Aerospike.Test.MockTlsServer do
  @moduledoc false

  alias Aerospike.Protocol.Message

  @doc """
  Generates self-signed test certificate configs using OTP's built-in
  `pkix_test_data/1`. Returns `server_ssl_opts` — a keyword list suitable
  for `:ssl.handshake/3`.

  Uses RSA-2048 keys (EC keys have TLS 1.3 compatibility issues on some OTP versions).
  """
  def generate_test_certs do
    %{server_config: server_conf, client_config: _client_conf} =
      :public_key.pkix_test_data(%{
        server_chain: %{
          root: [key: {:rsa, 2048, 65_537}],
          peer: [key: {:rsa, 2048, 65_537}]
        },
        client_chain: %{
          root: [key: {:rsa, 2048, 65_537}],
          peer: [key: {:rsa, 2048, 65_537}]
        }
      })

    server_conf
  end

  @doc """
  Starts a TCP listener on a random port. Returns `{:ok, listen_socket, port}`.
  """
  def start do
    {:ok, lsock} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, true}])
    {:ok, {_, port}} = :inet.sockname(lsock)
    {:ok, lsock, port}
  end

  @doc """
  Accepts one TCP connection, upgrades it to TLS via `:ssl.handshake/3`,
  calls `handler.(ssl_socket)`, then tears down both sockets.
  """
  def accept_once_tls(lsock, server_ssl_opts, handler) do
    {:ok, tcp_client} = :gen_tcp.accept(lsock, 5_000)
    {:ok, ssl_client} = :ssl.handshake(tcp_client, server_ssl_opts, 5_000)
    handler.(ssl_client)
    _ = :ssl.close(ssl_client)
    :gen_tcp.close(lsock)
  end

  @doc """
  Reads one full Aerospike wire message from a TLS socket (8-byte header + body).
  """
  def recv_message(socket) do
    {:ok, header} = :ssl.recv(socket, 8, 5_000)
    {:ok, {_version, _type, length}} = Message.decode_header(header)

    body =
      if length > 0 do
        {:ok, b} = :ssl.recv(socket, length, 5_000)
        b
      else
        <<>>
      end

    {:ok, header, body}
  end

  @doc """
  Sends a framed Aerospike info response (type 1) over TLS.
  """
  def send_info_response(socket, body) do
    frame = Message.encode(Message.proto_version(), Message.type_info(), body)
    :ssl.send(socket, frame)
  end

  @doc """
  Sends a zero-length info response over TLS.
  """
  def send_empty_info_response(socket) do
    send_info_response(socket, <<>>)
  end
end
