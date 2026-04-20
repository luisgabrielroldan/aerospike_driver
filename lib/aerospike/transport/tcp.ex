defmodule Aerospike.Transport.Tcp do
  @moduledoc """
  Plaintext `:gen_tcp` implementation of `Aerospike.NodeTransport`.

  One socket per connection, no pooling, no TLS, no auth, no compression.
  Each call is a single request/response over a passive (`active: false`) socket
  with `:packet, :raw` framing. The Aerospike 8-byte protocol header determines
  body length, so framing is owned by this module — not by `:gen_tcp`.

  Failures are returned as `{:error, %Aerospike.Error{}}` — sockets are not
  reused after an error and the caller is expected to `close/1` them.
  """

  @behaviour Aerospike.NodeTransport

  alias Aerospike.Error
  alias Aerospike.Protocol.Info
  alias Aerospike.Protocol.Message

  @default_timeout 5_000
  @header_size 8
  @type_info Message.type_info()

  @typedoc "Concrete connection handle returned by `connect/3`."
  @opaque conn :: %__MODULE__{
            socket: :gen_tcp.socket(),
            recv_timeout: non_neg_integer()
          }

  @enforce_keys [:socket, :recv_timeout]
  defstruct [:socket, :recv_timeout]

  @impl true
  def connect(host, port, opts \\ []) when is_binary(host) and is_integer(port) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    recv_timeout = Keyword.get(opts, :recv_timeout, timeout)

    tcp_opts = [:binary, {:active, false}, {:packet, :raw}, {:send_timeout, timeout}]

    case :gen_tcp.connect(to_charlist(host), port, tcp_opts, timeout) do
      {:ok, socket} ->
        {:ok, %__MODULE__{socket: socket, recv_timeout: recv_timeout}}

      {:error, reason} ->
        {:error, connect_error(host, port, reason)}
    end
  end

  @impl true
  def close(%__MODULE__{socket: socket}) do
    _ = :gen_tcp.close(socket)
    :ok
  end

  @impl true
  def info(%__MODULE__{} = conn, commands) when is_list(commands) do
    request = Info.encode_request(commands)

    with {:ok, _version, @type_info, body} <- send_recv(conn, request),
         {:ok, map} <- Info.decode_response(body) do
      {:ok, map}
    else
      {:ok, _version, type, _body} ->
        {:error,
         %Error{
           code: :parse_error,
           message: "expected info reply (type #{@type_info}), got type #{type}"
         }}

      {:error, %Error{}} = err ->
        err
    end
  end

  @impl true
  def command(%__MODULE__{} = conn, request) do
    case send_recv(conn, request) do
      {:ok, _version, _type, body} -> {:ok, body}
      {:error, %Error{}} = err -> err
    end
  end

  defp send_recv(%__MODULE__{socket: socket} = conn, request) do
    case :gen_tcp.send(socket, request) do
      :ok -> recv_message(conn)
      {:error, reason} -> {:error, transport_error(:send, reason)}
    end
  end

  defp recv_message(%__MODULE__{socket: socket, recv_timeout: timeout}) do
    with {:ok, header} <- recv_exact(socket, @header_size, timeout),
         {:ok, {version, type, length}} <- decode_header(header),
         {:ok, body} <- recv_body(socket, length, timeout) do
      {:ok, version, type, body}
    end
  end

  defp recv_body(_socket, 0, _timeout), do: {:ok, <<>>}

  defp recv_body(socket, length, timeout) when length > 0 do
    recv_exact(socket, length, timeout)
  end

  defp recv_exact(socket, length, timeout) do
    case :gen_tcp.recv(socket, length, timeout) do
      {:ok, data} -> {:ok, data}
      {:error, reason} -> {:error, transport_error(:recv, reason)}
    end
  end

  defp decode_header(header) do
    case Message.decode_header(header) do
      {:ok, _} = ok ->
        ok

      {:error, :incomplete_header} ->
        {:error, %Error{code: :parse_error, message: "incomplete protocol header from server"}}
    end
  end

  defp connect_error(host, port, :timeout) do
    %Error{code: :timeout, message: "timed out connecting to #{host}:#{port}"}
  end

  defp connect_error(host, port, reason) do
    %Error{
      code: :connection_error,
      message: "failed to connect to #{host}:#{port}: #{format_reason(reason)}"
    }
  end

  defp transport_error(_op, :timeout) do
    %Error{code: :timeout, message: "transport timed out"}
  end

  defp transport_error(:send, :closed) do
    %Error{code: :network_error, message: "send failed: socket closed"}
  end

  defp transport_error(:recv, :closed) do
    %Error{code: :network_error, message: "recv failed: socket closed"}
  end

  defp transport_error(op, reason) do
    %Error{code: :network_error, message: "#{op} failed: #{format_reason(reason)}"}
  end

  defp format_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp format_reason(reason), do: inspect(reason)
end
