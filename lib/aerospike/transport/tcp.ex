defmodule Aerospike.Transport.Tcp do
  @moduledoc """
  Plaintext `:gen_tcp` implementation of `Aerospike.NodeTransport`.

  One socket per connection, no pooling, no TLS, no auth, no compression.
  Each call is a single request/response over a passive (`active: false`) socket
  with `:packet, :raw` framing. The Aerospike 8-byte protocol header determines
  body length, so framing is owned by this module — not by `:gen_tcp`.

  Reads use `:gen_tcp.recv(socket, N, timeout)` with `N > 0`, which blocks
  inside the VM until exactly `N` bytes arrive, so server-side TCP
  fragmentation is transparent: the header and body are read in two exact
  recv calls regardless of how the kernel delivers them. Coalescing the two
  reads is not possible because the body length lives in the header, and a
  per-connection read buffer would only pay off for pipelined in-flight
  requests, which Tier 1.5 explicitly defers (see
  `docs/plans/tier-1-5-pool-hardening/notes.md`, Finding 7).

  The read deadline is supplied per `command/3` call by the caller rather
  than stored on the connection, so a retry layer can budget each attempt
  independently (see `Aerospike.NodeTransport.command/3`). `info/2` still
  uses the default connect-time timeout because it is only issued from the
  Tender path, which has no per-call deadline of its own.

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
            info_timeout: non_neg_integer()
          }

  @enforce_keys [:socket, :info_timeout]
  defstruct [:socket, :info_timeout]

  @impl true
  def connect(host, port, opts \\ []) when is_binary(host) and is_integer(port) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    info_timeout = Keyword.get(opts, :info_timeout, timeout)

    tcp_opts = [:binary, {:active, false}, {:packet, :raw}, {:send_timeout, timeout}]

    case :gen_tcp.connect(to_charlist(host), port, tcp_opts, timeout) do
      {:ok, socket} ->
        {:ok, %__MODULE__{socket: socket, info_timeout: info_timeout}}

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
  def info(%__MODULE__{info_timeout: timeout} = conn, commands) when is_list(commands) do
    request = Info.encode_request(commands)

    with {:ok, _version, @type_info, body} <- send_recv(conn, request, timeout),
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
  def command(%__MODULE__{} = conn, request, deadline_ms)
      when is_integer(deadline_ms) and deadline_ms >= 0 do
    case send_recv(conn, request, deadline_ms) do
      {:ok, _version, _type, body} -> {:ok, body}
      {:error, %Error{}} = err -> err
    end
  end

  defp send_recv(%__MODULE__{socket: socket} = conn, request, deadline_ms) do
    case :gen_tcp.send(socket, request) do
      :ok -> recv_message(conn, deadline_ms)
      {:error, reason} -> {:error, transport_error(:send, reason)}
    end
  end

  # Two-recv framing: the 8-byte Aerospike header carries the body length,
  # so we `recv_exact/3` the header, decode it, then `recv_exact/3` the
  # body. `recv_exact/3` wraps `:gen_tcp.recv(socket, N, deadline)` which
  # on a passive `{:packet, :raw}` socket blocks until exactly `N` bytes
  # arrive — server-side TCP fragmentation is invisible to the caller. See
  # the moduledoc for why we do not coalesce or buffer further.
  defp recv_message(%__MODULE__{socket: socket}, deadline_ms) do
    with {:ok, header} <- recv_exact(socket, @header_size, deadline_ms),
         {:ok, {version, type, length}} <- decode_header(header),
         {:ok, body} <- recv_body(socket, length, deadline_ms) do
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
