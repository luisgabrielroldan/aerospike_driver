defmodule Aerospike.Connection do
  @moduledoc false
  # A single TCP (optionally TLS) connection to one Aerospike node. Wraps
  # `:gen_tcp` and optionally `:ssl` with idle-timeout tracking and Aerospike
  # wire-protocol framing (8-byte header). Connections are not shared across
  # processes — they are owned by NimblePool workers.

  import Bitwise

  alias Aerospike.Protocol.Admin
  alias Aerospike.Protocol.Info
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.ResultCode
  alias Aerospike.Protocol.ScanResponse

  # Wire protocol message type for admin commands (login, create-user, etc.).
  @admin_message_type 2
  @type_info Message.type_info()

  @type transport :: {:gen_tcp | :ssl, :gen_tcp.socket() | :ssl.sslsocket()}

  defstruct [
    # Tagged `{module, socket}` — `:gen_tcp` or `:ssl` with the underlying socket.
    :transport,
    # Hostname or IP this connection was opened to.
    :host,
    # Port number this connection was opened to.
    :port,
    # Monotonic-ms deadline after which the pool considers this connection idle.
    :idle_deadline,
    # Duration in ms used to compute idle_deadline on each refresh.
    :idle_timeout,
    # Per-recv timeout in ms; passed to transport recv/3.
    :recv_timeout
  ]

  @type t :: %__MODULE__{
          transport: transport(),
          host: String.t(),
          port: :inet.port_number(),
          idle_deadline: integer(),
          idle_timeout: non_neg_integer(),
          recv_timeout: non_neg_integer()
        }

  @default_host "127.0.0.1"
  @default_port 3000
  @default_timeout 5_000
  @default_idle_timeout 55_000

  @doc """
  Opens a TCP connection to a single Aerospike node, optionally upgraded to TLS.

  ## Options

  - `:host` — hostname or IP (default `#{@default_host}`)
  - `:port` — port number (default `#{@default_port}`)
  - `:timeout` — connect, `send_timeout`, and TLS handshake timeout in ms (default `#{@default_timeout}`)
  - `:recv_timeout` — per-read timeout for responses (default: same as `:timeout`)
  - `:idle_timeout` — milliseconds until the connection is considered idle (default `#{@default_idle_timeout}`)
  - `:tls` — when `true`, runs `:ssl.connect/3` after TCP connect (default `false`)
  - `:tls_opts` — keyword list for `:ssl.connect/3` (default `[]`). When the host is not an IP address,
    `:server_name_indication` is set to the hostname unless already present in `:tls_opts`.

  """
  @spec connect(keyword()) :: {:ok, t()} | {:error, term()}
  def connect(opts \\ []) when is_list(opts) do
    host = Keyword.get(opts, :host, @default_host)
    port = Keyword.get(opts, :port, @default_port)
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    recv_timeout = Keyword.get(opts, :recv_timeout, timeout)
    idle_timeout = Keyword.get(opts, :idle_timeout, @default_idle_timeout)
    tls? = Keyword.get(opts, :tls, false)
    tls_opts = Keyword.get(opts, :tls_opts, [])

    host_ch = to_charlist(host)

    tcp_opts = [:binary, {:active, false}, {:packet, :raw}, {:send_timeout, timeout}]

    with {:ok, tcp_socket} <- :gen_tcp.connect(host_ch, port, tcp_opts, timeout),
         {:ok, mod, sock} <- maybe_tls_upgrade(tls?, tcp_socket, host, host_ch, tls_opts, timeout) do
      now = :erlang.monotonic_time(:millisecond)

      {:ok,
       %__MODULE__{
         transport: {mod, sock},
         host: host,
         port: port,
         idle_deadline: now + idle_timeout,
         idle_timeout: idle_timeout,
         recv_timeout: recv_timeout
       }}
    end
  end

  defp maybe_tls_upgrade(false, tcp_socket, _host, _host_ch, _tls_opts, _timeout) do
    {:ok, :gen_tcp, tcp_socket}
  end

  defp maybe_tls_upgrade(true, tcp_socket, _host, host_ch, tls_opts, timeout) do
    ssl_opts =
      case :inet.parse_address(host_ch) do
        {:ok, _} -> tls_opts
        {:error, _} -> Keyword.put_new(tls_opts, :server_name_indication, host_ch)
      end

    case :ssl.connect(tcp_socket, ssl_opts, timeout) do
      {:ok, ssl_sock} ->
        {:ok, :ssl, ssl_sock}

      {:error, _} = err ->
        _ = :gen_tcp.close(tcp_socket)
        err
    end
  end

  @doc """
  Closes the socket. Idempotent: closing an already-closed socket returns `:ok`.
  """
  @spec close(t() | :gen_tcp.socket() | nil) :: :ok
  def close(nil), do: :ok

  def close(%__MODULE__{transport: {mod, socket}}) do
    _ = mod.close(socket)
    :ok
  end

  def close(socket) when is_port(socket) do
    _ = :gen_tcp.close(socket)
    :ok
  end

  @doc """
  Returns `peername` for liveness checks — `:ssl.peername/1` when transport is TLS, else `:inet.peername/1`.
  """
  @spec transport_peername(t()) :: {:ok, term()} | {:error, term()}
  def transport_peername(%__MODULE__{transport: {:ssl, socket}}) do
    :ssl.peername(socket)
  end

  def transport_peername(%__MODULE__{transport: {:gen_tcp, socket}}) do
    :inet.peername(socket)
  end

  @doc """
  Sends a full wire message and reads one complete response (8-byte header + body).

  On success, refreshes the idle deadline. One in-flight request per connection.

  After an `{:error, _}` return, the connection should be closed — the socket state is undefined.
  """
  @spec request(t(), iodata()) ::
          {:ok, t(), non_neg_integer(), non_neg_integer(), binary()}
          | {:error, term()}
  def request(%__MODULE__{} = conn, data) do
    {mod, socket} = conn.transport

    case mod.send(socket, data) do
      :ok -> recv_message(conn)
      {:error, _} = err -> err
    end
  end

  @doc """
  Sends wire data to the server without reading a response.

  Used to initiate multi-frame commands (scan, query) where frames
  are read individually via `recv_frame/1`.
  """
  @spec send_command(t(), iodata()) :: {:ok, t()} | {:error, term()}
  def send_command(%__MODULE__{} = conn, data) do
    {mod, socket} = conn.transport

    case mod.send(socket, data) do
      :ok -> {:ok, refresh_idle(conn)}
      {:error, _} = err -> err
    end
  end

  @doc """
  Reads one complete message from the socket (header + body) and refreshes idle deadline.
  """
  @spec recv_message(t()) ::
          {:ok, t(), non_neg_integer(), non_neg_integer(), binary()}
          | {:error, term()}
  def recv_message(%__MODULE__{} = conn) do
    {mod, socket} = conn.transport

    with {:ok, header} <- mod.recv(socket, 8, conn.recv_timeout),
         {:ok, {version, type, length}} <- Message.decode_header(header),
         {:ok, body} <- recv_exact(mod, socket, length, conn.recv_timeout) do
      {:ok, refresh_idle(conn), version, type, body}
    end
  end

  defp recv_exact(_mod, _socket, 0, _timeout), do: {:ok, <<>>}

  defp recv_exact(mod, socket, len, timeout) when len > 0 do
    case mod.recv(socket, len, timeout) do
      {:ok, body} -> {:ok, body}
      {:error, _} = err -> err
    end
  end

  @doc """
  Reads one protocol frame from the connection.

  Returns `{:ok, conn, body, last?}` where `last?` is true when
  the frame carries the INFO3_LAST sentinel (end of scan/query stream).
  """
  @spec recv_frame(t()) :: {:ok, t(), binary(), boolean()} | {:error, term()}
  def recv_frame(%__MODULE__{} = conn) do
    {mod, socket} = conn.transport

    with {:ok, header} <- mod.recv(socket, 8, conn.recv_timeout),
         {:ok, {_version, _type, length}} <- Message.decode_header(header),
         {:ok, body} <- recv_exact(mod, socket, length, conn.recv_timeout) do
      {:ok, refresh_idle(conn), body, stream_last_frame?(body)}
    end
  end

  @doc """
  Sends wire data and reads a multi-frame response (batch/scan/query).

  The server sends one proto frame per record, terminated by a frame whose
  AS_MSG header has the INFO3_LAST bit set (byte 3, bit 0). All frame bodies
  are concatenated and returned as a single binary.
  """
  @spec request_stream(t(), iodata()) ::
          {:ok, t(), binary()} | {:error, term()}
  def request_stream(%__MODULE__{} = conn, data) do
    {mod, socket} = conn.transport

    case mod.send(socket, data) do
      :ok -> recv_stream(conn, mod, socket, [])
      {:error, _} = err -> err
    end
  end

  defp recv_stream(conn, mod, socket, acc) do
    with {:ok, header} <- mod.recv(socket, 8, conn.recv_timeout),
         {:ok, {_version, _type, length}} <- Message.decode_header(header),
         {:ok, body} <- recv_exact(mod, socket, length, conn.recv_timeout) do
      acc = [body | acc]

      if ScanResponse.lazy_stream_chunk_terminal?(body) do
        {:ok, refresh_idle(conn), IO.iodata_to_binary(Enum.reverse(acc))}
      else
        recv_stream(conn, mod, socket, acc)
      end
    end
  end

  # recv_frame/1 may see synthetic short bodies in tests; real scan AS_MSG chunks from
  # request_stream/2 are always large enough for ScanResponse.lazy_stream_chunk_terminal?/1.
  defp stream_last_frame?(body) when byte_size(body) < 22 do
    recv_frame_stream_terminal?(body)
  end

  defp stream_last_frame?(body) when is_binary(body) do
    ScanResponse.lazy_stream_chunk_terminal?(body)
  end

  @info3_last_bit 0x01

  defp recv_frame_stream_terminal?(body) when byte_size(body) < 4, do: true

  defp recv_frame_stream_terminal?(body) do
    <<_hdr::8, _i1::8, _i2::8, i3::8, _::binary>> = body
    last? = (i3 &&& @info3_last_bit) != 0

    if byte_size(body) >= 6 do
      <<_::32, _i4::8, rc::8, _::binary>> = body
      last? or rc != 0
    else
      last?
    end
  end

  @doc """
  Runs Aerospike INFO command(s) and returns a map of key-value strings.
  """
  @spec request_info(t(), [String.t()]) ::
          {:ok, t(), map()}
          | {:error, term()}
  def request_info(%__MODULE__{} = conn, commands) when is_list(commands) do
    data = Info.encode_request(commands)

    case request(conn, data) do
      {:ok, conn2, _version, type, body} -> decode_info_response(conn2, type, body)
      {:error, _} = err -> err
    end
  end

  defp decode_info_response(conn, @type_info, body) do
    {:ok, map} = Info.decode_response(body)
    {:ok, conn, map}
  end

  defp decode_info_response(_conn, _type, _body) do
    {:error, :unexpected_message_type}
  end

  @doc """
  Performs internal LOGIN when `:user` and `:credential` are set; otherwise returns the connection unchanged.

  Credential must be the bcrypt hash blob expected by the server (see `Aerospike.Protocol.Admin.encode_login/2`).
  """
  @spec login(t(), keyword()) :: {:ok, t()} | {:error, term()}
  def login(conn, opts \\ [])

  def login(%__MODULE__{} = conn, opts) when is_list(opts) do
    user = Keyword.get(opts, :user)
    cred = Keyword.get(opts, :credential)

    if user in [nil, ""] or cred in [nil, ""] do
      {:ok, conn}
    else
      cred_bin = if is_binary(cred), do: cred, else: to_string(cred)
      req = Admin.encode_login(user, cred_bin)

      case request(conn, req) do
        {:ok, conn2, _version, type, body} -> interpret_login(conn2, type, body)
        {:error, _} = err -> err
      end
    end
  end

  defp interpret_login(_conn, type, _body) when type != @admin_message_type do
    {:error, :unexpected_message_type}
  end

  defp interpret_login(conn, @admin_message_type, body) do
    case Admin.decode_admin_body(body) do
      {:ok, %{result_code: rc}} ->
        interpret_login_result(conn, rc)

      {:error, _} = err ->
        err
    end
  end

  # :ok means login succeeded; future phase will extract session_token here.
  defp interpret_login_result(conn, :ok) do
    {:ok, conn}
  end

  # Treat "security not enabled/supported" as success — the server has no auth.
  defp interpret_login_result(conn, rc)
       when rc in [:security_not_enabled, :security_not_supported] do
    {:ok, conn}
  end

  defp interpret_login_result(_conn, rc) do
    {:error, %{code: rc, message: ResultCode.message(rc)}}
  end

  @doc """
  Returns true if the idle deadline (monotonic ms) has passed.
  """
  @spec idle?(t()) :: boolean()
  def idle?(%__MODULE__{idle_deadline: d}) do
    :erlang.monotonic_time(:millisecond) > d
  end

  @doc """
  Sets the idle deadline to now + idle_timeout.
  """
  @spec refresh_idle(t()) :: t()
  def refresh_idle(%__MODULE__{idle_timeout: t} = conn) do
    now = :erlang.monotonic_time(:millisecond)
    %{conn | idle_deadline: now + t}
  end

  @doc false
  def admin_message_type, do: @admin_message_type
end
