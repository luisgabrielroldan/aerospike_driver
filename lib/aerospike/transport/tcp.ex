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
  requests, which this transport does not support.

  The read deadline is supplied per `command/3` call by the caller rather
  than stored on the connection, so a retry layer can budget each attempt
  independently (see `Aerospike.NodeTransport.command/3`). `info/2` still
  uses the default connect-time timeout because it is only issued from the
  Tender path, which has no per-call deadline of its own.

  Failures are returned as `{:error, %Aerospike.Error{}}` — sockets are not
  reused after an error and the caller is expected to `close/1` them.

  ## `connect/3` options

  Every option is a key in the `opts` keyword list. Unknown keys are
  ignored so the same keyword list can be shared across transport
  implementations.

    * `:connect_timeout_ms` — milliseconds to wait for the TCP handshake
      and for the `:gen_tcp.send/2` write buffer to drain. Defaults to
      `#{5_000}` ms.
    * `:info_timeout` — read deadline applied to every `info/2` call.
      Defaults to `:connect_timeout_ms` so a caller that sets one value
      gets consistent behaviour across connect and info probes.
    * `:tcp_nodelay` — boolean. When `true` (default), the socket is
      opened with `{:nodelay, true}` so small info probes are not
      delayed by Nagle. Set `false` to let the kernel coalesce writes.
    * `:tcp_keepalive` — boolean. When `true` (default), the socket is
      opened with `{:keepalive, true}` so the kernel probes a half-open
      peer independently of the driver's tend loop.
    * `:tcp_sndbuf` — positive integer. When set, translates to
      `{:sndbuf, n}`. Left unset (default `nil`) the kernel picks its
      own send-buffer size.
    * `:tcp_rcvbuf` — positive integer. When set, translates to
      `{:recbuf, n}` (the `:gen_tcp` spelling of `SO_RCVBUF`). Left
      unset (default `nil`) the kernel picks its own receive-buffer
      size.
    * `:node_name` — opaque label stashed on the returned connection
      handle and attached to every telemetry event emitted for that
      handle. `nil` (default) when the caller does not know the node
      name yet — e.g. seed bootstrap and peer-discovery probes open
      sockets before the `node` info key has been read.
    * `:user` / `:password` — internal-auth session login credentials.
      When both are present, `connect/3` runs the admin-protocol login
      handshake immediately after the TCP handshake and returns the
      authenticated socket. On a server with security disabled the
      login result code `SECURITY_NOT_ENABLED` (52) is treated as a
      successful no-op and the socket is returned as usual. Any other
      non-zero result closes the socket and surfaces as an
      `%Aerospike.Error{}`.
    * `:session_token` — opaque session token issued by an earlier
      login. When present, `connect/3` sends an `AUTHENTICATE` command
      on the fresh socket instead of the full password handshake.
      `:user` must be present alongside the token. The transport
      returns `{:error, %Aerospike.Error{code: :expired_session}}` when
      the server rejects the token; the caller is expected to retry
      with `:user`/`:password` to acquire a fresh token.
    * `:login_timeout_ms` — read deadline applied to the login reply.
      Defaults to `:connect_timeout_ms`.

  See `:inet.setopts/2` for the underlying semantics. Opt translation
  happens once in `connect/3`; callers pass the public names above.

  ## Telemetry

  Every `command/4` emits a `[:aerospike, :command, :send]` span around
  the socket write and a `[:aerospike, :command, :recv]` span around
  the header + body read. Every `info/2` emits a
  `[:aerospike, :info, :rpc]` span around the full round trip. Metadata
  keys follow `Aerospike.Telemetry`'s taxonomy (`:node_name`,
  `:attempt`, `:deadline_ms` for commands; `:node_name`, `:commands`
  for info). `command/4` callers that do not pass an `:attempt` key
  (every retry-driver call does) default to `0` in metadata.
  """

  @behaviour Aerospike.NodeTransport

  alias Aerospike.Error
  alias Aerospike.Protocol.Info
  alias Aerospike.Protocol.Login
  alias Aerospike.Protocol.Message
  alias Aerospike.Telemetry

  @default_connect_timeout_ms 5_000
  @header_size 8
  # Outbound requests below this size are sent uncompressed even when the
  # caller sets `use_compression: true`. Matches Go
  # `command.go:_COMPRESS_THRESHOLD` and Java `Command.COMPRESS_THRESHOLD`.
  @compress_threshold 128
  @proto_version Message.proto_version()
  @type_info Message.type_info()
  @type_as_msg Message.type_as_msg()
  @type_compressed Message.type_compressed()
  @login_header_size Login.reply_header_size()

  @typedoc "Concrete connection handle returned by `connect/3`."
  @opaque conn :: %__MODULE__{
            socket: :gen_tcp.socket(),
            info_timeout: non_neg_integer(),
            node_name: String.t() | nil
          }

  @enforce_keys [:socket, :info_timeout]
  defstruct [:socket, :info_timeout, node_name: nil]

  @impl true
  def connect(host, port, opts \\ []) when is_binary(host) and is_integer(port) do
    connect_timeout_ms = Keyword.get(opts, :connect_timeout_ms, @default_connect_timeout_ms)
    info_timeout = Keyword.get(opts, :info_timeout, connect_timeout_ms)
    login_timeout_ms = Keyword.get(opts, :login_timeout_ms, connect_timeout_ms)
    node_name = Keyword.get(opts, :node_name)

    tcp_opts = build_tcp_opts(opts, connect_timeout_ms)

    case :gen_tcp.connect(to_charlist(host), port, tcp_opts, connect_timeout_ms) do
      {:ok, socket} ->
        conn = %__MODULE__{socket: socket, info_timeout: info_timeout, node_name: node_name}
        maybe_login(conn, opts, login_timeout_ms, host, port)

      {:error, reason} ->
        {:error, connect_error(host, port, reason)}
    end
  end

  # Runs the admin-protocol login or authenticate handshake on a freshly
  # connected socket. Selection order:
  #
  #   1. `:session_token` present → send AUTHENTICATE (cheap, no bcrypt).
  #      On `:expired_session` the caller is expected to retry with
  #      password creds; the socket is closed so callers cannot reuse it.
  #   2. `:user` + `:password` present → send LOGIN (internal auth).
  #      Returns `{:ok, conn}` regardless of whether the server produced a
  #      session token; callers that need the token must call
  #      `login/2` on an unauthenticated connection instead.
  #   3. Neither set → return the connection untouched (plaintext, no auth).
  #
  # `SECURITY_NOT_ENABLED` (code 52) is treated as success at every entry
  # point: the socket is usable, the server simply had security disabled.
  # Any other non-zero result closes the socket and surfaces as a typed
  # `%Aerospike.Error{}`.
  defp maybe_login(conn, opts, timeout_ms, host, port) do
    user = Keyword.get(opts, :user)
    password = Keyword.get(opts, :password)
    session_token = Keyword.get(opts, :session_token)

    cond do
      is_binary(user) and is_binary(session_token) ->
        authenticate_session_with_fallback(
          conn,
          user,
          password,
          session_token,
          timeout_ms,
          host,
          port
        )

      is_binary(user) and is_binary(password) ->
        login_internal(conn, user, password, timeout_ms, host, port)

      true ->
        {:ok, conn}
    end
  end

  # Runs AUTHENTICATE first; if the server rejects the session token with
  # `:expired_session` and the caller also supplied `:password`, reopens
  # the handshake on the same socket with a full password login so the
  # pool worker recovers from an expired cached token without bouncing
  # the socket. On any other failure the socket is closed and the error
  # surfaces to the caller.
  defp authenticate_session_with_fallback(conn, user, password, token, timeout_ms, host, port) do
    frame = Login.encode_authenticate(user, token)

    case run_login_rpc(conn, frame, timeout_ms) do
      {:ok, _reply} ->
        {:ok, conn}

      {:error, %Error{code: :expired_session}} when is_binary(password) ->
        login_internal(conn, user, password, timeout_ms, host, port)

      {:error, %Error{} = err} ->
        close_and_fail(conn, err, host, port)
    end
  end

  defp login_internal(conn, user, password, timeout_ms, host, port) do
    hashed = Login.hash_password(password)
    frame = Login.encode_login_internal(user, hashed)

    case run_login_rpc(conn, frame, timeout_ms) do
      {:ok, :ok_no_token} ->
        {:ok, conn}

      {:ok, :security_not_enabled} ->
        {:ok, conn}

      {:ok, {:session, _token, _ttl}} ->
        {:ok, conn}

      {:error, %Error{} = err} ->
        close_and_fail(conn, err, host, port)
    end
  end

  # Public entry point so callers that want the raw login reply (the
  # Tender, which caches the session token per node) can reach it
  # without going through `connect/3`'s default swallow-the-token
  # behaviour. Runs on an already-connected socket; the caller owns
  # the socket and is expected to close it on error.
  @impl true
  @spec login(conn(), keyword()) ::
          {:ok, Login.login_reply()} | {:error, Error.t()}
  def login(%__MODULE__{} = conn, opts) when is_list(opts) do
    timeout_ms = Keyword.get(opts, :login_timeout_ms, conn.info_timeout)

    frame =
      case Keyword.get(opts, :session_token) do
        nil ->
          user = Keyword.fetch!(opts, :user)
          password = Keyword.fetch!(opts, :password)
          Login.encode_login_internal(user, Login.hash_password(password))

        token when is_binary(token) ->
          user = Keyword.fetch!(opts, :user)
          Login.encode_authenticate(user, token)
      end

    run_login_rpc(conn, frame, timeout_ms)
  end

  # Sends the login/authenticate frame, reads the 24-byte reply header and
  # the trailing field block, and translates the decoded reply into either
  # a `Login.login_reply` or an `%Aerospike.Error{}`. Wrapped in a
  # `[:aerospike, :info, :rpc, :*]` span so the login RPC shows up in the
  # same event stream as ordinary info probes.
  defp run_login_rpc(%__MODULE__{node_name: node_name} = conn, frame, timeout_ms) do
    :telemetry.span(
      Telemetry.info_rpc_span(),
      %{node_name: node_name, commands: [:login]},
      fn ->
        result = do_run_login_rpc(conn, frame, timeout_ms)
        {result, %{node_name: node_name, commands: [:login]}}
      end
    )
  end

  defp do_run_login_rpc(conn, frame, timeout_ms) do
    with :ok <- send_login(conn, frame),
         {:ok, header} <- recv_exact(conn.socket, @login_header_size, timeout_ms),
         {:ok, {result_code, field_count, body_length}} <- decode_login_header(header),
         {:ok, body} <- recv_login_body(conn.socket, body_length, timeout_ms) do
      interpret_login_reply(result_code, field_count, body)
    end
  end

  defp send_login(%__MODULE__{socket: socket}, frame) do
    case :gen_tcp.send(socket, frame) do
      :ok -> :ok
      {:error, reason} -> {:error, transport_error(:send, reason)}
    end
  end

  defp recv_login_body(_socket, 0, _timeout), do: {:ok, <<>>}

  defp recv_login_body(socket, length, timeout) when length > 0 do
    recv_exact(socket, length, timeout)
  end

  defp decode_login_header(header) do
    case Login.decode_reply_header(header) do
      {:ok, result_code, field_count, body_length} ->
        {:ok, {result_code, field_count, body_length}}

      {:error, :incomplete_header} ->
        {:error,
         %Error{code: :parse_error, message: "incomplete admin-protocol header from server"}}

      {:error, {:wrong_version, version}} ->
        {:error,
         %Error{
           code: :parse_error,
           message: "unexpected admin proto version from server: got #{version}"
         }}

      {:error, {:wrong_type, type}} ->
        {:error,
         %Error{
           code: :parse_error,
           message: "unexpected admin proto type from server: got #{type}"
         }}
    end
  end

  defp interpret_login_reply(:ok, field_count, body) do
    case Login.decode_login_fields(body, field_count) do
      {:ok, reply} ->
        {:ok, reply}

      {:error, :parse_error} ->
        {:error, %Error{code: :parse_error, message: "malformed admin-protocol login fields"}}
    end
  end

  defp interpret_login_reply(:security_not_enabled, _field_count, _body),
    do: {:ok, :security_not_enabled}

  defp interpret_login_reply(code, _field_count, _body) when is_atom(code) do
    {:error, Error.from_result_code(code)}
  end

  defp interpret_login_reply(code, _field_count, _body) when is_integer(code) do
    {:error,
     %Error{
       code: :server_error,
       message: "login failed with unknown result code #{code}"
     }}
  end

  defp close_and_fail(%__MODULE__{socket: socket}, %Error{} = err, _host, _port) do
    _ = :gen_tcp.close(socket)
    {:error, err}
  end

  # Builds the `:gen_tcp.connect/4` opt list. The first four entries are
  # fixed framing (`:binary`, passive, raw, send-timeout tied to the
  # connect deadline) that every Aerospike connection needs. Remaining
  # entries are the optional tuning knobs documented in the moduledoc,
  # translated here from public names to the `:inet.setopts/2` spellings
  # in exactly one place.
  defp build_tcp_opts(opts, connect_timeout_ms) do
    base = [:binary, {:active, false}, {:packet, :raw}, {:send_timeout, connect_timeout_ms}]

    base
    |> maybe_put_bool(opts, :tcp_nodelay, :nodelay, true)
    |> maybe_put_bool(opts, :tcp_keepalive, :keepalive, true)
    |> maybe_put_size(opts, :tcp_sndbuf, :sndbuf)
    |> maybe_put_size(opts, :tcp_rcvbuf, :recbuf)
  end

  defp maybe_put_bool(acc, opts, public_key, inet_key, default) do
    case Keyword.get(opts, public_key, default) do
      value when is_boolean(value) -> acc ++ [{inet_key, value}]
      other -> raise ArgumentError, bool_error(public_key, other)
    end
  end

  defp maybe_put_size(acc, opts, public_key, inet_key) do
    case Keyword.get(opts, public_key) do
      nil ->
        acc

      value when is_integer(value) and value > 0 ->
        acc ++ [{inet_key, value}]

      other ->
        raise ArgumentError, size_error(public_key, other)
    end
  end

  defp bool_error(key, value) do
    "Aerospike.Transport.Tcp: #{inspect(key)} must be a boolean, got #{inspect(value)}"
  end

  defp size_error(key, value) do
    "Aerospike.Transport.Tcp: #{inspect(key)} must be a positive integer or nil, " <>
      "got #{inspect(value)}"
  end

  @impl true
  def close(%__MODULE__{socket: socket}) do
    _ = :gen_tcp.close(socket)
    :ok
  end

  @impl true
  def info(%__MODULE__{info_timeout: timeout, node_name: node_name} = conn, commands)
      when is_list(commands) do
    :telemetry.span(
      Telemetry.info_rpc_span(),
      %{node_name: node_name, commands: commands},
      fn ->
        request = Info.encode_request(commands)

        result =
          with {:ok, version, type, body} <- send_recv(conn, request, timeout),
               :ok <- validate_version(version),
               :ok <- validate_type(type, @type_info) do
            Info.decode_response(body)
          end

        {result, %{node_name: node_name, commands: commands}}
      end
    )
  end

  @impl true
  def command(%__MODULE__{node_name: node_name} = conn, request, deadline_ms, opts \\ [])
      when is_integer(deadline_ms) and deadline_ms >= 0 and is_list(opts) do
    framed = maybe_compress(request, opts)
    attempt = Keyword.get(opts, :attempt, 0)

    span_metadata = %{node_name: node_name, attempt: attempt, deadline_ms: deadline_ms}

    with :ok <- send_framed(conn, framed, span_metadata),
         {:ok, version, type, body} <- recv_framed(conn, deadline_ms, span_metadata),
         :ok <- validate_version(version),
         :ok <- validate_command_type(type) do
      maybe_decompress(type, body)
    end
  end

  # Wraps the socket write in a `[:aerospike, :command, :send]` span so
  # the caller's handler sees every send attempt (including transport
  # failures via the span's `:exception` event).
  defp send_framed(conn, framed, metadata) do
    :telemetry.span(Telemetry.command_send_span(), metadata, fn ->
      result =
        case :gen_tcp.send(conn.socket, framed) do
          :ok -> :ok
          {:error, reason} -> {:error, transport_error(:send, reason)}
        end

      {result, metadata}
    end)
  end

  # Wraps the two-recv reassembly in a `[:aerospike, :command, :recv]`
  # span. The stop metadata carries `:bytes` so a handler can track
  # payload size alongside latency without re-decoding the frame.
  defp recv_framed(conn, deadline_ms, metadata) do
    :telemetry.span(Telemetry.command_recv_span(), metadata, fn ->
      result = recv_message(conn, deadline_ms)
      stop_metadata = Map.put(metadata, :bytes, recv_bytes(result))
      {result, stop_metadata}
    end)
  end

  defp recv_bytes({:ok, _version, _type, body}), do: byte_size(body)
  defp recv_bytes(_), do: 0

  # When `:use_compression` is set and the request is large enough for the
  # reference-client threshold, compress it into a type-4 envelope. If the
  # resulting envelope is not smaller than the original frame, fall back to
  # the plain iodata — matching Java `Command.compress`'s
  # `if (def.finished())` guard. The `:zlib` writer can produce output
  # larger than its input for tiny payloads, so the threshold alone does
  # not guarantee a win.
  defp maybe_compress(request, opts) do
    case Keyword.get(opts, :use_compression, false) do
      true -> compress_if_worthwhile(request, IO.iodata_length(request))
      false -> request
    end
  end

  defp compress_if_worthwhile(request, uncompressed_size)
       when uncompressed_size <= @compress_threshold,
       do: request

  defp compress_if_worthwhile(request, uncompressed_size) do
    uncompressed_frame = IO.iodata_to_binary(request)
    compressed = Message.encode_compressed_payload(uncompressed_frame)

    if byte_size(compressed) < uncompressed_size do
      compressed
    else
      request
    end
  end

  defp validate_version(@proto_version), do: :ok

  defp validate_version(version) do
    {:error,
     %Error{
       code: :parse_error,
       message: "unexpected proto version from server: expected #{@proto_version}, got #{version}"
     }}
  end

  defp validate_type(type, type), do: :ok
  defp validate_type(type, expected), do: type_mismatch_error(type, [expected])

  defp validate_command_type(@type_as_msg), do: :ok
  defp validate_command_type(@type_compressed), do: :ok

  defp validate_command_type(type),
    do: type_mismatch_error(type, [@type_as_msg, @type_compressed])

  # For a plain AS_MSG reply the body is returned verbatim. For a
  # compressed reply (type 4, `AS_MSG_COMPRESSED`) we peel the 8-byte
  # uncompressed-size prefix, inflate the remaining bytes, verify the
  # inflated frame matches that prefix, and re-parse its own 8-byte proto
  # header — the inner frame must be a plain AS_MSG (type 3, version 2).
  # Layout reference: Go `command.go:3574-3627`,
  # `multi_command.go:150-173` (see `notes.md` Finding 3).
  defp maybe_decompress(@type_as_msg, body), do: {:ok, body}

  defp maybe_decompress(@type_compressed, body) do
    with {:ok, {uncompressed_size, compressed}} <- decode_compressed_payload(body),
         {:ok, inflated} <- safe_uncompress(compressed),
         :ok <- validate_uncompressed_size(inflated, uncompressed_size),
         {:ok, {inner_version, inner_type, inner_body}} <- decode_inner_frame(inflated),
         :ok <- validate_version(inner_version),
         :ok <- validate_inner_type(inner_type) do
      {:ok, inner_body}
    end
  end

  defp decode_compressed_payload(body) do
    case Message.decode_compressed_payload(body) do
      {:ok, _} = ok ->
        ok

      {:error, :incomplete_compressed_payload} ->
        {:error,
         %Error{
           code: :parse_error,
           message: "compressed reply is missing its 8-byte uncompressed-size prefix"
         }}
    end
  end

  defp safe_uncompress(compressed) do
    {:ok, :zlib.uncompress(compressed)}
  rescue
    e in ErlangError ->
      {:error,
       %Error{
         code: :parse_error,
         message: "failed to inflate compressed reply: #{inspect(e.original)}"
       }}
  end

  defp validate_uncompressed_size(inflated, expected_size) do
    actual = byte_size(inflated)

    if actual == expected_size do
      :ok
    else
      {:error,
       %Error{
         code: :parse_error,
         message:
           "compressed reply size mismatch: header advertised #{expected_size}, inflated #{actual}"
       }}
    end
  end

  defp decode_inner_frame(inflated) do
    case Message.decode(inflated) do
      {:ok, _} = ok ->
        ok

      {:error, :incomplete_header} ->
        {:error,
         %Error{
           code: :parse_error,
           message: "inflated compressed reply has an incomplete proto header"
         }}

      {:error, :incomplete_body} ->
        {:error,
         %Error{
           code: :parse_error,
           message: "inflated compressed reply has a truncated body"
         }}
    end
  end

  defp validate_inner_type(@type_as_msg), do: :ok
  defp validate_inner_type(type), do: type_mismatch_error(type, [@type_as_msg])

  defp type_mismatch_error(type, expected) do
    {:error,
     %Error{
       code: :parse_error,
       message: "unexpected proto type from server: expected #{inspect(expected)}, got #{type}"
     }}
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
