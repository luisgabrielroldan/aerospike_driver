defmodule Aerospike.NodeTransport do
  @moduledoc """
  Behaviour for per-node I/O, isolating cluster logic from transport details.

  The Tender, Router, and command modules call this behaviour — they must
  never reach `:gen_tcp` directly. The same contract is served by a real
  TCP implementation in production and a scripted fake in tests, so every
  cluster-logic path is exercised deterministically without sockets.

  This behaviour is intentionally narrow: `info/2`, unary `command/4`,
  optional `login/2`, and optional stream open/read/close callbacks are the
  execution shapes supported today. Streaming replies and fan-out
  orchestration are separate contracts, not hidden variants of
  `command/4`.

  Implementations are free to choose their own `conn` representation
  (a socket, a pid, a reference, a struct). The opaque type prevents
  transport internals from leaking into callers.
  """

  @typedoc "Opaque connection handle returned by `c:connect/3`."
  @type conn :: term()

  @typedoc """
  Options accepted by `c:connect/3`. Implementations decide which keys they
  honour; the only key callers are expected to pass is `:timeout`
  (milliseconds, default left to the implementation).
  """
  @type connect_opts :: keyword()

  @doc """
  Opens a connection to `host:port`.

  On success, returns an opaque connection handle suitable for passing to
  the other callbacks. On failure, returns an `Aerospike.Error` tagged
  with a transport-appropriate code (e.g. `:connection_error`, `:timeout`).
  """
  @callback connect(host :: String.t(), port :: :inet.port_number(), opts :: connect_opts()) ::
              {:ok, conn()} | {:error, Aerospike.Error.t()}

  @doc """
  Closes the connection. Must be idempotent — calling `close/1` on an
  already-closed handle returns `:ok`.
  """
  @callback close(conn()) :: :ok

  @doc """
  Issues one info request containing `commands` and returns the parsed
  key/value map from the server's reply.

  The single-round-trip shape matches the Aerospike info protocol: all
  commands ship in one request, all results come back in one response.
  """
  @callback info(conn(), commands :: [String.t()]) ::
              {:ok, %{String.t() => String.t()}} | {:error, Aerospike.Error.t()}

  @typedoc """
  Options accepted by `c:command/4`.

    * `:use_compression` — when `true`, requests whose encoded size exceeds
      a fixed 128-byte threshold are wrapped in a type-4
      (`AS_MSG_COMPRESSED`) proto frame before being sent. Smaller requests
      are sent plain even when the flag is set, matching the Go and Java
      clients (`_COMPRESS_THRESHOLD = 128`). Implementations that ignore
      compression (e.g. `Aerospike.Transport.Fake`) must still accept the
      option without error. Defaults to `false`.
  """
  @type command_opts :: [use_compression: boolean()]

  @doc """
  Sends a pre-encoded AS_MSG request and returns the full reply bytes.

  The request is expected to be complete wire bytes (proto header + body)
  produced by `Aerospike.Protocol.Message` / `Aerospike.Protocol.AsmMsg`.
  The reply is the full response payload; framing/parsing is the caller's
  responsibility.

  `deadline_ms` is a per-socket-read deadline in milliseconds applied to
  each `:gen_tcp.recv/3` call (header and body are read separately on
  passive `{:packet, :raw}` sockets; see `Aerospike.Transport.Tcp`). It is
  deliberately separate from the caller's total-operation budget: a slow
  node can blow its read deadline without the caller having to track a
  monotonic deadline manually. The caller remains responsible for the
  overall operation budget — the transport does not enforce it.

  `opts` is a keyword list documented by `t:command_opts/0`. The only key
  currently recognised is `:use_compression`; implementations must accept
  (and may ignore) it.

  Single request, single response — streaming and multi-frame replies
  (scan, query) are out of scope for this behaviour.
  """
  @callback command(
              conn(),
              request :: iodata(),
              deadline_ms :: non_neg_integer(),
              opts :: command_opts()
            ) ::
              {:ok, binary()} | {:error, Aerospike.Error.t()}

  @typedoc """
  Options accepted by `c:stream_open/4`. The stream seam currently does not
  interpret any public keys, but callers pass a keyword list so later
  implementations can share capability flags without changing the callback
  shape.
  """
  @type stream_opts :: keyword()

  @typedoc "Opaque stream handle returned by `c:stream_open/4`."
  @type stream :: term()

  @doc """
  Opens a streaming request and returns an opaque handle for reading frames.

  The transport owns the long-lived socket state and delivery mechanics; the
  caller owns any parsing of returned frames. The request bytes are already
  encoded when they reach the transport.
  """
  @callback stream_open(
              conn(),
              request :: iodata(),
              deadline_ms :: non_neg_integer(),
              opts :: stream_opts()
            ) :: {:ok, stream()} | {:error, Aerospike.Error.t()}

  @doc """
  Reads the next frame from an open stream.

  Returns `{:ok, frame}` for a delivered frame, `:done` when the stream has
  reached end-of-stream, or `{:error, error}` for a transport-class failure.
  """
  @callback stream_read(stream(), deadline_ms :: non_neg_integer()) ::
              {:ok, binary()} | :done | {:error, Aerospike.Error.t()}

  @doc """
  Closes an open stream handle. Must be idempotent.
  """
  @callback stream_close(stream()) :: :ok

  @typedoc """
  Parsed admin-protocol login reply returned by `c:login/2`.

    * `:ok_no_token` — server accepted the login but issued no session
      token (e.g. PKI with no user mapping).
    * `{:session, token, ttl_seconds_or_nil}` — server accepted the login
      and issued `token`. `ttl_seconds` is the server-reported TTL (nil
      if omitted).
    * `:security_not_enabled` — server has security disabled (result
      code 52); callers treat this as success with no token.
  """
  @type login_reply ::
          :ok_no_token
          | {:session, binary(), non_neg_integer() | nil}
          | :security_not_enabled

  @doc """
  Runs the admin-protocol login (or session-token authenticate) handshake
  on an already-connected socket and returns the parsed reply.

  Optional — transports that do not support authentication raise
  `UndefinedFunctionError`; callers that use this callback must ensure
  the configured transport implements it.

  Opts:

    * `:user` — username (required when `:session_token` is absent).
    * `:password` — cleartext password (required when `:session_token`
      is absent; the transport hashes it as the server requires).
    * `:session_token` — when present, runs AUTHENTICATE instead of a
      fresh LOGIN. `:user` is still required.
    * `:login_timeout_ms` — read deadline applied to the login reply.
      Transport-specific default.
  """
  @callback login(conn(), opts :: keyword()) ::
              {:ok, login_reply()} | {:error, Aerospike.Error.t()}

  @optional_callbacks login: 2, stream_open: 4, stream_read: 2, stream_close: 1
end
