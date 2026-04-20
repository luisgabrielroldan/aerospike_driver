defmodule Aerospike.NodeTransport do
  @moduledoc """
  Behaviour for per-node I/O, isolating cluster logic from transport details.

  The Tender, Router, and command modules call this behaviour — they must
  never reach `:gen_tcp` directly. The same contract is served by a real
  TCP implementation in production and a scripted fake in tests, so every
  cluster-logic path is exercised deterministically without sockets.

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
end
