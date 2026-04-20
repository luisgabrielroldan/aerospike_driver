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
  """

  @behaviour Aerospike.NodeTransport

  alias Aerospike.Error
  alias Aerospike.Protocol.Info
  alias Aerospike.Protocol.Message

  @default_timeout 5_000
  @header_size 8
  # Outbound requests below this size are sent uncompressed even when the
  # caller sets `use_compression: true`. Matches Go
  # `command.go:_COMPRESS_THRESHOLD` and Java `Command.COMPRESS_THRESHOLD`.
  @compress_threshold 128
  @proto_version Message.proto_version()
  @type_info Message.type_info()
  @type_as_msg Message.type_as_msg()
  @type_compressed Message.type_compressed()

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

    with {:ok, version, type, body} <- send_recv(conn, request, timeout),
         :ok <- validate_version(version),
         :ok <- validate_type(type, @type_info) do
      Info.decode_response(body)
    end
  end

  @impl true
  def command(%__MODULE__{} = conn, request, deadline_ms, opts \\ [])
      when is_integer(deadline_ms) and deadline_ms >= 0 and is_list(opts) do
    framed = maybe_compress(request, opts)

    with {:ok, version, type, body} <- send_recv(conn, framed, deadline_ms),
         :ok <- validate_version(version),
         :ok <- validate_command_type(type) do
      maybe_decompress(type, body)
    end
  end

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
