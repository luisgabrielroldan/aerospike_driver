defmodule Aerospike.Transport.TcpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Transport.Tcp

  # Tier 1.5 Task 4 — exercise `command/3`'s read-deadline plumbing end to
  # end against a real loopback `:gen_tcp` listener. Integration GET tests
  # already cover the happy path against a live Aerospike; these tests
  # pin behaviour the happy-path can't observe (partial deadline blowout,
  # connection closed mid-read).

  setup do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])
    {:ok, port} = :inet.port(listener)

    on_exit(fn ->
      _ = :gen_tcp.close(listener)
    end)

    %{listener: listener, port: port}
  end

  describe "command/3 read deadline" do
    test "times out with a :timeout error when the server never replies", %{
      listener: listener,
      port: port
    } do
      server = spawn_server(listener, fn client_sock -> hold_client(client_sock) end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      started = System.monotonic_time(:millisecond)
      assert {:error, %Error{code: :timeout}} = Tcp.command(conn, <<"req">>, 80)
      elapsed = System.monotonic_time(:millisecond) - started

      # Deadline must have fired, not run to the generous connect timeout.
      assert elapsed < 500,
             "expected read to fail near 80 ms deadline, took #{elapsed} ms"

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "returns the full body when the server answers before the deadline", %{
      listener: listener,
      port: port
    } do
      body = <<10, 20, 30, 40>>
      reply = header(body) <> body

      server =
        spawn_server(listener, fn client_sock ->
          # Consume one request then send the prepared reply.
          {:ok, _} = :gen_tcp.recv(client_sock, 0, 1_000)
          :ok = :gen_tcp.send(client_sock, reply)
          hold_client(client_sock)
        end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      assert {:ok, ^body} = Tcp.command(conn, <<"req">>, 500)

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "command/3 fragmentation" do
    # Tier 1.5 Task 5 — passive `{:packet, :raw}` + `:gen_tcp.recv(socket,
    # N, _)` already handles server-side TCP fragmentation transparently
    # (see notes.md Finding 7). This test pins that guarantee so a future
    # change to the recv shape — a switch to active mode, a buffering
    # refactor — cannot silently regress it.
    test "reassembles a reply split across multiple sends", %{
      listener: listener,
      port: port
    } do
      body = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12>>
      full_reply = header(body) <> body
      # Split inside the header so both `recv_exact` calls must survive
      # fragmentation, not just the body recv.
      <<first::binary-size(4), rest::binary>> = full_reply

      server =
        spawn_server(listener, fn client_sock ->
          {:ok, _} = :gen_tcp.recv(client_sock, 0, 1_000)
          :ok = :gen_tcp.send(client_sock, first)
          # Small delay so the client's first `recv` is likely to be
          # woken by the partial arrival before the rest lands. On a
          # loopback socket this is best-effort, but the assertion does
          # not depend on timing — only on reassembly correctness.
          Process.sleep(20)
          :ok = :gen_tcp.send(client_sock, rest)
          hold_client(client_sock)
        end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      assert {:ok, ^body} = Tcp.command(conn, <<"req">>, 1_000)

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "command/3 header validation" do
    # Tier 2.5 Task 1 — the transport must route on `type` and validate
    # `version == 2` before handing the body back. A reply with the wrong
    # version or type is a typed `:parse_error`, never silently decoded.
    test "rejects a reply with an unexpected proto version", %{
      listener: listener,
      port: port
    } do
      body = <<1, 2, 3>>
      # version = 3 is not used by any released server; should be rejected.
      reply = typed_header(3, 3, byte_size(body)) <> body

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      assert {:error, %Error{code: :parse_error, message: msg}} =
               Tcp.command(conn, <<"req">>, 500)

      assert msg =~ "proto version"
      assert msg =~ "got 3"

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "rejects an info-typed reply on the command path", %{
      listener: listener,
      port: port
    } do
      body = <<9>>
      # type = 1 (info) on the command path must not decode silently.
      reply = typed_header(2, 1, byte_size(body)) <> body

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      assert {:error, %Error{code: :parse_error, message: msg}} =
               Tcp.command(conn, <<"req">>, 500)

      assert msg =~ "proto type"
      assert msg =~ "got 1"

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "command/3 compressed replies" do
    # Tier 2.5 Task 2 — the transport must transparently inflate a type-4
    # compressed AS_MSG reply and return the inner frame's body to the
    # caller, matching the plain-reply shape. Layout: Go
    # `command.go:3574-3627` + `multi_command.go:150-173`.
    test "returns the inflated inner body verbatim", %{listener: listener, port: port} do
      inner_body = <<99, 88, 77, 66, 55>>
      reply = compressed_reply_of(inner_body)

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      assert {:ok, ^inner_body} = Tcp.command(conn, <<"req">>, 500)

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "rejects a compressed reply with a corrupted zlib stream", %{
      listener: listener,
      port: port
    } do
      # 16-byte body: 8-byte size prefix (advertises an inner frame) + 8 bytes
      # that are not a valid zlib stream.
      compressed_body = <<100::64-big, 0, 0, 0, 0, 0, 0, 0, 0>>
      reply = typed_header(2, 4, byte_size(compressed_body)) <> compressed_body

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      assert {:error, %Error{code: :parse_error, message: msg}} =
               Tcp.command(conn, <<"req">>, 500)

      assert msg =~ "inflate"

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "rejects a compressed reply advertising a wrong inner size", %{
      listener: listener,
      port: port
    } do
      inner_body = <<1, 2, 3>>
      inner_frame = typed_header(2, 3, byte_size(inner_body)) <> inner_body
      compressed = :zlib.compress(inner_frame)
      # Advertise a size that does not match the inflated frame.
      wrong_size = byte_size(inner_frame) + 1
      compressed_body = <<wrong_size::64-big, compressed::binary>>
      reply = typed_header(2, 4, byte_size(compressed_body)) <> compressed_body

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      assert {:error, %Error{code: :parse_error, message: msg}} =
               Tcp.command(conn, <<"req">>, 500)

      assert msg =~ "size mismatch"

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "rejects a compressed reply wrapping a non-AS_MSG body", %{
      listener: listener,
      port: port
    } do
      inner_body = <<7>>
      # Inner frame has type = 1 (info) — the compressed wrapper only ever
      # carries AS_MSG (type 3) in any official client.
      inner_frame = typed_header(2, 1, byte_size(inner_body)) <> inner_body
      reply = wrap_compressed(inner_frame)

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      assert {:error, %Error{code: :parse_error, message: msg}} =
               Tcp.command(conn, <<"req">>, 500)

      assert msg =~ "proto type"
      assert msg =~ "got 1"

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "rejects a compressed reply missing its 8-byte size prefix", %{
      listener: listener,
      port: port
    } do
      # Body shorter than 8 bytes is not even long enough for the prefix.
      compressed_body = <<1, 2, 3>>
      reply = typed_header(2, 4, byte_size(compressed_body)) <> compressed_body

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      assert {:error, %Error{code: :parse_error, message: msg}} =
               Tcp.command(conn, <<"req">>, 500)

      assert msg =~ "uncompressed-size prefix"

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "info/2 header validation" do
    test "rejects a reply with an unexpected proto version", %{
      listener: listener,
      port: port
    } do
      body = <<>>
      reply = typed_header(3, 1, byte_size(body)) <> body

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000, info_timeout: 500)

      assert {:error, %Error{code: :parse_error, message: msg}} = Tcp.info(conn, ["node"])
      assert msg =~ "proto version"
      assert msg =~ "got 3"

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "rejects an AS_MSG-typed reply on the info path", %{
      listener: listener,
      port: port
    } do
      body = <<0, 0, 0>>
      # type = 3 (AS_MSG) on the info path must surface as :parse_error.
      reply = typed_header(2, 3, byte_size(body)) <> body

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000, info_timeout: 500)

      assert {:error, %Error{code: :parse_error, message: msg}} = Tcp.info(conn, ["node"])
      assert msg =~ "proto type"
      assert msg =~ "got 3"

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  # Build a valid 8-byte AS proto header for a given body. Matches
  # `Aerospike.Protocol.Message.encode_header/3` for version 2, type 3
  # (AS_MSG). The body content does not matter for `command/3` — the
  # transport returns whatever bytes it reads after the header.
  defp header(body), do: typed_header(2, 3, byte_size(body))

  defp typed_header(version, type, length) do
    import Bitwise
    proto_word = length ||| version <<< 56 ||| type <<< 48
    <<proto_word::64-big>>
  end

  # Build a type-4 compressed reply whose inner frame is a plain AS_MSG
  # (type 3) carrying `inner_body`. Matches the Go client's encoder in
  # `command.go:3574-3627`.
  defp compressed_reply_of(inner_body) do
    inner_frame = typed_header(2, 3, byte_size(inner_body)) <> inner_body
    wrap_compressed(inner_frame)
  end

  # Wrap a pre-built inner frame in the compressed-reply envelope: outer
  # type-4 proto header + 8-byte big-endian uncompressed-size prefix +
  # zlib-compressed inner-frame bytes.
  defp wrap_compressed(inner_frame) do
    compressed = :zlib.compress(inner_frame)
    body = <<byte_size(inner_frame)::64-big, compressed::binary>>
    typed_header(2, 4, byte_size(body)) <> body
  end

  defp spawn_reply_server(listener, reply) do
    spawn_server(listener, fn client_sock ->
      {:ok, _} = :gen_tcp.recv(client_sock, 0, 1_000)
      :ok = :gen_tcp.send(client_sock, reply)
      hold_client(client_sock)
    end)
  end

  defp spawn_server(listener, client_fn) do
    parent = self()

    spawn_link(fn ->
      case :gen_tcp.accept(listener, 2_000) do
        {:ok, client_sock} ->
          send(parent, {:accepted, self()})
          client_fn.(client_sock)

        {:error, _} ->
          :ok
      end
    end)
  end

  # Keep the client socket open and blocked so the transport's `recv`
  # trips the read deadline rather than observing a premature close.
  defp hold_client(client_sock) do
    receive do
      :stop -> :gen_tcp.close(client_sock)
    after
      5_000 -> :gen_tcp.close(client_sock)
    end
  end

  defp stop_server(server) do
    if Process.alive?(server), do: send(server, :stop)
    :ok
  end
end
