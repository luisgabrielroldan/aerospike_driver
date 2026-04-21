defmodule Aerospike.Transport.TcpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Protocol.Login
  alias Aerospike.Protocol.Message
  alias Aerospike.Telemetry
  alias Aerospike.Transport.Tcp

  # Exercise `command/3`'s read-deadline plumbing end to end against a
  # real loopback `:gen_tcp` listener. Integration GET tests already cover
  # the happy path against a live Aerospike; these tests pin behaviour the
  # happy-path can't observe (partial deadline blowout, connection closed
  # mid-read).

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

      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

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

      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:ok, ^body} = Tcp.command(conn, <<"req">>, 500)

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "command/3 fragmentation" do
    # Passive `{:packet, :raw}` + `:gen_tcp.recv(socket, N, _)` already
    # handles server-side TCP fragmentation transparently. This test pins
    # that guarantee so a future change to the recv shape — a switch to
    # active mode, a buffering refactor — cannot silently regress it.
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

      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:ok, ^body} = Tcp.command(conn, <<"req">>, 1_000)

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "command/3 header validation" do
    # The transport must route on `type` and validate `version == 2`
    # before handing the body back. A reply with the wrong version or
    # type is a typed `:parse_error`, never silently decoded.
    test "rejects a reply with an unexpected proto version", %{
      listener: listener,
      port: port
    } do
      body = <<1, 2, 3>>
      # version = 3 is not used by any released server; should be rejected.
      reply = typed_header(3, 3, byte_size(body)) <> body

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

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
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:error, %Error{code: :parse_error, message: msg}} =
               Tcp.command(conn, <<"req">>, 500)

      assert msg =~ "proto type"
      assert msg =~ "got 1"

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "command/3 compressed replies" do
    # The transport must transparently inflate a type-4 compressed AS_MSG
    # reply and return the inner frame's body to the caller, matching the
    # plain-reply shape. Layout: Go `command.go:3574-3627` +
    # `multi_command.go:150-173`.
    test "returns the inflated inner body verbatim", %{listener: listener, port: port} do
      inner_body = <<99, 88, 77, 66, 55>>
      reply = compressed_reply_of(inner_body)

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

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
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

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
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

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
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

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
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:error, %Error{code: :parse_error, message: msg}} =
               Tcp.command(conn, <<"req">>, 500)

      assert msg =~ "uncompressed-size prefix"

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "command/4 outbound compression" do
    # With `use_compression: true` and a request larger than the 128-byte
    # reference-client threshold, the wire frame is type 4 and its inner
    # zlib-compressed payload matches the original request. Below the
    # threshold or when compression would bloat the frame, the plain
    # request is sent verbatim.
    test "compresses requests above the 128-byte threshold", %{
      listener: listener,
      port: port
    } do
      request = request_frame(:binary.copy(<<0xA5>>, 200))
      body = <<7, 7, 7, 7>>
      reply = typed_header(2, 3, byte_size(body)) <> body

      {server, received_ref} = spawn_echo_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:ok, ^body} = Tcp.command(conn, request, 500, use_compression: true)

      assert_receive {^received_ref, sent}, 500

      <<header::binary-8, body_bytes::binary>> = sent
      assert {:ok, {2, 4, length}} = Message.decode_header(header)
      assert length == byte_size(body_bytes)

      {:ok, {uncompressed_size, compressed}} =
        Message.decode_compressed_payload(body_bytes)

      assert uncompressed_size == IO.iodata_length(request)
      assert :zlib.uncompress(compressed) == IO.iodata_to_binary(request)

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "sends requests at or below the threshold uncompressed", %{
      listener: listener,
      port: port
    } do
      request = request_frame(:binary.copy(<<0xB1>>, 64))
      body = <<3, 3, 3>>
      reply = typed_header(2, 3, byte_size(body)) <> body

      {server, received_ref} = spawn_echo_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:ok, ^body} = Tcp.command(conn, request, 500, use_compression: true)

      assert_receive {^received_ref, sent}, 500
      assert sent == IO.iodata_to_binary(request)

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "falls back to plain framing when compression would bloat the payload", %{
      listener: listener,
      port: port
    } do
      # Just above threshold but already high-entropy: zlib will add
      # framing overhead that exceeds any gain, so we must fall back.
      high_entropy = :crypto.strong_rand_bytes(140)
      request = request_frame(high_entropy)
      body = <<1>>
      reply = typed_header(2, 3, byte_size(body)) <> body

      {server, received_ref} = spawn_echo_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:ok, ^body} = Tcp.command(conn, request, 500, use_compression: true)

      assert_receive {^received_ref, sent}, 500
      assert sent == IO.iodata_to_binary(request)

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "defaults to no compression when the option is omitted", %{
      listener: listener,
      port: port
    } do
      request = request_frame(:binary.copy(<<0xC3>>, 400))
      body = <<9>>
      reply = typed_header(2, 3, byte_size(body)) <> body

      {server, received_ref} = spawn_echo_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:ok, ^body} = Tcp.command(conn, request, 500)

      assert_receive {^received_ref, sent}, 500
      assert sent == IO.iodata_to_binary(request)

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "round-trips a compressed request against a compressed reply", %{
      listener: listener,
      port: port
    } do
      request = request_frame(:binary.copy(<<0xD4>>, 300))
      inner_body = <<42, 42, 42, 42, 42>>
      reply = compressed_reply_of(inner_body)

      {server, received_ref} = spawn_echo_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:ok, ^inner_body} = Tcp.command(conn, request, 500, use_compression: true)

      assert_receive {^received_ref, sent}, 500
      <<outer_header::binary-8, _rest::binary>> = sent
      assert {:ok, {2, 4, _}} = Message.decode_header(outer_header)

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "login/2 and authenticated connect/3" do
    test "returns the parsed session token and ttl from a successful login reply", %{
      listener: listener,
      port: port
    } do
      token = "session-token"
      ttl = 3_600
      reply = login_reply(0, [{5, token}, {6, <<ttl::32-big>>}])

      server =
        spawn_server(listener, fn client_sock ->
          {:ok, request} = :gen_tcp.recv(client_sock, 0, 1_000)
          assert_login_request(request, 20)
          :ok = :gen_tcp.send(client_sock, reply)
          hold_client(client_sock)
        end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      assert {:ok, {:session, ^token, ^ttl}} =
               Tcp.login(conn, user: "admin", password: "secret")

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "emits the info_rpc span for login with [:login] commands metadata", %{
      listener: listener,
      port: port
    } do
      token = "session-token"
      ttl = 3_600
      reply = login_reply(0, [{5, token}, {6, <<ttl::32-big>>}])

      server =
        spawn_server(listener, fn client_sock ->
          {:ok, _request} = :gen_tcp.recv(client_sock, 0, 1_000)
          :ok = :gen_tcp.send(client_sock, reply)
          hold_client(client_sock)
        end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000, node_name: "A1")
      handler = attach_span_handlers(:login_rpc, [Telemetry.info_rpc_span()])

      try do
        assert {:ok, {:session, ^token, ^ttl}} =
                 Tcp.login(conn, user: "admin", password: "secret")

        assert_receive {:event, [:aerospike, :info, :rpc, :start], _meas,
                        %{node_name: "A1", commands: [:login]}},
                       500

        assert_receive {:event, [:aerospike, :info, :rpc, :stop], %{duration: _},
                        %{node_name: "A1", commands: [:login]}},
                       500
      after
        :telemetry.detach(handler)
      end

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "falls back from an expired session token to password login and keeps unary commands usable",
         %{listener: listener, port: port} do
      body = <<9, 8, 7>>
      command_reply = header(body) <> body

      server =
        spawn_server(listener, fn client_sock ->
          {:ok, auth_request} = :gen_tcp.recv(client_sock, 0, 1_000)
          assert_login_request(auth_request, 0)
          :ok = :gen_tcp.send(client_sock, login_reply(66))

          {:ok, login_request} = :gen_tcp.recv(client_sock, 0, 1_000)
          assert_login_request(login_request, 20)
          :ok = :gen_tcp.send(client_sock, login_reply(0))

          {:ok, command_request} = :gen_tcp.recv(client_sock, 0, 1_000)
          assert byte_size(command_request) > 0
          :ok = :gen_tcp.send(client_sock, command_reply)
          hold_client(client_sock)
        end)

      assert {:ok, conn} =
               Tcp.connect("127.0.0.1", port,
                 connect_timeout_ms: 1_000,
                 user: "admin",
                 password: "secret",
                 session_token: "cached-token"
               )

      assert {:ok, ^body} = Tcp.command(conn, <<"req">>, 500)

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

      {:ok, conn} =
        Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000, info_timeout: 500)

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

      {:ok, conn} =
        Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000, info_timeout: 500)

      assert {:error, %Error{code: :parse_error, message: msg}} = Tcp.info(conn, ["node"])
      assert msg =~ "proto type"
      assert msg =~ "got 3"

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "connect/3 TCP tuning opts" do
    # Every public TCP opt documented in the moduledoc must land on the
    # socket via `:inet.getopts/2`. Defaults turn `:nodelay` and
    # `:keepalive` on; buffer sizes stay at kernel defaults unless
    # explicitly set.
    test "defaults set :nodelay and :keepalive on", %{listener: listener, port: port} do
      server = spawn_server(listener, fn client -> hold_client(client) end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      {:ok, vals} = :inet.getopts(conn.socket, [:nodelay, :keepalive])
      assert Keyword.fetch!(vals, :nodelay) == true
      assert Keyword.fetch!(vals, :keepalive) == true

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "tcp_nodelay: false turns Nagle back on", %{listener: listener, port: port} do
      server = spawn_server(listener, fn client -> hold_client(client) end)

      {:ok, conn} =
        Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000, tcp_nodelay: false)

      {:ok, [nodelay: false]} = :inet.getopts(conn.socket, [:nodelay])

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "tcp_keepalive: false disables SO_KEEPALIVE", %{listener: listener, port: port} do
      server = spawn_server(listener, fn client -> hold_client(client) end)

      {:ok, conn} =
        Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000, tcp_keepalive: false)

      {:ok, [keepalive: false]} = :inet.getopts(conn.socket, [:keepalive])

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "tcp_sndbuf and tcp_rcvbuf set SO_SNDBUF / SO_RCVBUF", %{
      listener: listener,
      port: port
    } do
      server = spawn_server(listener, fn client -> hold_client(client) end)

      requested_sndbuf = 64 * 1024
      requested_rcvbuf = 128 * 1024

      {:ok, conn} =
        Tcp.connect("127.0.0.1", port,
          connect_timeout_ms: 1_000,
          tcp_sndbuf: requested_sndbuf,
          tcp_rcvbuf: requested_rcvbuf
        )

      {:ok, [sndbuf: sndbuf, recbuf: recbuf]} =
        :inet.getopts(conn.socket, [:sndbuf, :recbuf])

      # Kernels may round the requested value up to the page size, so
      # only assert the socket at-least-honours what we asked for.
      assert sndbuf >= requested_sndbuf
      assert recbuf >= requested_rcvbuf

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "rejects a non-boolean :tcp_nodelay with ArgumentError", %{
      listener: listener,
      port: port
    } do
      server = spawn_server(listener, fn client -> hold_client(client) end)

      assert_raise ArgumentError, ~r/:tcp_nodelay must be a boolean/, fn ->
        Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000, tcp_nodelay: :yes)
      end

      stop_server(server)
    end

    test "rejects a non-positive :tcp_sndbuf with ArgumentError", %{
      listener: listener,
      port: port
    } do
      server = spawn_server(listener, fn client -> hold_client(client) end)

      assert_raise ArgumentError, ~r/:tcp_sndbuf must be a positive integer/, fn ->
        Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000, tcp_sndbuf: 0)
      end

      stop_server(server)
    end
  end

  describe "command/4 telemetry" do
    # `command/4` must emit a `[:aerospike, :command, :send]` span around
    # the socket write and a `[:aerospike, :command, :recv]` span around
    # the header + body read. Metadata must carry `:node_name`,
    # `:attempt`, and `:deadline_ms`; `:recv` stop metadata must also
    # carry `:bytes`.
    test "emits send and recv spans tagged with :node_name and :attempt", %{
      listener: listener,
      port: port
    } do
      body = <<1, 2, 3, 4>>
      reply = header(body) <> body

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000, node_name: "A1")

      handler =
        attach_span_handlers(:command_send_and_recv, [
          Telemetry.command_send_span(),
          Telemetry.command_recv_span()
        ])

      try do
        assert {:ok, ^body} = Tcp.command(conn, <<"req">>, 500, attempt: 2)

        assert_receive {:event, [:aerospike, :command, :send, :start], _m1,
                        %{node_name: "A1", attempt: 2, deadline_ms: 500}}

        assert_receive {:event, [:aerospike, :command, :send, :stop], %{duration: _},
                        %{node_name: "A1", attempt: 2, deadline_ms: 500}}

        assert_receive {:event, [:aerospike, :command, :recv, :start], _m2,
                        %{node_name: "A1", attempt: 2, deadline_ms: 500}}

        assert_receive {:event, [:aerospike, :command, :recv, :stop], %{duration: _},
                        %{node_name: "A1", attempt: 2, deadline_ms: 500, bytes: 4}}
      after
        :telemetry.detach(handler)
      end

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "send-span stop fires when the socket write fails", %{
      listener: listener,
      port: port
    } do
      # Accept, then close immediately so the client's next send fails.
      server =
        spawn_server(listener, fn client_sock ->
          :gen_tcp.close(client_sock)
        end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000, node_name: "A1")
      # Give the server a moment to close the accepted socket so the
      # next send hits a closed peer rather than succeeding.
      Process.sleep(50)

      handler =
        attach_span_handlers(:command_send_failure, [
          Telemetry.command_send_span(),
          Telemetry.command_recv_span()
        ])

      try do
        # The send either succeeds (kernel buffer) then recv fails, or
        # the send fails outright. Either way the send span must fire a
        # stop event, so we only assert that form.
        _ = Tcp.command(conn, <<"req">>, 100, attempt: 0)

        assert_receive {:event, [:aerospike, :command, :send, :start], _m,
                        %{node_name: "A1", attempt: 0}}

        assert_receive {:event, [:aerospike, :command, :send, :stop], %{duration: _},
                        %{node_name: "A1", attempt: 0}}
      after
        :telemetry.detach(handler)
      end

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "defaults :attempt metadata to 0 when the caller omits it", %{
      listener: listener,
      port: port
    } do
      body = <<>>
      reply = header(body) <> body

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000, node_name: "A1")

      handler = attach_span_handlers(:command_default_attempt, [Telemetry.command_send_span()])

      try do
        assert {:ok, _} = Tcp.command(conn, <<"req">>, 500)

        assert_receive {:event, [:aerospike, :command, :send, :start], _m,
                        %{node_name: "A1", attempt: 0}}
      after
        :telemetry.detach(handler)
      end

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test ":node_name metadata falls back to nil when the opt is omitted", %{
      listener: listener,
      port: port
    } do
      body = <<0>>
      reply = header(body) <> body

      server = spawn_reply_server(listener, reply)
      {:ok, conn} = Tcp.connect("127.0.0.1", port, connect_timeout_ms: 1_000)

      handler = attach_span_handlers(:command_nil_node, [Telemetry.command_send_span()])

      try do
        assert {:ok, _} = Tcp.command(conn, <<"req">>, 500)

        assert_receive {:event, [:aerospike, :command, :send, :start], _m, %{node_name: nil}}
      after
        :telemetry.detach(handler)
      end

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "info/2 telemetry" do
    test "emits a [:aerospike, :info, :rpc] span with :commands metadata", %{
      listener: listener,
      port: port
    } do
      # Minimal info-shaped reply: `key\tvalue\n` body typed as 1 (info).
      body = "node\tabc\n"
      reply = typed_header(2, 1, byte_size(body)) <> body

      server = spawn_reply_server(listener, reply)

      {:ok, conn} =
        Tcp.connect("127.0.0.1", port,
          connect_timeout_ms: 1_000,
          info_timeout: 500,
          node_name: "A1"
        )

      handler = attach_span_handlers(:info_rpc, [Telemetry.info_rpc_span()])

      try do
        assert {:ok, %{"node" => "abc"}} = Tcp.info(conn, ["node"])

        assert_receive {:event, [:aerospike, :info, :rpc, :start], _m,
                        %{node_name: "A1", commands: ["node"]}}

        assert_receive {:event, [:aerospike, :info, :rpc, :stop], %{duration: _},
                        %{node_name: "A1", commands: ["node"]}}
      after
        :telemetry.detach(handler)
      end

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  # Forwarder used by the telemetry handlers below. Captured as a module
  # function to avoid the "local function handler" performance warning
  # `:telemetry.attach/4` emits for anonymous captures.
  @doc false
  def forward(event, measurements, metadata, test_pid) do
    send(test_pid, {:event, event, measurements, metadata})
  end

  # Attach a forwarding handler for the [:start, :stop, :exception] suffix
  # of every span prefix in `prefixes`. Returns the handler id used to
  # detach. `:telemetry.attach_many/4` requires a unique id per call, so
  # tests must pass a distinct `tag` to avoid collisions with sibling
  # tests running concurrently (async: true).
  defp attach_span_handlers(tag, prefixes) do
    handler_id = {__MODULE__, tag, make_ref()}

    events =
      for prefix <- prefixes,
          suffix <- [:start, :stop, :exception],
          do: prefix ++ [suffix]

    :ok = :telemetry.attach_many(handler_id, events, &__MODULE__.forward/4, self())

    handler_id
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

  defp assert_login_request(frame, command_id) do
    <<2, 2, _length::48-big, 0, 0, ^command_id, field_count, _::96, _rest::binary>> = frame
    assert field_count == 2
  end

  defp login_reply(result_code, fields \\ []) do
    fields_iodata =
      Enum.map(fields, fn {field_id, value} when is_binary(value) ->
        size = byte_size(value) + 1
        [<<size::32-big, field_id::8>>, value]
      end)

    body_size = Login.reply_header_size() - 8 + IO.iodata_length(fields_iodata)
    field_count = length(fields_iodata)

    [
      <<2, 2, body_size::48-big, 0, result_code::8, 0, field_count::8, 0::96>>,
      fields_iodata
    ]
  end

  # Accept one connection, buffer every byte the client sends until it
  # stops writing, forward the buffer to the test for assertion, then
  # send a prepared reply. The client-side `:gen_tcp.send/2` is a single
  # flush, so one `recv(socket, 0, _)` covers the full request. The
  # receive loop adds belt-and-suspenders for the rare case where the
  # kernel fragments a large frame.
  defp spawn_echo_server(listener, reply) do
    parent = self()
    ref = make_ref()

    server =
      spawn_server(listener, fn client_sock ->
        sent = recv_all(client_sock, <<>>)
        send(parent, {ref, sent})
        :ok = :gen_tcp.send(client_sock, reply)
        hold_client(client_sock)
      end)

    {server, ref}
  end

  defp recv_all(client_sock, acc) do
    case :gen_tcp.recv(client_sock, 0, 200) do
      {:ok, bytes} -> recv_all(client_sock, acc <> bytes)
      {:error, _} -> acc
    end
  end

  # Build a valid AS_MSG request frame (8-byte proto header + body). The
  # body is arbitrary bytes — the transport's outbound path does not
  # inspect it.
  defp request_frame(body) when is_binary(body) do
    typed_header(2, 3, byte_size(body)) <> body
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
