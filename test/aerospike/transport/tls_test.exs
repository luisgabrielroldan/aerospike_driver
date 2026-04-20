defmodule Aerospike.Transport.TlsTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  # Exercises `Aerospike.Transport.Tls.connect/3` end-to-end against a
  # local `:ssl` listener loaded with the same CA + server + client
  # fixtures the EE TLS / PKI compose services use. The goal is to pin
  # the transport's wiring of `:ssl.connect/3` opts — hostname
  # verification, SNI, CA bundle, client cert / key — without needing an
  # Aerospike server. Command and info behaviour is already covered by
  # `Aerospike.Transport.TcpTest`; after the TLS handshake both
  # transports run the same code path.

  alias Aerospike.Error
  alias Aerospike.Transport.Tcp
  alias Aerospike.Transport.Tls

  @fixtures_dir "test/support/fixtures/tls"
  @ca_cert Path.join(@fixtures_dir, "ca.crt")
  @server_cert Path.join(@fixtures_dir, "server.crt")
  @server_key Path.join(@fixtures_dir, "server.key")
  @client_cert Path.join(@fixtures_dir, "client.crt")
  @client_key Path.join(@fixtures_dir, "client.key")

  setup_all do
    # Lazily ensure `:ssl` is started for the whole test module.
    {:ok, _} = Application.ensure_all_started(:ssl)
    :ok
  end

  setup do
    {listener, port} = listen_tls(require_client_cert: false)

    on_exit(fn ->
      _ = :ssl.close(listener)
    end)

    %{listener: listener, port: port}
  end

  describe "connect/3 standard TLS" do
    test "succeeds with a valid CA bundle and matching server name", %{
      listener: listener,
      port: port
    } do
      server = spawn_ssl_server(listener, fn _sock -> :ok end)

      assert {:ok, %Tcp{socket_mod: :ssl}} =
               Tls.connect("127.0.0.1", port,
                 connect_timeout_ms: 1_000,
                 tls_name: "localhost",
                 tls_cacertfile: @ca_cert
               )

      wait_for(server)
    end

    test "fails with :connection_error when the CA cannot verify the server cert", %{
      listener: listener,
      port: port
    } do
      server = spawn_ssl_server(listener, fn _sock -> :ok end)

      # Use the server cert itself as the CA bundle — it is self-issued
      # by the test CA, so presenting it as the trust root fails to
      # chain up to the server leaf.
      assert {:error, %Error{code: :connection_error, message: msg}} =
               Tls.connect("127.0.0.1", port,
                 connect_timeout_ms: 1_000,
                 tls_name: "localhost",
                 tls_cacertfile: @server_cert
               )

      assert msg =~ "TLS handshake failed"

      wait_for(server)
    end

    test "fails when the server name does not match the certificate's SAN", %{
      listener: listener,
      port: port
    } do
      server = spawn_ssl_server(listener, fn _sock -> :ok end)

      assert {:error, %Error{code: :connection_error, message: msg}} =
               Tls.connect("127.0.0.1", port,
                 connect_timeout_ms: 1_000,
                 tls_name: "wrong-host.example.com",
                 tls_cacertfile: @ca_cert
               )

      assert msg =~ "TLS handshake failed"

      wait_for(server)
    end

    test ":verify_none succeeds against an untrusted CA and logs a warning", %{
      listener: listener,
      port: port
    } do
      server = spawn_ssl_server(listener, fn _sock -> :ok end)

      log =
        capture_log(fn ->
          assert {:ok, %Tcp{socket_mod: :ssl} = conn} =
                   Tls.connect("127.0.0.1", port,
                     connect_timeout_ms: 1_000,
                     tls_name: "whatever",
                     tls_verify: :verify_none
                   )

          :ok = Tls.close(conn)
        end)

      assert log =~ ":tls_verify is :verify_none"

      wait_for(server)
    end
  end

  describe "connect/3 mTLS" do
    test "succeeds when both server and client present trusted certs" do
      {listener, port} = listen_tls(require_client_cert: true)

      server = spawn_ssl_server(listener, fn _sock -> :ok end)

      assert {:ok, %Tcp{socket_mod: :ssl} = conn} =
               Tls.connect("127.0.0.1", port,
                 connect_timeout_ms: 1_000,
                 tls_name: "localhost",
                 tls_cacertfile: @ca_cert,
                 tls_certfile: @client_cert,
                 tls_keyfile: @client_key
               )

      :ok = Tls.close(conn)

      wait_for(server)
      _ = :ssl.close(listener)
    end

    test "fails when the client cert is missing on a server that requires it" do
      # Force TLS 1.2 on both ends. Under TLS 1.3 a mTLS server can
      # accept the client's ClientHello, send its own Finished, and only
      # abort *after* the client's connect has already returned — the
      # handshake shape is asymmetric. Pinning 1.2 makes the server
      # alert observable synchronously on the client side, which is the
      # contract we care about.
      {listener, port} = listen_tls(require_client_cert: true, versions: [:"tlsv1.2"])

      server = spawn_ssl_server(listener, fn _sock -> :ok end)

      assert {:error, %Error{code: :connection_error}} =
               Tls.connect("127.0.0.1", port,
                 connect_timeout_ms: 1_000,
                 tls_name: "localhost",
                 tls_cacertfile: @ca_cert,
                 tls_opts: [versions: [:"tlsv1.2"]]
               )

      wait_for(server)
      _ = :ssl.close(listener)
    end
  end

  describe "connect/3 opt validation" do
    test "rejects a non-boolean :tcp_nodelay", %{port: port} do
      assert_raise ArgumentError, ~r/:tcp_nodelay must be a boolean/, fn ->
        Tls.connect("127.0.0.1", port,
          connect_timeout_ms: 1_000,
          tcp_nodelay: :yes,
          tls_name: "localhost",
          tls_cacertfile: @ca_cert
        )
      end
    end

    test "rejects a non-positive :tcp_sndbuf", %{port: port} do
      assert_raise ArgumentError, ~r/:tcp_sndbuf must be a positive integer/, fn ->
        Tls.connect("127.0.0.1", port,
          connect_timeout_ms: 1_000,
          tcp_sndbuf: 0,
          tls_name: "localhost",
          tls_cacertfile: @ca_cert
        )
      end
    end

    test "rejects an invalid :tls_verify atom", %{port: port} do
      assert_raise ArgumentError, ~r/:tls_verify must be :verify_peer or :verify_none/, fn ->
        Tls.connect("127.0.0.1", port,
          connect_timeout_ms: 1_000,
          tls_name: "localhost",
          tls_verify: :yolo
        )
      end
    end

    test "rejects a half-configured client cert / key pair", %{port: port} do
      assert_raise ArgumentError, ~r/:tls_certfile and :tls_keyfile/, fn ->
        Tls.connect("127.0.0.1", port,
          connect_timeout_ms: 1_000,
          tls_name: "localhost",
          tls_cacertfile: @ca_cert,
          tls_certfile: @client_cert
        )
      end
    end
  end

  ## helpers

  defp listen_tls(opts) do
    require_client_cert? = Keyword.fetch!(opts, :require_client_cert)
    versions = Keyword.get(opts, :versions)

    base = [
      :binary,
      {:active, false},
      {:packet, :raw},
      {:reuseaddr, true},
      {:certfile, @server_cert},
      {:keyfile, @server_key},
      {:cacertfile, @ca_cert}
    ]

    listen_opts =
      base
      |> maybe_append(require_client_cert?, [
        {:verify, :verify_peer},
        {:fail_if_no_peer_cert, true}
      ])
      |> maybe_append(is_list(versions), [{:versions, versions}])

    {:ok, listener} = :ssl.listen(0, listen_opts)
    {:ok, {_addr, port}} = :ssl.sockname(listener)
    {listener, port}
  end

  defp maybe_append(acc, false, _opts), do: acc
  defp maybe_append(acc, true, opts), do: acc ++ opts

  # Accept one TLS connection and run `handler` against the handshaked
  # socket. `:ssl.handshake/2` is the explicit handshake step `:ssl.accept/2`
  # used to inline; using it here keeps the test listener robust against
  # OTP 26+'s stricter handshake separation.
  defp spawn_ssl_server(listener, handler) do
    parent = self()

    spawn_link(fn -> accept_one(listener, parent, handler) end)
  end

  defp accept_one(listener, parent, handler) do
    case :ssl.transport_accept(listener, 2_000) do
      {:ok, transport} -> do_handshake(transport, parent, handler)
      {:error, _} -> :ok
    end
  end

  defp do_handshake(transport, parent, handler) do
    case :ssl.handshake(transport, 2_000) do
      {:ok, sock} ->
        send(parent, {:accepted, self()})
        handler.(sock)
        :ssl.close(sock)

      {:error, _} ->
        send(parent, {:handshake_failed, self()})
        :ok
    end
  end

  defp wait_for(server) do
    receive do
      {:accepted, ^server} -> :ok
      {:handshake_failed, ^server} -> :ok
    after
      1_000 -> :ok
    end
  end
end
