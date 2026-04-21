defmodule Aerospike.Transport.Tls do
  @moduledoc """
  TLS (`:ssl`) implementation of `Aerospike.NodeTransport`.

  The only real difference between this transport and
  `Aerospike.Transport.Tcp` is how the socket is opened: `connect/3`
  performs a plain TCP handshake and immediately upgrades the socket via
  `:ssl.connect/3` with the caller-supplied verification / cert / key
  opts. Once the TLS handshake completes the connection handle is an
  `Aerospike.Transport.Tcp{}` struct whose `socket_mod` is `:ssl` rather
  than `:gen_tcp`, so every post-connect operation — framing, auth,
  compression, telemetry, stream framing — is shared with the plaintext
  path. The other callbacks (`command/4`, `info/2`, `stream_open/4`,
  `stream_read/2`, `stream_close/1`, `close/1`, `login/2`) therefore
  delegate straight to `Aerospike.Transport.Tcp`.

  Callers select TLS by setting `transport: Aerospike.Transport.Tls` on
  `Aerospike.start_link/1`. Three deployment shapes are supported:

    * **Standard TLS** — server certificate verified against a CA bundle,
      no client cert. Set `:tls_cacertfile` and `:tls_name`.
    * **mTLS** — client presents a cert and key in addition to verifying
      the server. Set `:tls_certfile` / `:tls_keyfile` alongside the CA
      bundle.
    * **PKI** — the server accepts the client cert as auth; `:user` /
      `:password` may be omitted. Same opt shape as mTLS; auth is a
      server-side configuration concern, not a transport one.

  ## `connect/3` options

  Every option lives in the `opts` keyword list. Keys that `Transport.Tcp`
  also accepts (`:connect_timeout_ms`, `:info_timeout`, `:node_name`,
  `:user`, `:password`, `:session_token`, TCP tuning knobs) flow through
  unchanged — the TCP handshake uses them before the TLS upgrade, and the
  login RPC runs on top of the upgraded socket if credentials are set.

  TLS-specific keys:

    * `:tls_name` — string compared against the server certificate's
      subject / SAN. When set, enables Server Name Indication (SNI) and
      hostname verification. When absent, falls back to the `host`
      argument passed to `connect/3`; callers that connect by IP must
      set `:tls_name` explicitly because OTP's default `:verify_peer`
      rejects an IP as a host name.
    * `:tls_cacertfile` — path to a PEM-encoded CA bundle used to verify
      the server certificate. Required when `:tls_verify` is
      `:verify_peer` (the default).
    * `:tls_certfile` / `:tls_keyfile` — PEM-encoded client certificate
      and private key. Required for mTLS and PKI; omitted for standard
      one-way TLS.
    * `:tls_verify` — `:verify_peer` (default) or `:verify_none`. The
      latter is test-only and logs a `:warning` at connect time.
    * `:tls_opts` — extra keyword list passed through to `:ssl.connect/3`
      verbatim after the plan-level keys above have been translated.
      Use for rarely-tuned knobs (cipher suites, depth, CRL files) that
      the plan does not expose as first-class options.

  ## Security notes

  OTP does not verify hostnames by default even with `:verify_peer` —
  the caller must supply `:server_name_indication` and a `:customize_hostname_check`
  configuration. This module builds both from `:tls_name` so a caller
  who sets the CA bundle and the expected name automatically gets
  hostname verification. Callers who need stricter verification (CRL,
  OCSP, pinning) pass extra opts through `:tls_opts`.
  """

  @behaviour Aerospike.NodeTransport

  require Logger

  alias Aerospike.Error
  alias Aerospike.Transport.Tcp

  # `:ssl` and `:public_key` ship with OTP but are not always loaded by
  # the Elixir compiler's xref step on small hosts. They are listed in
  # `extra_applications` (`mix.exs`) so the release starts them at
  # boot; this silences the compile-time warning without masking real
  # typos.
  @compile {:no_warn_undefined, [:ssl, :public_key]}

  @default_connect_timeout_ms 5_000

  @impl true
  def connect(host, port, opts \\ []) when is_binary(host) and is_integer(port) do
    connect_timeout_ms = Keyword.get(opts, :connect_timeout_ms, @default_connect_timeout_ms)

    tcp_opts = build_tcp_opts(opts, connect_timeout_ms)
    ssl_opts = build_ssl_opts(host, opts)

    with {:ok, tcp_socket} <-
           :gen_tcp.connect(to_charlist(host), port, tcp_opts, connect_timeout_ms),
         {:ok, ssl_socket} <- ssl_upgrade(tcp_socket, ssl_opts, connect_timeout_ms, host, port) do
      conn = Tcp.wrap_ssl_socket(ssl_socket, opts)
      Tcp.maybe_login_after_handshake(conn, opts, host, port)
    end
  end

  @impl true
  defdelegate close(conn), to: Tcp

  @impl true
  defdelegate info(conn, commands), to: Tcp

  @impl true
  defdelegate command(conn, request, deadline_ms, opts), to: Tcp

  @impl true
  defdelegate stream_open(conn, request, deadline_ms, opts), to: Tcp

  @impl true
  defdelegate stream_read(stream, deadline_ms), to: Tcp

  @impl true
  defdelegate stream_close(stream), to: Tcp

  @impl true
  defdelegate login(conn, opts), to: Tcp

  # Base TCP opts only — every TLS-relevant option is applied by
  # `:ssl.connect/3`, not the underlying TCP socket. We still honour the
  # same send-buffer / recv-buffer / keepalive / nodelay tuning knobs the
  # plaintext transport exposes, because they apply to the kernel socket
  # regardless of whether it has been upgraded.
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

  # Translates the plan-level TLS keys into `:ssl.connect/3` options.
  # OTP defaults to `:verify_none`, which silently accepts forged certs;
  # we flip the default to `:verify_peer` so every TLS caller that does
  # not opt out gets real verification. `:tls_verify: :verify_none`
  # requires an explicit opt-out and logs a `:warning` at connect time.
  defp build_ssl_opts(host, opts) do
    verify = Keyword.get(opts, :tls_verify, :verify_peer)
    name = Keyword.get(opts, :tls_name, host)
    cacertfile = Keyword.get(opts, :tls_cacertfile)
    certfile = Keyword.get(opts, :tls_certfile)
    keyfile = Keyword.get(opts, :tls_keyfile)
    extra = Keyword.get(opts, :tls_opts, [])

    if verify == :verify_none do
      Logger.warning(
        "Aerospike.Transport.Tls: :tls_verify is :verify_none — " <>
          "server certificate will not be validated. Only acceptable in tests."
      )
    end

    []
    |> put_verify(verify)
    |> put_sni_and_hostname_check(name, verify)
    |> put_cacertfile(cacertfile)
    |> put_client_cert(certfile, keyfile)
    |> Kernel.++(extra)
  end

  defp put_verify(acc, :verify_peer), do: acc ++ [{:verify, :verify_peer}]
  defp put_verify(acc, :verify_none), do: acc ++ [{:verify, :verify_none}]

  defp put_verify(_acc, other) do
    raise ArgumentError,
          "Aerospike.Transport.Tls: :tls_verify must be :verify_peer or :verify_none, " <>
            "got #{inspect(other)}"
  end

  # SNI is always useful (lets the server pick the right cert); hostname
  # verification only applies under `:verify_peer`. OTP's default peer
  # verification accepts any cert signed by the trusted CA even if the
  # name does not match — `customize_hostname_check` with
  # `:https.match_fun/1` is what pins the subject / SAN check.
  defp put_sni_and_hostname_check(acc, nil, _verify), do: acc

  defp put_sni_and_hostname_check(acc, name, :verify_peer) when is_binary(name) do
    acc ++
      [
        {:server_name_indication, to_charlist(name)},
        {:customize_hostname_check,
         [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]}
      ]
  end

  defp put_sni_and_hostname_check(acc, name, :verify_none) when is_binary(name) do
    acc ++ [{:server_name_indication, to_charlist(name)}]
  end

  defp put_cacertfile(acc, nil), do: acc

  defp put_cacertfile(acc, path) when is_binary(path) do
    acc ++ [{:cacertfile, path}]
  end

  defp put_cacertfile(_acc, other) do
    raise ArgumentError,
          "Aerospike.Transport.Tls: :tls_cacertfile must be a path string, got #{inspect(other)}"
  end

  defp put_client_cert(acc, nil, nil), do: acc

  defp put_client_cert(acc, certfile, keyfile)
       when is_binary(certfile) and is_binary(keyfile) do
    acc ++ [{:certfile, certfile}, {:keyfile, keyfile}]
  end

  defp put_client_cert(_acc, certfile, keyfile) do
    raise ArgumentError,
          "Aerospike.Transport.Tls: :tls_certfile and :tls_keyfile must both be strings or " <>
            "both be absent, got certfile=#{inspect(certfile)} keyfile=#{inspect(keyfile)}"
  end

  defp ssl_upgrade(tcp_socket, ssl_opts, timeout_ms, host, port) do
    case :ssl.connect(tcp_socket, ssl_opts, timeout_ms) do
      {:ok, ssl_socket} ->
        {:ok, ssl_socket}

      {:error, reason} ->
        _ = :gen_tcp.close(tcp_socket)

        {:error,
         %Error{
           code: :connection_error,
           message:
             "TLS handshake failed connecting to #{host}:#{port}: #{format_ssl_reason(reason)}"
         }}
    end
  end

  defp bool_error(key, value) do
    "Aerospike.Transport.Tls: #{inspect(key)} must be a boolean, got #{inspect(value)}"
  end

  defp size_error(key, value) do
    "Aerospike.Transport.Tls: #{inspect(key)} must be a positive integer or nil, " <>
      "got #{inspect(value)}"
  end

  defp format_ssl_reason({:tls_alert, {alert, detail}}) when is_atom(alert) do
    "tls_alert #{alert}: #{detail}"
  end

  defp format_ssl_reason({:options, reason}) do
    "invalid ssl options: #{inspect(reason)}"
  end

  defp format_ssl_reason(:timeout), do: "timeout"
  defp format_ssl_reason(reason), do: inspect(reason)
end
