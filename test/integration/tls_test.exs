defmodule Aerospike.Integration.TlsTest do
  @moduledoc """
  Exercises the `:ssl`-based transport variant against real Enterprise
  Edition containers that speak TLS on port 4333 (standard TLS, no
  client-cert requirement) and port 4334 (PKI, server demands a client
  cert at the handshake). Neither container has security enabled, so
  these tests cover the transport seam only; auth behaviour is covered
  by `Aerospike.Integration.AuthTest`.

  Start the containers with
  `docker compose --profile enterprise up -d aerospike-ee-tls aerospike-ee-pki`
  before running the suite.

  Tagged `:integration` and `:enterprise` so it is excluded from the
  default suite; run with
  `mix test --include integration --include enterprise`.
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :enterprise

  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Key

  @tls_port 4333
  @pki_port 4334
  @namespace "test"

  @fixtures_dir "test/support/fixtures/tls"
  @ca_cert Path.join(@fixtures_dir, "ca.crt")
  @client_cert Path.join(@fixtures_dir, "client.crt")
  @client_key Path.join(@fixtures_dir, "client.key")

  setup_all do
    probe!("aerospike-ee-tls", "localhost", @tls_port)
    probe!("aerospike-ee-pki", "localhost", @pki_port)
    :ok
  end

  describe "standard TLS (aerospike-ee-tls on 4333)" do
    test "authenticated TLS handshake followed by a successful info probe" do
      cluster =
        start_cluster!(
          name: :"spike_tls_std_#{System.unique_integer([:positive])}",
          port: @tls_port,
          connect_opts: [
            tls_name: "localhost",
            tls_cacertfile: @ca_cert
          ]
        )

      :ok = Tender.tend_now(cluster)

      assert Tender.ready?(cluster),
             "Tender must be ready after one tend cycle against aerospike-ee-tls"

      key =
        Key.new(
          @namespace,
          "spike",
          "spike_tls_missing_#{System.unique_integer([:positive])}"
        )

      assert {:error, %Error{code: :key_not_found}} = Aerospike.get(cluster, key)
    end

    test "untrusted CA bundle surfaces as cluster_not_ready" do
      # Point `:tls_cacertfile` at the client cert itself — that cert
      # cannot chain up to the server, so the TLS handshake fails and
      # the seed never registers.
      cluster =
        start_cluster!(
          name: :"spike_tls_bad_ca_#{System.unique_integer([:positive])}",
          port: @tls_port,
          connect_opts: [
            tls_name: "localhost",
            tls_cacertfile: @client_cert
          ]
        )

      :ok = Tender.tend_now(cluster)

      refute Tender.ready?(cluster),
             "Tender must not register a node when TLS verification fails"

      key = Key.new(@namespace, "spike", "spike_tls_bad_ca_#{System.unique_integer([:positive])}")
      assert {:error, :cluster_not_ready} = Aerospike.get(cluster, key)
    end

    test "wrong :tls_name surfaces as cluster_not_ready" do
      cluster =
        start_cluster!(
          name: :"spike_tls_wrong_name_#{System.unique_integer([:positive])}",
          port: @tls_port,
          connect_opts: [
            tls_name: "not-the-server-name.example.com",
            tls_cacertfile: @ca_cert
          ]
        )

      :ok = Tender.tend_now(cluster)

      refute Tender.ready?(cluster),
             "Tender must not register a node when hostname verification fails"

      key =
        Key.new(@namespace, "spike", "spike_tls_wrong_name_#{System.unique_integer([:positive])}")

      assert {:error, :cluster_not_ready} = Aerospike.get(cluster, key)
    end
  end

  describe "PKI (aerospike-ee-pki on 4334)" do
    test "mTLS handshake with a trusted client cert authenticates at the transport layer" do
      cluster =
        start_cluster!(
          name: :"spike_pki_ok_#{System.unique_integer([:positive])}",
          port: @pki_port,
          connect_opts: [
            tls_name: "localhost",
            tls_cacertfile: @ca_cert,
            tls_certfile: @client_cert,
            tls_keyfile: @client_key
          ]
        )

      :ok = Tender.tend_now(cluster)

      assert Tender.ready?(cluster),
             "Tender must be ready after an mTLS handshake against aerospike-ee-pki"

      key =
        Key.new(
          @namespace,
          "spike",
          "spike_pki_missing_#{System.unique_integer([:positive])}"
        )

      assert {:error, %Error{code: :key_not_found}} = Aerospike.get(cluster, key)
    end
  end

  defp start_cluster!(opts) do
    name = Keyword.fetch!(opts, :name)
    port = Keyword.fetch!(opts, :port)
    connect_opts = Keyword.fetch!(opts, :connect_opts)

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tls,
        hosts: ["localhost:#{port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 2,
        connect_opts: connect_opts
      )

    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    name
  end

  defp probe!(container, host, port) do
    # Plain TCP probe: does the host forward the port at all?
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 1_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, reason} ->
        raise "#{container} not reachable at #{host}:#{port} " <>
                "(#{inspect(reason)}). Run `docker compose --profile enterprise " <>
                "up -d #{container}` first."
    end
  end
end
