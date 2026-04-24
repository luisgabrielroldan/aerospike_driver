defmodule Aerospike.Integration.AuthTest do
  @moduledoc """
  Exercises the user/password + session-token login flow against a real
  Enterprise Edition container with `security.enable-quotas` on.

  The EE security profile publishes port 3200 on the host and ships with
  a pre-created `admin/admin` superuser. The test starts a fresh cluster
  supervisor per case, drives one manual tend cycle, and asserts that:

    * correct credentials authenticate the info socket and every pool
      worker, so `Aerospike.get/3` reaches the server and returns
      `:key_not_found` for a random missing key;
    * wrong credentials prevent the Tender from registering any node,
      so the request surfaces as `{:error, :cluster_not_ready}`;
    * omitting credentials entirely against a security-enabled server
      is indistinguishable from wrong credentials at the driver level —
      no node is registered and the request short-circuits with
      `{:error, :cluster_not_ready}`.

  Tagged `:integration` and `:enterprise` so it is excluded from the
  default suite; run with
  `mix test --include integration --include enterprise`.
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :enterprise

  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Test.IntegrationSupport

  @host "localhost"
  @port 3200
  @namespace "test"
  @container "aerospike-ee-security"
  @data_user "spike"
  @data_password "spike-password"

  setup_all do
    IntegrationSupport.probe_aerospike!(
      @host,
      @port,
      "Run `docker compose --profile enterprise up -d aerospike-ee-security` first."
    )

    ensure_data_user!(@data_user, @data_password)
    :ok
  end

  setup do
    :ok
  end

  describe "with the EE security profile" do
    test "valid data-plane credentials let get/3 reach the server" do
      cluster = start_cluster!(user: @data_user, password: @data_password)

      IntegrationSupport.wait_for_tender_ready!(cluster, 5_000)
      key = IntegrationSupport.unique_key(@namespace, "spike", "spike_auth_missing")

      assert {:error, %Error{code: :key_not_found}} = Aerospike.get(cluster, key)
    end

    test "wrong password prevents bootstrap and requests fail fast" do
      cluster = start_cluster!(user: @data_user, password: "definitely-not-the-password")

      # tend_now returns :ok even when the seed login fails; the Tender
      # logs the auth error and leaves the cluster view empty. The
      # observable contract is therefore `ready?/1` staying false and
      # the routing layer refusing to dispatch.
      :ok = Tender.tend_now(cluster)

      refute Tender.ready?(cluster),
             "Tender must not register a node when the seed login fails"

      key = IntegrationSupport.unique_key(@namespace, "spike", "spike_auth_wrong")
      assert {:error, :cluster_not_ready} = Aerospike.get(cluster, key)
    end

    test "no credentials against a security-enabled server also fail fast" do
      # No `:user`/`:password` → Tender skips login. The first info
      # probe fails because the server rejects unauthenticated info
      # sockets, so no node is registered.
      cluster = start_cluster!([])

      :ok = Tender.tend_now(cluster)

      refute Tender.ready?(cluster),
             "Tender must not register a node without credentials against a secured cluster"

      key = IntegrationSupport.unique_key(@namespace, "spike", "spike_auth_nocreds")

      assert {:error, :cluster_not_ready} = Aerospike.get(cluster, key)
    end
  end

  defp start_cluster!(extra_opts) do
    name = IntegrationSupport.unique_atom("spike_auth_cluster")

    base_opts = [
      name: name,
      transport: Aerospike.Transport.Tcp,
      hosts: ["#{@host}:#{@port}"],
      namespaces: [@namespace],
      tend_trigger: :manual,
      pool_size: 2
    ]

    {:ok, sup} = Aerospike.start_link(Keyword.merge(base_opts, extra_opts))

    on_exit(fn ->
      IntegrationSupport.stop_supervisor_quietly(sup)
    end)

    name
  end

  # Idempotently provisions a non-admin user with data-plane privileges via
  # `asadm`. The built-in `admin` account ships with only the `user-admin`
  # role, which cannot read records — the driver has to authenticate as a
  # user that actually holds `read-write` for the positive-path assertion
  # to prove end-to-end reachability rather than just "login succeeded".
  defp ensure_data_user!(user, password) do
    case asadm(["manage acl create user #{user} password #{password} roles read-write"]) do
      {_out, 0} ->
        :ok

      {out, _} ->
        if out =~ "already exists" do
          :ok
        else
          raise "Failed to provision data user #{user}: #{out}"
        end
    end
  end

  defp asadm(commands) do
    args =
      Enum.flat_map(commands, fn cmd -> ["-e", cmd] end)

    System.cmd(
      "docker",
      ["exec", @container, "asadm", "-U", "admin", "-P", "admin", "--enable"] ++ args,
      stderr_to_stdout: true
    )
  end
end
