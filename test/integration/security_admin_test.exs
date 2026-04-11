defmodule Aerospike.Integration.SecurityAdminTest do
  use ExUnit.Case, async: false

  alias Aerospike.Admin.PasswordHash
  alias Aerospike.Error
  alias Aerospike.Privilege
  alias Aerospike.Role
  alias Aerospike.Tables
  alias Aerospike.User

  @moduletag :enterprise
  @moduletag :security

  @namespace "test"

  setup do
    host = System.get_env("AEROSPIKE_SECURITY_EE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_SECURITY_EE_PORT", "3200") |> String.to_integer()
    admin_user = System.get_env("AEROSPIKE_SECURITY_EE_USER", "admin")
    admin_password = System.get_env("AEROSPIKE_SECURITY_EE_PASSWORD", "admin")

    unless ee_running?(host, port) do
      flunk("""
      Security admin integration server not running on #{host}:#{port}. Provide a security-enabled
      Aerospike Enterprise node and credentials via:

          AEROSPIKE_SECURITY_EE_HOST
          AEROSPIKE_SECURITY_EE_PORT
          AEROSPIKE_SECURITY_EE_USER
          AEROSPIKE_SECURITY_EE_PASSWORD
      """)
    end

    name = :"security_admin_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      tend_interval: 60_000,
      auth_opts: [user: admin_user, credential: PasswordHash.hash(admin_password)]
    ]

    {:ok, _sup} = start_supervised({Aerospike, opts})
    await_cluster_ready(name)
    assert_security_ready!(name)

    {:ok, conn: name}
  end

  test "user lifecycle works against a secured cluster", %{conn: conn} do
    user_name = unique_name("sec_user")
    password = "pw-#{System.unique_integer([:positive, :monotonic])}"

    on_exit(fn ->
      if :ets.whereis(Tables.meta(conn)) != :undefined do
        _ = Aerospike.drop_user(conn, user_name)
      end
    end)

    assert :ok = Aerospike.create_user(conn, user_name, password, ["read"])

    assert_eventually("created user appears with initial roles", fn ->
      case Aerospike.query_user(conn, user_name) do
        {:ok, %User{name: ^user_name, roles: roles}} ->
          Enum.sort(roles) == ["read"]

        _ ->
          false
      end
    end)

    assert :ok = Aerospike.grant_roles(conn, user_name, ["read-write"])

    assert_eventually("grant_roles updates the user role set", fn ->
      case Aerospike.query_user(conn, user_name) do
        {:ok, %User{roles: roles}} -> Enum.sort(roles) == ["read", "read-write"]
        _ -> false
      end
    end)

    assert :ok = Aerospike.revoke_roles(conn, user_name, ["read"])

    assert_eventually("revoke_roles removes the revoked role", fn ->
      case Aerospike.query_user(conn, user_name) do
        {:ok, %User{roles: roles}} -> roles == ["read-write"]
        _ -> false
      end
    end)

    assert :ok = Aerospike.drop_user(conn, user_name)

    assert_eventually("dropped user disappears from query_users/2", fn ->
      case Aerospike.query_users(conn) do
        {:ok, users} -> not Enum.any?(users, &(&1.name == user_name))
        _ -> false
      end
    end)
  end

  test "role lifecycle works against a secured cluster", %{conn: conn} do
    role_name = unique_name("sec_role")

    scoped_privilege = %Privilege{
      code: :read,
      namespace: @namespace,
      set: role_name
    }

    granted_privilege = %Privilege{
      code: :read_write,
      namespace: @namespace,
      set: "#{role_name}_rw"
    }

    on_exit(fn ->
      if :ets.whereis(Tables.meta(conn)) != :undefined do
        _ = Aerospike.drop_role(conn, role_name)
      end
    end)

    assert :ok =
             Aerospike.create_role(
               conn,
               role_name,
               [scoped_privilege],
               whitelist: ["127.0.0.1"],
               read_quota: 100,
               write_quota: 200
             )

    assert_eventually("created role appears with initial privilege and quotas", fn ->
      case Aerospike.query_role(conn, role_name) do
        {:ok,
         %Role{
           name: ^role_name,
           privileges: privileges,
           whitelist: whitelist,
           read_quota: 100,
           write_quota: 200
         }} ->
          whitelist == ["127.0.0.1"] and
            Enum.member?(privileges, scoped_privilege)

        _ ->
          false
      end
    end)

    assert :ok = Aerospike.grant_privileges(conn, role_name, [granted_privilege])
    assert :ok = Aerospike.set_whitelist(conn, role_name, ["127.0.0.1", "127.0.0.2"])
    assert :ok = Aerospike.set_quotas(conn, role_name, 150, 250)

    assert_eventually("role updates are visible through query_role/3", fn ->
      case Aerospike.query_role(conn, role_name) do
        {:ok,
         %Role{
           privileges: privileges,
           whitelist: whitelist,
           read_quota: 150,
           write_quota: 250
         }} ->
          Enum.member?(privileges, scoped_privilege) and
            Enum.member?(privileges, granted_privilege) and
            Enum.sort(whitelist) == ["127.0.0.1", "127.0.0.2"]

        _ ->
          false
      end
    end)

    assert :ok = Aerospike.revoke_privileges(conn, role_name, [granted_privilege])

    assert_eventually("revoke_privileges removes the granted privilege", fn ->
      case Aerospike.query_role(conn, role_name) do
        {:ok, %Role{privileges: privileges}} ->
          Enum.member?(privileges, scoped_privilege) and
            Enum.member?(privileges, granted_privilege) == false

        _ ->
          false
      end
    end)

    assert :ok = Aerospike.drop_role(conn, role_name)

    assert_eventually("dropped role is no longer queryable", fn ->
      case Aerospike.query_role(conn, role_name) do
        {:ok, nil} -> true
        {:error, %Error{code: :invalid_role}} -> true
        _ -> false
      end
    end)
  end

  defp assert_security_ready!(conn) do
    case Aerospike.query_users(conn) do
      {:ok, _users} ->
        :ok

      {:error, %Error{code: :security_not_enabled}} ->
        flunk("security admin tests require a security-enabled Aerospike Enterprise server")

      {:error, %Error{code: :not_authenticated}} ->
        flunk(
          "security admin tests require valid AEROSPIKE_SECURITY_EE_USER/AEROSPIKE_SECURITY_EE_PASSWORD credentials"
        )

      {:error, %Error{} = error} ->
        flunk("security admin setup failed: #{inspect(error)}")
    end
  end

  defp unique_name(prefix) do
    "#{prefix}_#{System.unique_integer([:positive, :monotonic])}"
  end

  defp ee_running?(host, port) do
    case :gen_tcp.connect(~c"#{host}", port, [], 2_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        true

      _ ->
        false
    end
  end

  defp await_cluster_ready(name, timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_cluster_ready_loop(name, deadline, timeout)
  end

  defp await_cluster_ready_loop(name, deadline, timeout) do
    cond do
      match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key())) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("secured cluster not ready within #{timeout}ms")

      true ->
        Process.sleep(100)
        await_cluster_ready_loop(name, deadline, timeout)
    end
  end

  defp assert_eventually(message, fun, timeout \\ 10_000, interval \\ 200)
       when is_binary(message) and is_function(fun, 0) do
    deadline = System.monotonic_time(:millisecond) + timeout
    assert_eventually_loop(message, fun, deadline, interval)
  end

  defp assert_eventually_loop(message, fun, deadline, interval) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("condition did not become true within timeout: #{message}")

      true ->
        Process.sleep(interval)
        assert_eventually_loop(message, fun, deadline, interval)
    end
  end
end
