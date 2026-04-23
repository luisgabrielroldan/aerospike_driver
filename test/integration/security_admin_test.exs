defmodule Aerospike.Integration.SecurityAdminTest do
  @moduledoc """
  Exercises the enterprise security-admin user surface against the
  `aerospike-ee-security` profile.

  Run with:

      mix test test/integration/security_admin_test.exs --include integration --include enterprise --seed 0
  """

  use ExUnit.Case, async: false

  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Privilege
  alias Aerospike.Role
  alias Aerospike.User

  @moduletag :integration
  @moduletag :enterprise

  @host "localhost"
  @port 3200
  @namespace "test"

  setup_all do
    probe_aerospike!(@host, @port)
    :ok
  end

  setup do
    cluster = start_cluster!("admin", "admin")
    assert_security_ready!(cluster)
    {:ok, conn: cluster}
  end

  test "user lifecycle works against the secured cluster", %{conn: conn} do
    user_name = unique_name("sec_user")
    password = "pw-#{System.unique_integer([:positive, :monotonic])}"

    on_exit(fn ->
      if is_pid(Process.whereis(conn)) do
        _ = Aerospike.drop_user(conn, user_name)
      end
    end)

    assert :ok = Aerospike.create_user(conn, user_name, password, ["read"])

    assert_eventually("created user appears with the initial role", fn ->
      case Aerospike.query_user(conn, user_name) do
        {:ok, %User{name: ^user_name, roles: roles}} -> roles == ["read"]
        _ -> false
      end
    end)

    assert :ok = Aerospike.grant_roles(conn, user_name, ["read-write"])

    assert_eventually("grant_roles updates the role list", fn ->
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

    assert_eventually("query_users no longer lists the dropped user", fn ->
      case Aerospike.query_users(conn) do
        {:ok, users} -> not Enum.any?(users, &(&1.name == user_name))
        _ -> false
      end
    end)
  end

  test "self password change rotates the running cluster auth source", %{conn: conn} do
    user_name = unique_name("sec_rotate")
    initial_password = "pw-#{System.unique_integer([:positive, :monotonic])}"
    rotated_password = "pw-#{System.unique_integer([:positive, :monotonic])}"
    key = Key.new(@namespace, "spike", unique_name("security_key"))

    on_exit(fn ->
      if is_pid(Process.whereis(conn)) do
        safe_cleanup(fn -> Aerospike.delete(conn, key) end)
        safe_cleanup(fn -> Aerospike.drop_user(conn, user_name) end)
      end
    end)

    assert :ok = Aerospike.create_user(conn, user_name, initial_password, ["read-write"])

    assert_eventually("created user appears before the self-service flow starts", fn ->
      case Aerospike.query_user(conn, user_name) do
        {:ok, %User{name: ^user_name, roles: roles}} -> roles == ["read-write"]
        _ -> false
      end
    end)

    with_security_cluster(user_name, initial_password, fn user_conn ->
      assert :ok = Aerospike.change_password(user_conn, user_name, rotated_password)
      assert %{user: ^user_name, password: ^rotated_password} = Tender.auth_credentials(user_conn)
      assert {:error, %Error{code: :key_not_found}} = Aerospike.get(user_conn, key)
    end)

    with_security_cluster(user_name, rotated_password, fn rotated_conn ->
      assert {:error, %Error{code: :key_not_found}} = Aerospike.get(rotated_conn, key)
    end)
  end

  test "role lifecycle works against the secured cluster", %{conn: conn} do
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
      if is_pid(Process.whereis(conn)) do
        safe_cleanup(fn -> Aerospike.drop_role(conn, role_name) end)
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

    assert_eventually("created role appears with the initial privilege and quotas", fn ->
      case Aerospike.query_role(conn, role_name) do
        {:ok,
         %Role{
           name: ^role_name,
           privileges: privileges,
           whitelist: whitelist,
           read_quota: 100,
           write_quota: 200
         }} ->
          whitelist == ["127.0.0.1"] and Enum.member?(privileges, scoped_privilege)

        _ ->
          false
      end
    end)

    assert :ok = Aerospike.grant_privileges(conn, role_name, [granted_privilege])

    assert_eventually("grant_privileges updates the role privilege list", fn ->
      case Aerospike.query_role(conn, role_name) do
        {:ok, %Role{privileges: privileges}} ->
          Enum.member?(privileges, scoped_privilege) and
            Enum.member?(privileges, granted_privilege)

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
        flunk("security admin tests require the enterprise security profile")

      {:error, %Error{} = error} ->
        flunk("security admin setup failed: #{inspect(error)}")
    end
  end

  defp start_cluster!(user, password) do
    name = :"spike_security_admin_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["#{@host}:#{@port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 1,
        user: user,
        password: password
      )

    :ok = Tender.tend_now(name)
    assert Tender.ready?(name), "security test cluster must become ready after one tend cycle"

    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    name
  end

  defp with_security_cluster(user, password, fun)
       when is_binary(user) and is_binary(password) and is_function(fun, 1) do
    name = :"spike_security_temp_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["#{@host}:#{@port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 1,
        user: user,
        password: password
      )

    try do
      :ok = Tender.tend_now(name)
      assert Tender.ready?(name), "temporary security cluster must become ready"
      fun.(name)
    after
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end
  end

  defp unique_name(prefix) do
    "#{prefix}_#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"
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

  defp probe_aerospike!(host, port) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 1_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, reason} ->
        raise "Aerospike EE security profile not reachable at #{host}:#{port} " <>
                "(#{inspect(reason)}). Run `docker compose --profile enterprise " <>
                "up -d aerospike-ee-security` first."
    end
  end

  defp safe_cleanup(fun) when is_function(fun, 0) do
    try do
      _ = fun.()
      :ok
    catch
      :exit, _ -> :ok
    end
  end
end
