defmodule Demo.Examples.SecurityAdmin do
  @moduledoc """
  Demonstrates Enterprise security administration helpers.

  The example uses the local security-enabled Enterprise service on
  `localhost:3200` by default. Override the endpoint and credentials with
  `AEROSPIKE_SECURITY_HOST`, `AEROSPIKE_SECURITY_USER`, and
  `AEROSPIKE_SECURITY_PASSWORD`.
  """

  require Logger

  alias Aerospike.Error
  alias Aerospike.Privilege
  alias Aerospike.Role
  alias Aerospike.User

  @repo Demo.SecurityRepo
  @namespace "test"
  @call_opts [timeout: 5_000, pool_checkout_timeout: 2_000]
  @skip_codes [
    :security_not_enabled,
    :security_not_supported,
    :not_authenticated,
    :role_violation
  ]
  @quota_unsupported_codes [:quotas_not_enabled, :invalid_quota]

  def run do
    case security_ready?() do
      :ok ->
        run_security_admin()

      {:skip, reason} ->
        Logger.warning("  SecurityAdmin: skipped -- #{reason}")
        :skipped
    end
  end

  defp security_ready? do
    case @repo.query_users(@call_opts) do
      {:ok, _users} ->
        :ok

      {:error, :cluster_not_ready} ->
        {:skip, "security endpoint not reachable on #{security_endpoint()}"}

      {:error, %Error{code: code}} when code in @skip_codes ->
        {:skip, "security admin unavailable (#{code})"}

      {:error, %Error{} = error} ->
        {:skip, Exception.message(error)}

      {:error, reason} ->
        {:skip, inspect(reason)}
    end
  end

  defp run_security_admin do
    names = unique_names()

    cleanup(names)

    try do
      quota_support = create_role(names.role)
      update_role(names.role, quota_support)
      manage_password_user(names.user, names.password, names.role)
      manage_pki_user(names.pki_user)
      :ok = bang(@repo.drop_role(names.role, @call_opts))
      assert_role_absent(names.role)
      cleanup(names)
      :ok
    after
      cleanup(names)
    end
  end

  defp create_role(role_name) do
    Logger.info("  Creating scoped role #{role_name}...")

    privilege = read_privilege(role_name)

    case @repo.create_role(role_name, [privilege],
           whitelist: ["127.0.0.1"],
           read_quota: 100,
           write_quota: 200
         ) do
      :ok ->
        assert_role(role_name, fn %Role{} = role ->
          role.whitelist == ["127.0.0.1"] and role.read_quota == 100 and
            role.write_quota == 200 and Enum.member?(role.privileges, privilege)
        end)

        :quotas_available

      {:error, %Error{code: code}} when code in @quota_unsupported_codes ->
        Logger.warning(
          "    Quotas not available on this server; continuing without quota checks."
        )

        :ok = bang(@repo.create_role(role_name, [privilege], whitelist: ["127.0.0.1"]))

        assert_role(role_name, fn %Role{} = role ->
          role.whitelist == ["127.0.0.1"] and Enum.member?(role.privileges, privilege)
        end)

        :quotas_unavailable

      result ->
        bang(result)
    end
  end

  defp update_role(role_name, quota_support) do
    read = read_privilege(role_name)
    write = write_privilege(role_name)

    :ok = bang(@repo.grant_privileges(role_name, [write], @call_opts))

    assert_role(role_name, fn %Role{privileges: privileges} ->
      Enum.member?(privileges, read) and Enum.member?(privileges, write)
    end)

    :ok = bang(@repo.set_whitelist(role_name, ["127.0.0.1", "10.0.0.0/24"], @call_opts))

    assert_role(role_name, fn %Role{whitelist: whitelist} ->
      Enum.sort(whitelist) == ["10.0.0.0/24", "127.0.0.1"]
    end)

    :ok = bang(@repo.set_whitelist(role_name, [], @call_opts))
    assert_role(role_name, &match?(%Role{whitelist: []}, &1))

    maybe_update_quotas(role_name, quota_support)

    :ok = bang(@repo.revoke_privileges(role_name, [write], @call_opts))

    assert_role(role_name, fn %Role{privileges: privileges} ->
      Enum.member?(privileges, read) and not Enum.member?(privileges, write)
    end)
  end

  defp maybe_update_quotas(role_name, :quotas_available) do
    :ok = bang(@repo.set_quotas(role_name, 25, 50, @call_opts))
    assert_role(role_name, &match?(%Role{read_quota: 25, write_quota: 50}, &1))

    :ok = bang(@repo.set_quotas(role_name, 0, 0, @call_opts))
    assert_role(role_name, &match?(%Role{read_quota: 0, write_quota: 0}, &1))
  end

  defp maybe_update_quotas(_role_name, :quotas_unavailable), do: :ok

  defp manage_password_user(user_name, password, role_name) do
    Logger.info("  Creating password user #{user_name}...")

    :ok = bang(@repo.create_user(user_name, password, [role_name], @call_opts))

    assert_user(user_name, fn %User{roles: roles} ->
      roles == [role_name]
    end)

    :ok = bang(@repo.grant_roles(user_name, ["read"], @call_opts))

    assert_user(user_name, fn %User{roles: roles} ->
      Enum.sort(roles) == Enum.sort([role_name, "read"])
    end)

    :ok = bang(@repo.revoke_roles(user_name, [role_name], @call_opts))

    assert_user(user_name, fn %User{roles: roles} ->
      roles == ["read"]
    end)

    :ok = bang(@repo.drop_user(user_name, @call_opts))
    assert_user_absent(user_name)
  end

  defp manage_pki_user(user_name) do
    Logger.info("  Creating PKI user #{user_name}...")

    case @repo.create_pki_user(user_name, ["read"], @call_opts) do
      :ok ->
        assert_user(user_name, fn %User{roles: roles} -> roles == ["read"] end)
        :ok = bang(@repo.drop_user(user_name, @call_opts))
        assert_user_absent(user_name)

      {:error, %Error{code: :invalid_argument} = error} ->
        Logger.warning("    PKI user setup skipped: #{error.message}")

      {:error, %Error{code: code}} when code in @skip_codes ->
        Logger.warning("    PKI user setup skipped: #{code}")

      result ->
        bang(result)
    end
  end

  defp cleanup(%{user: user_name, pki_user: pki_user, role: role_name}) do
    _ = @repo.drop_user(user_name, @call_opts)
    _ = @repo.drop_user(pki_user, @call_opts)
    _ = @repo.drop_role(role_name, @call_opts)
    :ok
  end

  defp assert_user(user_name, predicate) do
    assert_eventually("user #{user_name}", fn ->
      case @repo.query_user(user_name, @call_opts) do
        {:ok, %User{name: ^user_name} = user} -> predicate.(user)
        _ -> false
      end
    end)
  end

  defp assert_user_absent(user_name) do
    assert_eventually("user #{user_name} absent", fn ->
      case @repo.query_users(@call_opts) do
        {:ok, users} -> not Enum.any?(users, &(&1.name == user_name))
        _ -> false
      end
    end)
  end

  defp assert_role(role_name, predicate) do
    assert_eventually("role #{role_name}", fn ->
      case @repo.query_role(role_name, @call_opts) do
        {:ok, %Role{name: ^role_name} = role} -> predicate.(role)
        _ -> false
      end
    end)
  end

  defp assert_role_absent(role_name) do
    assert_eventually("role #{role_name} absent", fn ->
      case @repo.query_role(role_name, @call_opts) do
        {:ok, nil} -> true
        {:error, %Error{code: :invalid_role}} -> true
        _ -> false
      end
    end)
  end

  defp assert_eventually(description, predicate),
    do: assert_eventually(description, predicate, 20)

  defp assert_eventually(description, _predicate, 0) do
    raise "Timed out waiting for #{description}"
  end

  defp assert_eventually(description, predicate, attempts) do
    if predicate.() do
      :ok
    else
      Process.sleep(100)
      assert_eventually(description, predicate, attempts - 1)
    end
  end

  defp bang(:ok), do: :ok

  defp bang({:error, %Error{} = error}) do
    raise "Security admin command failed: #{Exception.message(error)}"
  end

  defp bang({:error, reason}) do
    raise "Security admin command failed: #{inspect(reason)}"
  end

  defp read_privilege(role_name) do
    %Privilege{code: :read, namespace: @namespace, set: role_name}
  end

  defp write_privilege(role_name) do
    %Privilege{code: :write, namespace: @namespace, set: "#{role_name}_writes"}
  end

  defp unique_names do
    suffix =
      System.unique_integer([:positive])
      |> Integer.to_string(36)
      |> String.downcase()

    %{
      role: "demo_security_role_#{suffix}",
      user: "demo_security_user_#{suffix}",
      pki_user: "demo_security_pki_#{suffix}",
      password: "demo-security-password-#{suffix}"
    }
  end

  defp security_endpoint do
    :demo
    |> Application.fetch_env!(@repo)
    |> Keyword.fetch!(:hosts)
    |> List.first()
  end
end
