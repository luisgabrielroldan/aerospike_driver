# Security Administration

This guide covers the Phase 1 security administration APIs for secured Aerospike
Enterprise clusters: user lifecycle, role lifecycle, scoped privileges,
whitelists, and quotas.

## Requirements

- A security-enabled Aerospike Enterprise node
- Credentials for a user with security admin privileges
- A client connection started with hashed credentials in `:auth_opts`

The repository's integration suite expects these environment variables for the
secured test target:

- `AEROSPIKE_SECURITY_EE_HOST`
- `AEROSPIKE_SECURITY_EE_PORT`
- `AEROSPIKE_SECURITY_EE_USER`
- `AEROSPIKE_SECURITY_EE_PASSWORD`

The repository currently does not ship a committed secure Enterprise bootstrap.
You need to provide that server externally before running the security tests.

## Connecting As An Admin User

```elixir
alias Aerospike.Admin.PasswordHash

{:ok, _pid} =
  Aerospike.start_link(
    name: :aero_admin,
    hosts: ["127.0.0.1:3200"],
    auth_opts: [
      user: System.fetch_env!("AEROSPIKE_SECURITY_EE_USER"),
      credential: PasswordHash.hash(System.fetch_env!("AEROSPIKE_SECURITY_EE_PASSWORD"))
    ]
  )
```

`Aerospike.create_user/5` and `Aerospike.change_password/4` accept cleartext
passwords and hash them internally before sending the admin command.

## Managing Users

Create a password-authenticated user, inspect it, update roles, and remove it:

```elixir
:ok = Aerospike.create_user(:aero_admin, "ops-reader", "secret-pass", ["read"])

{:ok, %Aerospike.User{name: "ops-reader", roles: roles}} =
  Aerospike.query_user(:aero_admin, "ops-reader")

true = Enum.sort(roles) == ["read"]

:ok = Aerospike.grant_roles(:aero_admin, "ops-reader", ["read-write"])
:ok = Aerospike.revoke_roles(:aero_admin, "ops-reader", ["read"])

{:ok, users} = Aerospike.query_users(:aero_admin)
true = Enum.any?(users, &(&1.name == "ops-reader"))

:ok = Aerospike.drop_user(:aero_admin, "ops-reader")
```

Create a PKI-authenticated user when the secured cluster is configured for PKI
user authentication:

```elixir
:ok = Aerospike.create_pki_user(:aero_admin, "CN=reporting-client", ["read"])
```

Change a password for the current authenticated user or another managed user:

```elixir
:ok = Aerospike.change_password(:aero_admin, "ops-reader", "rotated-pass")
```

When the authenticated client changes its own password, the running client
rotates its in-memory credential source so future reconnects in the same
process use the new password. A restarted client still uses whatever
credentials the application passes in `:auth_opts`, so the application or
operator must update startup configuration separately.

`Aerospike.query_user/3` returns `{:ok, nil}` when the user is absent.

## Managing Roles

Roles carry privileges plus optional network and rate-limit constraints.

```elixir
alias Aerospike.Privilege

scoped_read = %Privilege{
  code: :read,
  namespace: "test",
  set: "reports"
}

:ok =
  Aerospike.create_role(
    :aero_admin,
    "report_reader",
    [scoped_read],
    whitelist: ["10.0.0.0/24"],
    read_quota: 100,
    write_quota: 0
  )

{:ok, %Aerospike.Role{} = role} = Aerospike.query_role(:aero_admin, "report_reader")
["10.0.0.0/24"] = role.whitelist
100 = role.read_quota
```

Grant or revoke additional privileges and update the role controls:

```elixir
:ok =
  Aerospike.grant_privileges(
    :aero_admin,
    "report_reader",
    [%Privilege{code: :read_write, namespace: "test", set: "scratch"}]
  )

:ok = Aerospike.set_whitelist(:aero_admin, "report_reader", ["10.0.0.0/24", "10.0.1.0/24"])
:ok = Aerospike.set_quotas(:aero_admin, "report_reader", 150, 25)

:ok =
  Aerospike.revoke_privileges(
    :aero_admin,
    "report_reader",
    [%Privilege{code: :read_write, namespace: "test", set: "scratch"}]
  )

:ok = Aerospike.drop_role(:aero_admin, "report_reader")
```

`Aerospike.query_role/3` returns `{:ok, nil}` when the role is absent.

## Result Shapes

- `Aerospike.query_user/3` and `Aerospike.query_users/2` return `%Aerospike.User{}`
  values with `:name`, `:roles`, `:read_info`, `:write_info`, and
  `:connections_in_use`.
- `Aerospike.query_role/3` and `Aerospike.query_roles/2` return `%Aerospike.Role{}`
  values with `:name`, `:privileges`, `:whitelist`, `:read_quota`, and
  `:write_quota`.
- Privileges are represented as `%Aerospike.Privilege{code, namespace, set}`.

## Running The Security Integration Suite

Run the full project checks:

```bash
mix test.all
```

Or run the focused security suite:

```bash
mix test --only security test/integration/security_admin_test.exs
```

Those commands still require a reachable security-enabled Enterprise node that
matches the `AEROSPIKE_SECURITY_EE_*` environment contract above.
