# Security And XDR

Security administration helpers require Aerospike Enterprise with security
enabled and a cluster connection authenticated with the needed admin
privileges. The local Enterprise security profile exercises user lifecycle,
PKI users, password rotation, role privileges, whitelists, and quotas.

## Start An Authenticated Client

Pass credentials at cluster startup.

```elixir
{:ok, _pid} =
  Aerospike.start_link(
    name: :aerospike,
    transport: Aerospike.Transport.Tcp,
    hosts: ["127.0.0.1:3200"],
    namespaces: ["test"],
    user: "admin",
    password: "admin",
    pool_size: 2
  )
```

TLS uses the TLS transport and transport options under `connect_opts`.
Username/password auth remains top-level cluster configuration.

```elixir
{:ok, _pid} =
  Aerospike.start_link(
    name: :aerospike_tls,
    transport: Aerospike.Transport.Tls,
    hosts: ["aerospike.example.com:4333"],
    namespaces: ["test"],
    user: "svc_ingest",
    password: System.fetch_env!("AEROSPIKE_PASSWORD"),
    pool_size: 4,
    connect_opts: [
      tls_name: "aerospike.example.com",
      tls_cacertfile: "/etc/ssl/aerospike/ca.crt",
      connect_timeout_ms: 5_000,
      info_timeout: 5_000
    ]
  )
```

For mTLS or PKI deployments, add client certificate and key files:

```elixir
connect_opts: [
  tls_name: "aerospike.example.com",
  tls_cacertfile: "/etc/ssl/aerospike/ca.crt",
  tls_certfile: "/etc/ssl/aerospike/client.crt",
  tls_keyfile: "/etc/ssl/aerospike/client.key"
]
```

`tls_verify: :verify_peer` is the default. Use `tls_verify: :verify_none` only
for local test environments.

## Users

Create password-authenticated users with role names.

```elixir
:ok =
  Aerospike.create_user(
    :aerospike,
    "svc_ingest",
    "replace-this-password",
    ["read-write"]
  )

{:ok, user} = Aerospike.query_user(:aerospike, "svc_ingest")
user.roles

:ok = Aerospike.grant_roles(:aerospike, "svc_ingest", ["udf-admin"])
:ok = Aerospike.revoke_roles(:aerospike, "svc_ingest", ["udf-admin"])
:ok = Aerospike.drop_user(:aerospike, "svc_ingest")
```

Create PKI users when TLS certificate authentication is configured on the
server:

```elixir
:ok = Aerospike.create_pki_user(:aerospike, "svc_pki_ingest", ["read"])
```

`change_password/4` rotates the running cluster's in-memory credentials when
the changed user is the same user configured on that cluster.

## Roles And Privileges

Privileges are `%Aerospike.Privilege{}` structs. Global privileges leave
`:namespace` and `:set` as `nil`; data privileges may narrow scope.

```elixir
read_profiles = %Aerospike.Privilege{
  code: :read,
  namespace: "test",
  set: "profiles"
}

write_events = %Aerospike.Privilege{
  code: :write,
  namespace: "test",
  set: "events"
}

:ok =
  Aerospike.create_role(:aerospike, "ingest_limited", [read_profiles],
    whitelist: ["127.0.0.1"],
    read_quota: 1_000,
    write_quota: 500
  )

:ok = Aerospike.grant_privileges(:aerospike, "ingest_limited", [write_events])
{:ok, role} = Aerospike.query_role(:aerospike, "ingest_limited")
role.privileges
```

Whitelist and quota limits can be changed independently. An empty whitelist
clears the role whitelist; `0` clears an individual quota when the server is
configured to support quotas.

```elixir
:ok =
  Aerospike.set_whitelist(:aerospike, "ingest_limited", [
    "10.0.0.0/8",
    "192.168.10.20"
  ])

:ok = Aerospike.set_whitelist(:aerospike, "ingest_limited", [])
:ok = Aerospike.set_quotas(:aerospike, "ingest_limited", 0, 0)
:ok = Aerospike.drop_role(:aerospike, "ingest_limited")
```

## XDR Filters

`set_xdr_filter/4` sets or clears one Enterprise XDR expression filter for a
datacenter and namespace. This requires XDR to be configured on the target
Enterprise server. The checked-in local profiles do not configure an XDR
topology, so XDR validation is opt-in.

```elixir
filter =
  Aerospike.Exp.eq(
    Aerospike.Exp.int_bin("replicate"),
    Aerospike.Exp.int(1)
  )

:ok = Aerospike.set_xdr_filter(:aerospike, "dc-west", "test", filter)
:ok = Aerospike.set_xdr_filter(:aerospike, "dc-west", "test", nil)
```

Passing `nil` clears the current filter. Community Edition or Enterprise
servers without XDR configured may reject the command after local validation.
