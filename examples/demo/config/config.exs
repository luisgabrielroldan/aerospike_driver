import Config

config :demo, Demo.PrimaryClusterRepo,
  transport: Aerospike.Transport.Tcp,
  hosts: ["localhost:3000"],
  namespaces: ["test"],
  pool_size: 4

config :demo, Demo.EnterpriseRepo,
  transport: Aerospike.Transport.Tcp,
  hosts: ["localhost:3100"],
  namespaces: ["test"],
  pool_size: 2,
  connect_opts: [connect_timeout_ms: 5_000],
  tend_interval_ms: 60_000

config :demo, Demo.SecurityRepo,
  transport: Aerospike.Transport.Tcp,
  hosts: [System.get_env("AEROSPIKE_SECURITY_HOST", "localhost:3200")],
  namespaces: ["test"],
  user: System.get_env("AEROSPIKE_SECURITY_USER", "admin"),
  password: System.get_env("AEROSPIKE_SECURITY_PASSWORD", "admin"),
  pool_size: 2,
  connect_opts: [connect_timeout_ms: 5_000],
  tend_interval_ms: 60_000

config :demo, Demo.TlsClusterRepo,
  transport: Aerospike.Transport.Tls,
  hosts: ["localhost:4333"],
  namespaces: ["test"],
  connect_opts: [
    tls_name: "localhost",
    tls_verify: :verify_none
  ],
  pool_size: 1

config :demo, Demo.MtlsClusterRepo,
  transport: Aerospike.Transport.Tls,
  hosts: ["localhost:4334"],
  namespaces: ["test"],
  connect_opts: [
    tls_name: "localhost",
    tls_verify: :verify_none
  ],
  pool_size: 1
