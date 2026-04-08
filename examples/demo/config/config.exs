import Config

config :demo, Demo.PrimaryClusterRepo,
  hosts: ["localhost:3000"],
  pool_size: 4

config :demo, Demo.EnterpriseRepo,
  hosts: ["localhost:3100"],
  pool_size: 2,
  connect_timeout: 5_000,
  tend_interval: 60_000

config :demo, Demo.TlsClusterRepo,
  hosts: ["localhost:4333"],
  tls: true,
  tls_opts: [verify: :verify_none],
  pool_size: 1

config :demo, Demo.MtlsClusterRepo,
  hosts: ["localhost:4334"],
  tls: true,
  tls_opts: [verify: :verify_none],
  pool_size: 1
