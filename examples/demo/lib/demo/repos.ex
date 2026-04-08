defmodule Demo.PrimaryClusterRepo do
  @moduledoc """
  Repo bound to the primary community cluster.

  Default endpoint: `localhost:3000`.
  """

  use Aerospike.Repo, otp_app: :demo, name: :aero
end

defmodule Demo.EnterpriseRepo do
  @moduledoc """
  Repo bound to the enterprise cluster used for transaction examples.

  Default endpoint: `localhost:3100`.
  """

  use Aerospike.Repo, otp_app: :demo, name: :aero_ee
end

defmodule Demo.TlsClusterRepo do
  @moduledoc """
  Repo bound to the TLS endpoint.

  Default endpoint: `localhost:4333`.
  """

  use Aerospike.Repo, otp_app: :demo, name: :aero_tls
end

defmodule Demo.MtlsClusterRepo do
  @moduledoc """
  Repo bound to the mTLS endpoint.

  Default endpoint: `localhost:4334`.
  """

  use Aerospike.Repo, otp_app: :demo, name: :aero_mtls
end
