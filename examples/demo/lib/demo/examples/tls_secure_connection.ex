defmodule Demo.Examples.TlsSecureConnection do
  @moduledoc """
  Demonstrates TLS-secured connections to Aerospike.


  STATUS: Not implemented as a runnable example — TLS support exists in the driver
  (via `tls: true` and `tls_opts:` in start_link), but requires a TLS-configured
  Aerospike server and valid certificates to test.
  """

  require Logger

  def run do
    Logger.warning("  TlsSecureConnection: skipped — requires TLS-configured server")
    :skipped
  end
end
