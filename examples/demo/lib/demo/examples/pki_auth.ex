defmodule Demo.Examples.PkiAuth do
  @moduledoc """
  Demonstrates PKI (certificate-based) mutual TLS authentication.


  STATUS: Not implemented as a runnable example — requires an Aerospike Enterprise
  server configured with PKI authentication and valid client/CA certificates.
  """

  require Logger

  def run do
    Logger.warning("  PkiAuth: skipped — requires Enterprise server with PKI auth")
    :skipped
  end
end
