defmodule Demo.Examples.TxnConcurrent do
  @moduledoc """
  Demonstrates multi-record transactions with concurrent operations.


  STATUS: Not implemented — the Aerospike Elixir driver does not yet support
  multi-record transactions (MRT). This module will be implemented when the
  transaction API is available.
  """

  require Logger

  def run do
    Logger.warning("  TxnConcurrent: skipped — transaction API not yet available")
    :skipped
  end
end
