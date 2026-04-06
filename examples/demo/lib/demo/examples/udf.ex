defmodule Demo.Examples.Udf do
  @moduledoc """
  Demonstrates UDF (User Defined Function) registration and execution.


  STATUS: Not implemented — the Aerospike Elixir driver does not yet support
  UDF registration or server-side execution. This module will be implemented
  when the UDF API is available.
  """

  require Logger

  def run do
    Logger.warning("  UDF: skipped — UDF register/execute API not yet available")
    :skipped
  end
end
