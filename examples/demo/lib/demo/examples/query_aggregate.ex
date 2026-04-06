defmodule Demo.Examples.QueryAggregate do
  @moduledoc """
  Demonstrates query aggregation using UDFs (average, sum).


  STATUS: Not implemented — the Aerospike Elixir driver does not yet support
  query or UDF aggregation operations. This module will be implemented when available.
  """

  require Logger

  def run do
    Logger.warning("  QueryAggregate: skipped — query/aggregation API not yet available")
    :skipped
  end
end
