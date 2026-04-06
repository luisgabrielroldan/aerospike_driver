defmodule Demo.Examples.GeojsonQuery do
  @moduledoc """
  Demonstrates GeoJSON queries using secondary indexes and regions-contain-point filters.


  STATUS: Not implemented — the Aerospike Elixir driver does not yet support
  query, secondary index, or GeoJSON operations. This module will be implemented
  when available.
  """

  require Logger

  def run do
    Logger.warning("  GeojsonQuery: skipped — query/secondary index API not yet available")
    :skipped
  end
end
