defmodule Demo.Examples.Expressions do
  @moduledoc """
  Demonstrates server-side filter expressions (regex compare on keys).


  STATUS: Not implemented — the Aerospike Elixir driver only has `Exp.from_wire/1`.
  The full expression builder API (Exp.gt/2, Exp.int_bin/1, ExpRegexCompare, etc.)
  is not yet available. This module will be implemented when the Exp builder ships.
  """

  require Logger

  def run do
    Logger.warning("  Expressions: skipped — Exp builder API not yet available")
    :skipped
  end
end
