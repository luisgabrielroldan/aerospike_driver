defmodule AeroMarketLive.Aero do
  @moduledoc """
  Accessor for the supervised Aerospike connection.

  Call sites pass `AeroMarketLive.Aero.conn/0` to `Aerospike` functions
  rather than hard-coding the registered name.
  """

  @conn __MODULE__

  @spec conn() :: atom()
  def conn, do: @conn
end
