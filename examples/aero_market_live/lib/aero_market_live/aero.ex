defmodule AeroMarketLive.Aero do
  @moduledoc """
  Application-owned Aerospike facade for the market demo.
  """

  use Aerospike.Repo, otp_app: :aero_market_live
end
