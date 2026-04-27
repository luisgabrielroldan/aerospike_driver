defmodule AeroMarketLive.TickerConfig do
  @moduledoc """
  Static configuration for the synthetic tickers driven by the generator.

  `drift` and `volatility` are expressed per trading day. The generator
  converts them to per-tick values using `dt = minutes_per_tick / 390.0`
  (390 being the number of minutes in a US equity trading session).

  The default values are illustrative — rough orders of magnitude for the
  named equities. They are not financial data.
  """

  @enforce_keys [:symbol, :initial_price, :drift, :volatility]
  defstruct [:symbol, :initial_price, :drift, :volatility]

  @type t :: %__MODULE__{
          symbol: String.t(),
          initial_price: float(),
          drift: float(),
          volatility: float()
        }

  @doc """
  Returns the default six-symbol ticker set used by the dashboard demo.
  """
  @spec defaults() :: [t()]
  def defaults do
    [
      %__MODULE__{symbol: "AAPL", initial_price: 175.0, drift: 0.0003, volatility: 0.012},
      %__MODULE__{symbol: "MSFT", initial_price: 415.0, drift: 0.0004, volatility: 0.013},
      %__MODULE__{symbol: "NVDA", initial_price: 900.0, drift: 0.0008, volatility: 0.025},
      %__MODULE__{symbol: "TSLA", initial_price: 180.0, drift: 0.0002, volatility: 0.028},
      %__MODULE__{symbol: "GOOG", initial_price: 160.0, drift: 0.0003, volatility: 0.014},
      %__MODULE__{symbol: "AMZN", initial_price: 185.0, drift: 0.0004, volatility: 0.016}
    ]
  end
end
