defmodule AeroMarketLive.Sparkline do
  @moduledoc """
  Normalizes market tick buffers and renders sparkline SVGs.
  """

  @empty_svg ~s|<svg class="spark" viewBox="0 0 100 30"></svg>|

  @doc """
  Converts a CDT map response into a newest-first numeric price buffer.
  """
  @spec from_cdt_map(map() | nil, pos_integer()) :: [number()]
  def from_cdt_map(nil, _limit), do: []

  def from_cdt_map(m, limit) when is_map(m) and is_integer(limit) and limit > 0 do
    m
    |> Enum.sort_by(fn {k, _} -> k end)
    |> Enum.map(fn {_, v} -> v end)
    |> Enum.filter(&is_number/1)
    |> Enum.reverse()
    |> Enum.take(limit)
  end

  @doc """
  Prepends a numeric price to a newest-first buffer.
  """
  @spec prepend([term()], term(), pos_integer()) :: [number()]
  def prepend(buf, price, limit) when is_number(price) and is_integer(limit) and limit > 0 do
    [price | buf |> Enum.filter(&is_number/1) |> Enum.take(limit - 1)]
  end

  def prepend(buf, _price, limit) when is_integer(limit) and limit > 0 do
    buf |> Enum.filter(&is_number/1) |> Enum.take(limit)
  end

  @doc """
  Renders a newest-first price buffer as an inline SVG.
  """
  @spec render([term()]) :: String.t()
  def render(buf) do
    buf
    |> Enum.filter(&is_number/1)
    |> Enum.reverse()
    |> render_prices()
  end

  defp render_prices([]), do: @empty_svg

  defp render_prices(prices) do
    n = length(prices)
    lo = Enum.min(prices)
    hi = Enum.max(prices)
    spread = hi - lo

    points =
      prices
      |> Enum.with_index()
      |> Enum.map(fn {p, i} ->
        x = if n > 1, do: i / (n - 1) * 100.0, else: 50.0
        y = if spread > 0, do: (1.0 - (p - lo) / spread) * 28.0 + 1.0, else: 15.0
        "#{Float.round(x, 2)},#{Float.round(y, 2)}"
      end)
      |> Enum.join(" ")

    ~s|<svg class="spark" viewBox="0 0 100 30" preserveAspectRatio="none"><polyline points="#{points}" /></svg>|
  end
end
