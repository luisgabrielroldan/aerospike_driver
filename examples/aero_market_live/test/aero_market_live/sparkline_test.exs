defmodule AeroMarketLive.SparklineTest do
  use ExUnit.Case, async: true

  alias AeroMarketLive.Sparkline

  test "from_cdt_map drops nil values and returns newest prices first" do
    assert Sparkline.from_cdt_map(%{1 => 10.0, 2 => nil, 3 => 12.5}, 120) == [12.5, 10.0]
  end

  test "prepend keeps only numeric prices within the configured limit" do
    assert Sparkline.prepend([nil, 10.0, :bad, 9.5], 11.0, 3) == [11.0, 10.0, 9.5]
    assert Sparkline.prepend([nil, 10.0, :bad, 9.5], nil, 2) == [10.0, 9.5]
  end

  test "render ignores non-numeric values instead of raising" do
    assert Sparkline.render([nil, 10.0, 12.0]) =~ ~s|<svg class="spark"|
    assert Sparkline.render([nil]) == ~s|<svg class="spark" viewBox="0 0 100 30"></svg>|
  end
end
