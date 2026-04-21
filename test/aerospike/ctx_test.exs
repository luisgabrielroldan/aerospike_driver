defmodule Aerospike.CtxTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx

  test "builds every supported context step" do
    assert Ctx.list_index(3) == {0x10, 3}
    assert Ctx.list_rank(-1) == {0x11, -1}
    assert Ctx.list_value("vip") == {0x13, "vip"}
    assert Ctx.map_index(2) == {0x20, 2}
    assert Ctx.map_rank(4) == {0x21, 4}
    assert Ctx.map_key("roles") == {0x22, "roles"}
    assert Ctx.map_value(%{"active" => true}) == {0x23, %{"active" => true}}
  end
end
