defmodule Aerospike.CtxTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Exp
  alias Aerospike.Protocol.MessagePack

  test "builds every supported context step" do
    assert {0x04, %Exp{}} = Ctx.all_children()

    assert Ctx.all_children_with_filter(Exp.gt(Exp.int_loop_var(:value), Exp.int(10))) |> elem(0) ==
             0x04

    assert Ctx.list_index(3) == {0x10, 3}
    assert Ctx.list_index_create(3) == {0x50, 3}
    assert Ctx.list_index_create(3, :unordered, true) == {0x90, 3}
    assert Ctx.list_index_create(3, :ordered) == {0xD0, 3}
    assert Ctx.list_rank(-1) == {0x11, -1}
    assert Ctx.list_value("vip") == {0x13, "vip"}
    assert Ctx.map_index(2) == {0x20, 2}
    assert Ctx.map_rank(4) == {0x21, 4}
    assert Ctx.map_key("roles") == {0x22, "roles"}
    assert Ctx.map_key_create("roles") == {0x62, "roles"}
    assert Ctx.map_key_create("roles", :key_ordered) == {0xA2, "roles"}
    assert Ctx.map_key_create("roles", :key_value_ordered) == {0xE2, "roles"}
    assert Ctx.map_value(%{"active" => true}) == {0x23, %{"active" => true}}
  end

  test "serializes context paths to bytes and Base64" do
    filter = Exp.gt(Exp.int_loop_var(:value), Exp.int(10))
    ctx = [Ctx.map_key("items"), Ctx.all_children_with_filter(filter), Ctx.map_key("price")]

    assert MessagePack.unpack!(Ctx.to_bytes(ctx)) == [
             0x22,
             "items",
             0x04,
             filter.wire,
             0x22,
             "price"
           ]

    assert Ctx.from_bytes(Ctx.to_bytes(ctx)) == ctx
    assert Ctx.from_base64(Ctx.to_base64(ctx)) == ctx
  end

  test "rejects serialized contexts with an unmatched id" do
    bytes = MessagePack.pack!([0x22])

    assert_raise ArgumentError, ~r/context bytes must contain id\/value pairs/, fn ->
      Ctx.from_bytes(bytes)
    end
  end
end
