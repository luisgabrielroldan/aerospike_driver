defmodule Aerospike.CtxTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Protocol.CDT
  alias Aerospike.Protocol.MessagePack

  test "context steps encode inside CDT payload" do
    ctx = [Ctx.map_key("outer"), Ctx.list_index(0)]
    op = CDT.list_modify_op("bin", 1, ["x"], ctx)
    decoded = MessagePack.unpack!(op.data)

    assert decoded == [
             0xFF,
             [0x22, <<3, "outer">>, 0x10, 0],
             [1, <<3, "x">>]
           ]
  end

  test "list_index returns {0x10, index}" do
    assert Ctx.list_index(5) == {0x10, 5}
    assert Ctx.list_index(-1) == {0x10, -1}
  end

  test "list_rank returns {0x11, rank}" do
    assert Ctx.list_rank(0) == {0x11, 0}
    assert Ctx.list_rank(2) == {0x11, 2}
  end

  test "list_value returns {0x13, value}" do
    assert Ctx.list_value("hello") == {0x13, "hello"}
    assert Ctx.list_value(42) == {0x13, 42}
  end

  test "map_index returns {0x20, index}" do
    assert Ctx.map_index(0) == {0x20, 0}
    assert Ctx.map_index(3) == {0x20, 3}
  end

  test "map_rank returns {0x21, rank}" do
    assert Ctx.map_rank(0) == {0x21, 0}
    assert Ctx.map_rank(1) == {0x21, 1}
  end

  test "map_key returns {0x22, key}" do
    assert Ctx.map_key("name") == {0x22, "name"}
  end

  test "map_value returns {0x23, value}" do
    assert Ctx.map_value("target") == {0x23, "target"}
    assert Ctx.map_value(99) == {0x23, 99}
  end
end
