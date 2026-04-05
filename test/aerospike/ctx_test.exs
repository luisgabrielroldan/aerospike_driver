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
end
