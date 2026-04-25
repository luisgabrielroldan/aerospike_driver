defmodule Aerospike.Op.MapTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Op.Map, as: MapOp
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  defp payload(op), do: MessagePack.unpack!(op.data)

  defp assert_op(op, op_type, expected_payload) do
    assert op.op_type == op_type
    assert op.bin_name == "items"
    assert op.map_cdt
    assert payload(op) == expected_payload
  end

  test "return type helpers expose map selector constants" do
    assert MapOp.return_none() == 0
    assert MapOp.return_index() == 1
    assert MapOp.return_reverse_index() == 2
    assert MapOp.return_rank() == 3
    assert MapOp.return_reverse_rank() == 4
    assert MapOp.return_count() == 5
    assert MapOp.return_key() == 6
    assert MapOp.return_value() == 7
    assert MapOp.return_key_value() == 8
    assert MapOp.return_exists() == 13
  end

  test "modify builders encode reference op codes and payloads" do
    cases = [
      {MapOp.set_policy("items", 1), [64, 1]},
      {MapOp.put("items", "a", 1), [67, "a", 1, 0]},
      {MapOp.put("items", "a", 1, policy: %{attr: 1}), [67, "a", 1, 1]},
      {MapOp.put("items", "a", 1, policy: %{attr: 1, flags: 2}), [67, "a", 1, 1, 2]},
      {MapOp.put_items("items", %{"a" => 1}), [68, %{"a" => 1}, 0]},
      {MapOp.put_items("items", %{"a" => 1}, policy: %{attr: 1, flags: 2}),
       [68, %{"a" => 1}, 1, 2]},
      {MapOp.increment("items", "count", 5), [73, "count", 5, 0]},
      {MapOp.increment("items", "count", 1.5, policy: %{attr: 1}), [73, "count", 1.5, 1]},
      {MapOp.decrement("items", "count", 2), [74, "count", 2, 0]},
      {MapOp.clear("items"), [75]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_cdt_modify(), expected_payload)
    end)
  end

  test "remove-by selectors encode reference op codes and payloads" do
    cases = [
      {MapOp.remove_by_key("items", "a"), [76, 6, "a"]},
      {MapOp.remove_by_index("items", 0), [77, 6, 0]},
      {MapOp.remove_by_rank("items", -1), [79, 7, -1]},
      {MapOp.remove_by_key_list("items", ["a", "b"]), [81, 6, ["a", "b"]]},
      {MapOp.remove_by_value("items", 1), [82, 7, 1]},
      {MapOp.remove_by_value_list("items", [1, 2]), [83, 7, [1, 2]]},
      {MapOp.remove_by_key_range("items", "a", "z"), [84, 6, "a", "z"]},
      {MapOp.remove_by_index_range_from("items", 1), [85, 6, 1]},
      {MapOp.remove_by_index_range("items", 1, 2), [85, 6, 1, 2]},
      {MapOp.remove_by_value_range("items", 1, 9), [86, 7, 1, 9]},
      {MapOp.remove_by_rank_range_from("items", -2), [87, 7, -2]},
      {MapOp.remove_by_rank_range("items", -2, 2), [87, 7, -2, 2]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_cdt_modify(), expected_payload)
    end)
  end

  test "read builders encode reference op codes and payloads" do
    cases = [
      {MapOp.size("items"), [96]},
      {MapOp.get_by_key("items", "a"), [97, 7, "a"]},
      {MapOp.get_by_index("items", 0), [98, 8, 0]},
      {MapOp.get_by_rank("items", -1), [100, 8, -1]},
      {MapOp.get_by_value("items", 1), [102, 8, 1]},
      {MapOp.get_by_key_range("items", "a", "z"), [103, 8, "a", "z"]},
      {MapOp.get_by_index_range_from("items", 1), [104, 8, 1]},
      {MapOp.get_by_index_range("items", 1, 2), [104, 8, 1, 2]},
      {MapOp.get_by_value_range("items", 1, 9), [105, 8, 1, 9]},
      {MapOp.get_by_rank_range_from("items", -2), [106, 8, -2]},
      {MapOp.get_by_rank_range("items", -2, 2), [106, 8, -2, 2]},
      {MapOp.get_by_key_list("items", ["a", "b"]), [107, 8, ["a", "b"]]},
      {MapOp.get_by_value_list("items", [1, 2]), [108, 8, [1, 2]]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_cdt_read(), expected_payload)
    end)
  end

  test "return_type option overrides selector defaults" do
    assert payload(MapOp.get_by_index("items", 1, return_type: MapOp.return_value())) == [
             98,
             7,
             1
           ]

    assert payload(MapOp.remove_by_key("items", "a", return_type: MapOp.return_none())) == [
             76,
             0,
             "a"
           ]

    assert payload(MapOp.remove_by_rank("items", -1, return_type: MapOp.return_key())) == [
             79,
             6,
             -1
           ]
  end

  test "ctx option is encoded through the CDT helper path" do
    ctx = [Ctx.map_key("outer"), Ctx.list_index(0)]
    op = MapOp.put("items", "a", 1, ctx: ctx)

    assert payload(op) == [
             255,
             [0x22, "outer", 0x10, 0],
             [67, "a", 1, 0]
           ]
  end
end
