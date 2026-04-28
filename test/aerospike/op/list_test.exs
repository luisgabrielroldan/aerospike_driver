defmodule Aerospike.Op.ListTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Op.List, as: ListOp
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  defp payload(op), do: MessagePack.unpack!(op.data)

  defp assert_op(op, op_type, expected_payload) do
    assert op.op_type == op_type
    assert op.bin_name == "items"
    assert payload(op) == expected_payload
  end

  test "return type helpers expose list selector constants" do
    assert ListOp.order_unordered() == 0
    assert ListOp.order_ordered() == 1
    assert ListOp.write_default() == 0
    assert ListOp.write_add_unique() == 1
    assert ListOp.write_insert_bounded() == 2
    assert ListOp.write_no_fail() == 4
    assert ListOp.write_partial() == 8
    assert ListOp.return_none() == 0
    assert ListOp.return_index() == 1
    assert ListOp.return_reverse_index() == 2
    assert ListOp.return_rank() == 3
    assert ListOp.return_reverse_rank() == 4
    assert ListOp.return_count() == 5
    assert ListOp.return_value() == 7
    assert ListOp.return_exists() == 13
    assert ListOp.return_inverted() == 0x10000
  end

  test "modify builders encode reference op codes and payloads" do
    cases = [
      {ListOp.set_type("items", :ordered, :default), [0, 1, 0]},
      {ListOp.create("items", :ordered), [0, 1]},
      {ListOp.create("items", :ordered, persist_index: true), [0, 0x11]},
      {ListOp.set_order("items", :ordered), [0, 1]},
      {ListOp.set_order("items", :ordered, persist_index: true), [0, 0x11]},
      {ListOp.append("items", "a"), [1, "a"]},
      {ListOp.append("items", "a", policy: [order: :ordered, flags: :insert_bounded]),
       [1, "a", 1, 2]},
      {ListOp.append_items("items", ["a", "b"]), [2, ["a", "b"]]},
      {ListOp.append_items("items", ["a"], policy: [order: :ordered, flags: [:insert_bounded]]),
       [2, ["a"], 1, 2]},
      {ListOp.insert("items", 0, "a"), [3, 0, "a"]},
      {ListOp.insert("items", 0, "a", policy: [order: :ordered, flags: :insert_bounded]),
       [3, 0, "a", 1, 2]},
      {ListOp.insert_items("items", 0, ["a"]), [4, 0, ["a"]]},
      {ListOp.insert_items("items", 0, ["a"], policy: [order: :ordered, flags: :insert_bounded]),
       [4, 0, ["a"], 1, 2]},
      {ListOp.pop("items", -1), [5, -1]},
      {ListOp.pop_range("items", 0, 2), [6, 0, 2]},
      {ListOp.pop_range_from("items", 1), [6, 1]},
      {ListOp.remove("items", 0), [7, 0]},
      {ListOp.remove_range("items", 0, 2), [8, 0, 2]},
      {ListOp.remove_range_from("items", 1), [8, 1]},
      {ListOp.set("items", 0, "a"), [9, 0, "a"]},
      {ListOp.set("items", 0, "a", policy: [order: :ordered, flags: :insert_bounded]),
       [9, 0, "a", 1, 2]},
      {ListOp.trim("items", 0, 2), [10, 0, 2]},
      {ListOp.clear("items"), [11]},
      {ListOp.increment("items", 0, 3), [12, 0, 3]},
      {ListOp.increment("items", 0, 3, policy: [flags: :no_fail]), [12, 0, 3, 4]},
      {ListOp.sort("items"), [13, 0]},
      {ListOp.sort("items", [:descending, :drop_duplicates]), [13, 3]},
      {ListOp.sort("items", sort_flags: [:drop_duplicates]), [13, 2]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_cdt_modify(), expected_payload)
    end)
  end

  test "read builders encode reference op codes and payloads" do
    cases = [
      {ListOp.size("items"), [16]},
      {ListOp.get("items", 1), [17, 1]},
      {ListOp.get_range("items", 1, 2), [18, 1, 2]},
      {ListOp.get_range_from("items", 1), [18, 1]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_cdt_read(), expected_payload)
    end)
  end

  test "get-by selectors default to returning values" do
    cases = [
      {ListOp.get_by_index("items", 1), [19, 7, 1]},
      {ListOp.get_by_rank("items", -1), [21, 7, -1]},
      {ListOp.get_by_value("items", "a"), [22, 7, "a"]},
      {ListOp.get_by_value_list("items", ["a", "b"]), [23, 7, ["a", "b"]]},
      {ListOp.get_by_index_range_from("items", 1), [24, 7, 1]},
      {ListOp.get_by_index_range("items", 1, 2), [24, 7, 1, 2]},
      {ListOp.get_by_value_range("items", "a", "z"), [25, 7, "a", "z"]},
      {ListOp.get_by_rank_range_from("items", -2), [26, 7, -2]},
      {ListOp.get_by_rank_range("items", -2, 2), [26, 7, -2, 2]},
      {ListOp.get_by_value_rel_rank_range("items", "m", -1), [27, 7, "m", -1]},
      {ListOp.get_by_value_rel_rank_range_count("items", "m", -1, 3), [27, 7, "m", -1, 3]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_cdt_read(), expected_payload)
    end)
  end

  test "remove-by selectors default to returning values" do
    cases = [
      {ListOp.remove_by_index("items", 1), [32, 7, 1]},
      {ListOp.remove_by_rank("items", -1), [34, 7, -1]},
      {ListOp.remove_by_value("items", "a"), [35, 7, "a"]},
      {ListOp.remove_by_value_list("items", ["a", "b"]), [36, 7, ["a", "b"]]},
      {ListOp.remove_by_index_range_from("items", 1), [37, 7, 1]},
      {ListOp.remove_by_index_range("items", 1, 2), [37, 7, 1, 2]},
      {ListOp.remove_by_value_range("items", "a", "z"), [38, 7, "a", "z"]},
      {ListOp.remove_by_rank_range_from("items", -2), [39, 7, -2]},
      {ListOp.remove_by_rank_range("items", -2, 2), [39, 7, -2, 2]},
      {ListOp.remove_by_value_rel_rank_range("items", "m", -1), [40, 7, "m", -1]},
      {ListOp.remove_by_value_rel_rank_range_count("items", "m", -1, 3), [40, 7, "m", -1, 3]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_cdt_modify(), expected_payload)
    end)
  end

  test "return_type option overrides selector defaults" do
    assert payload(ListOp.get_by_index("items", 1, return_type: :count)) == [
             19,
             5,
             1
           ]

    assert payload(ListOp.get_by_value("items", "a", return_type: [:value, :inverted])) == [
             22,
             0x10007,
             "a"
           ]

    assert payload(ListOp.remove_by_value("items", "a", return_type: :none)) == [
             35,
             0,
             "a"
           ]
  end

  test "ctx option is encoded through the CDT helper path" do
    ctx = [Ctx.map_key("outer"), Ctx.list_index(0)]
    op = ListOp.append("items", "a", ctx: ctx)

    assert payload(op) == [
             255,
             [0x22, "outer", 0x10, 0],
             [1, "a"]
           ]
  end

  test "create builder applies order flags to the final context step" do
    ctx = [Ctx.map_key("outer"), Ctx.list_index(2)]
    op = ListOp.create("items", :unordered, ctx: ctx, pad: true)

    assert payload(op) == [
             255,
             [0x22, "outer", 0x90, 2],
             [0, 0]
           ]
  end

  test "ctx option supports create and all-children context steps" do
    ctx = [Ctx.map_key("outer"), Ctx.list_index_create(0, :ordered), Ctx.all_children()]
    op = ListOp.size("items", ctx: ctx)

    assert [255, encoded_ctx, [16]] = payload(op)
    assert [0x22, "outer", 0xD0, 0, 0x04, expression] = encoded_ctx
    assert is_binary(expression)
  end
end
