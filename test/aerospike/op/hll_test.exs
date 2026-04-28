defmodule Aerospike.Op.HLLTest do
  use ExUnit.Case, async: true

  alias Aerospike.Op.HLL
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.CDT
  alias Aerospike.Protocol.MessagePack

  defp payload(op), do: MessagePack.unpack!(op.data)

  defp assert_op(op, op_type, expected_payload) do
    assert op.op_type == op_type
    assert op.bin_name == "hll"
    refute op.map_cdt
    assert payload(op) == expected_payload
  end

  test "write flag helpers expose server policy values" do
    assert HLL.write_default() == 0
    assert HLL.write_create_only() == 1
    assert HLL.write_update_only() == 2
    assert HLL.write_no_fail() == 4
    assert HLL.write_allow_fold() == 8
  end

  test "modify builders encode reference op codes and payloads" do
    cases = [
      {HLL.init("hll"), [0, -1, -1, 0]},
      {HLL.init("hll", 10, 20, flags: HLL.write_update_only()), [0, 10, 20, 2]},
      {HLL.add("hll", ["a", "b"]), [1, ["a", "b"], -1, -1, 0]},
      {HLL.add("hll", ["a", {:bytes, <<1, 2>>}], 8, 16, flags: 3),
       [1, ["a", <<4, 1, 2>>], 8, 16, 3]},
      {HLL.set_union("hll", [{:bytes, <<1, 2, 3>>}], flags: HLL.write_no_fail()),
       [2, [<<4, 1, 2, 3>>], 4]},
      {HLL.refresh_count("hll"), [3]},
      {HLL.fold("hll", 6), [4, 6]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_hll_modify(), expected_payload)
    end)
  end

  test "read builders encode reference op codes and payloads" do
    hlls = [{:bytes, <<1, 2, 3>>}]

    cases = [
      {HLL.get_count("hll"), [50]},
      {HLL.get_union("hll", hlls), [51, [<<4, 1, 2, 3>>]]},
      {HLL.get_union_count("hll", hlls), [52, [<<4, 1, 2, 3>>]]},
      {HLL.get_intersect_count("hll", hlls), [53, [<<4, 1, 2, 3>>]]},
      {HLL.get_similarity("hll", hlls), [54, [<<4, 1, 2, 3>>]]},
      {HLL.describe("hll"), [55]}
    ]

    Enum.each(cases, fn {op, expected_payload} ->
      assert_op(op, Operation.op_hll_read(), expected_payload)
    end)
  end

  test "HLL builders do not expose nested context encoding" do
    refute function_exported?(CDT, :hll_read_op, 4)
    refute function_exported?(CDT, :hll_modify_op, 4)
    refute function_exported?(HLL, :init, 5)
    refute function_exported?(HLL, :get_count, 2)
  end
end
