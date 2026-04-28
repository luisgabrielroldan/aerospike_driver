defmodule Aerospike.PolicyIntegerTest do
  use ExUnit.Case, async: true

  alias Aerospike.PolicyInteger

  describe "list values" do
    test "normalizes orders, return types, write flags, and sort flags" do
      assert PolicyInteger.list_order(:ordered) == 1
      assert PolicyInteger.list_order({:raw, 17}) == 17
      assert PolicyInteger.list_return_type([:value, :inverted]) == 0x10007
      assert PolicyInteger.list_write_flags([:add_unique, :no_fail, :partial]) == 13
      assert PolicyInteger.list_sort_flags([:descending, :drop_duplicates]) == 3
    end

    test "normalizes list policies and create flags" do
      assert PolicyInteger.list_policy(order: :ordered, flags: [:insert_bounded]) == %{
               order: 1,
               flags: 2
             }

      assert PolicyInteger.list_order_attr(:ordered, true) == 0x11
      assert PolicyInteger.list_create_flag(:unordered, false) == 0x40
      assert PolicyInteger.list_create_flag(:unordered, true) == 0x80
      assert PolicyInteger.list_create_flag(:ordered, false) == 0xC0
      assert PolicyInteger.list_create_flag({:raw, 1}, true) == 0xC0
    end
  end

  describe "map values" do
    test "normalizes orders, return types, and write flags" do
      assert PolicyInteger.map_order(:key_value_ordered) == 3
      assert PolicyInteger.map_return_type([:key_value, :inverted]) == 0x10008
      assert PolicyInteger.map_return_type(:ordered_map) == 17
      assert PolicyInteger.map_write_flags([:update_only, :no_fail, :partial]) == 14
    end

    test "normalizes map policies and create flags" do
      assert PolicyInteger.map_policy(order: :key_ordered, flags: [:create_only]) == %{
               attr: 1,
               flags: 1
             }

      assert PolicyInteger.map_policy(%{attr: 3, flags: 2}) == %{attr: 3, flags: 2}
      assert PolicyInteger.map_order_attr(:key_ordered, true) == 0x11
      assert PolicyInteger.strip_persist_index(0x11) == 1
      assert PolicyInteger.map_create_flag(:unordered) == 0x40
      assert PolicyInteger.map_create_flag(:key_ordered) == 0x80
      assert PolicyInteger.map_create_flag(:key_value_ordered) == 0xC0
    end
  end

  describe "bit and hll values" do
    test "normalizes bit flags and overflow actions" do
      assert PolicyInteger.bit_write_flags([:no_fail, :partial]) == 12
      assert PolicyInteger.bit_resize_flags([:from_front, :grow_only]) == 3
      assert PolicyInteger.bit_overflow_action(:saturate) == 2
      assert PolicyInteger.bit_overflow_action(:wrap, true) == 5
      assert PolicyInteger.bit_signed_flag() == 1
    end

    test "normalizes hll write flags" do
      assert PolicyInteger.hll_write_flags([:create_only, :allow_fold]) == 9
    end
  end

  describe "expression values" do
    test "normalizes read and write flags" do
      assert PolicyInteger.exp_read_flags(:eval_no_fail) == 16
      assert PolicyInteger.exp_read_flags([:eval_no_fail]) == 16
      assert PolicyInteger.exp_write_flags([:create_only, :allow_delete, :policy_no_fail]) == 13
    end
  end

  describe "validation" do
    test "accepts non-negative integer compatibility values" do
      assert PolicyInteger.map_write_flags(12) == 12
      assert PolicyInteger.bit_resize_flags({:raw, 6}) == 6
    end

    test "rejects unknown atoms and negative integers" do
      assert_raise ArgumentError, ~r/unknown list write flags :missing/, fn ->
        PolicyInteger.list_write_flags(:missing)
      end

      assert_raise ArgumentError, ~r/invalid bit resize flags -1/, fn ->
        PolicyInteger.bit_resize_flags(-1)
      end
    end
  end
end
