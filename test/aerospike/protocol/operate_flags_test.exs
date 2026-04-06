defmodule Aerospike.Protocol.OperateFlagsTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.OperateFlags

  defp op(type, opts \\ []) do
    %Operation{
      op_type: type,
      bin_name: Keyword.get(opts, :bin_name, "b"),
      read_header: Keyword.get(opts, :read_header, false),
      map_cdt: Keyword.get(opts, :map_cdt, false)
    }
  end

  describe "scan_ops/1 flag accumulation" do
    test "empty op list returns zeroed flags" do
      result = OperateFlags.scan_ops([])

      assert result.info1 == 0
      assert result.info2 == 0
      assert result.info3 == 0
      assert result.has_write? == false
      assert result.read_bin? == false
      assert result.read_header? == false
      assert result.respond_all? == false
      assert result.header_only? == false
    end

    test "single READ op sets info1 READ and read_bin?" do
      result = OperateFlags.scan_ops([op(Operation.op_read())])

      assert (result.info1 &&& AsmMsg.info1_read()) != 0
      assert result.read_bin? == true
      assert result.has_write? == false
      assert result.header_only? == false
    end

    test "READ with empty bin_name sets GET_ALL" do
      result = OperateFlags.scan_ops([op(Operation.op_read(), bin_name: "")])

      assert (result.info1 &&& AsmMsg.info1_read()) != 0
      assert (result.info1 &&& AsmMsg.info1_get_all()) != 0
      assert result.read_bin? == true
    end

    test "READ with read_header: true sets read_header? but not read_bin?" do
      result = OperateFlags.scan_ops([op(Operation.op_read(), read_header: true)])

      assert (result.info1 &&& AsmMsg.info1_read()) != 0
      assert result.read_header? == true
      assert result.read_bin? == false
      assert result.header_only? == true
    end

    test "header_only? is false when both read_header? and read_bin? are true" do
      ops = [
        op(Operation.op_read(), read_header: true),
        op(Operation.op_read(), bin_name: "data")
      ]

      result = OperateFlags.scan_ops(ops)

      assert result.read_header? == true
      assert result.read_bin? == true
      assert result.header_only? == false
    end
  end

  describe "scan_ops/1 write op types" do
    @write_op_types [
      {:write, Operation.op_write()},
      {:cdt_modify, Operation.op_cdt_modify()},
      {:add, Operation.op_add()},
      {:exp_modify, Operation.op_exp_modify()},
      {:append, Operation.op_append()},
      {:prepend, Operation.op_prepend()},
      {:touch, Operation.op_touch()},
      {:bit_modify, Operation.op_bit_modify()},
      {:delete, Operation.op_delete()},
      {:hll_modify, Operation.op_hll_modify()}
    ]

    for {name, op_type} <- @write_op_types do
      test "#{name} sets has_write?" do
        result = OperateFlags.scan_ops([op(unquote(op_type))])
        assert result.has_write? == true
      end
    end
  end

  describe "scan_ops/1 read op types" do
    @read_like_ops [
      {:cdt_read, Operation.op_cdt_read()},
      {:exp_read, Operation.op_exp_read()},
      {:bit_read, Operation.op_bit_read()},
      {:hll_read, Operation.op_hll_read()}
    ]

    for {name, op_type} <- @read_like_ops do
      test "#{name} sets info1 READ and read_bin?" do
        result = OperateFlags.scan_ops([op(unquote(op_type))])

        assert (result.info1 &&& AsmMsg.info1_read()) != 0
        assert result.read_bin? == true
        assert result.has_write? == false
      end
    end
  end

  describe "scan_ops/1 respond_all?" do
    @respond_all_op_types [
      {:exp_read, Operation.op_exp_read()},
      {:exp_modify, Operation.op_exp_modify()},
      {:bit_read, Operation.op_bit_read()},
      {:bit_modify, Operation.op_bit_modify()},
      {:hll_read, Operation.op_hll_read()},
      {:hll_modify, Operation.op_hll_modify()}
    ]

    for {name, op_type} <- @respond_all_op_types do
      test "#{name} sets respond_all?" do
        result = OperateFlags.scan_ops([op(unquote(op_type))])
        assert result.respond_all? == true
      end
    end

    test "cdt_read with map_cdt: true sets respond_all?" do
      result = OperateFlags.scan_ops([op(Operation.op_cdt_read(), map_cdt: true)])
      assert result.respond_all? == true
    end

    test "cdt_modify with map_cdt: true sets respond_all?" do
      result = OperateFlags.scan_ops([op(Operation.op_cdt_modify(), map_cdt: true)])
      assert result.respond_all? == true
    end

    test "cdt_read with map_cdt: false does NOT set respond_all?" do
      result = OperateFlags.scan_ops([op(Operation.op_cdt_read(), map_cdt: false)])
      assert result.respond_all? == false
    end

    test "cdt_modify with map_cdt: false does NOT set respond_all?" do
      result = OperateFlags.scan_ops([op(Operation.op_cdt_modify(), map_cdt: false)])
      assert result.respond_all? == false
    end

    test "plain READ does NOT set respond_all?" do
      result = OperateFlags.scan_ops([op(Operation.op_read())])
      assert result.respond_all? == false
    end

    test "plain WRITE does NOT set respond_all?" do
      result = OperateFlags.scan_ops([op(Operation.op_write())])
      assert result.respond_all? == false
    end
  end

  describe "scan_ops/1 mixed operations" do
    test "read + write sets both read and write flags" do
      ops = [op(Operation.op_read()), op(Operation.op_write())]
      result = OperateFlags.scan_ops(ops)

      assert (result.info1 &&& AsmMsg.info1_read()) != 0
      assert result.read_bin? == true
      assert result.has_write? == true
    end

    test "multiple reads accumulate info1 flags" do
      ops = [
        op(Operation.op_read(), bin_name: "a"),
        op(Operation.op_read(), bin_name: "")
      ]

      result = OperateFlags.scan_ops(ops)

      assert (result.info1 &&& AsmMsg.info1_read()) != 0
      assert (result.info1 &&& AsmMsg.info1_get_all()) != 0
    end

    test "respond_all? stays true once set even if later ops don't require it" do
      ops = [
        op(Operation.op_exp_read()),
        op(Operation.op_read())
      ]

      result = OperateFlags.scan_ops(ops)
      assert result.respond_all? == true
    end

    test "unknown op type is a no-op (falls through to catch-all)" do
      unknown = %Operation{op_type: 255, bin_name: "x"}
      result = OperateFlags.scan_ops([unknown])

      assert result.info1 == 0
      assert result.has_write? == false
      assert result.read_bin? == false
    end
  end
end
