defmodule Aerospike.Protocol.ExpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.Exp

  # Expected bytes are computed independently from the Go reference (expression.go)
  # and cross-checked against the msgpack spec. This avoids circular encode/decode tests.
  #
  # msgpack fixint:  n (0x00..0x7F) -> <<n>>
  # msgpack fixarray(N): <<0x90 + N>>
  # msgpack fixstr(len):  <<0xA0 + len, bytes...>>
  # msgpack nil:    <<0xC0>>
  # msgpack false:  <<0xC2>>
  # msgpack true:   <<0xC3>>
  # msgpack float64: <<0xCB, f::64-float-big>>

  describe "literal values" do
    test "integer 42 encodes as msgpack fixint" do
      # 42 = 0x2A; positive fixint range [0, 127] -> single byte
      assert Exp.encode(%{val: 42}) == <<0x2A>>
    end

    test "integer 0 encodes as msgpack fixint" do
      assert Exp.encode(%{val: 0}) == <<0x00>>
    end

    test "negative integer -1 encodes as msgpack negative fixint" do
      # negative fixint: range [-32, -1] -> 0xE0..0xFF; -1 = 0xFF
      assert Exp.encode(%{val: -1}) == <<0xFF>>
    end

    test "float encodes as msgpack float64" do
      assert Exp.encode(%{val: 1.5}) == <<0xCB, 1.5::64-float-big>>
    end

    test "nil literal encodes as msgpack nil" do
      assert Exp.encode(%{val: nil}) == <<0xC0>>
    end

    test "false literal encodes as msgpack false" do
      assert Exp.encode(%{val: false}) == <<0xC2>>
    end

    test "true literal encodes as msgpack true" do
      assert Exp.encode(%{val: true}) == <<0xC3>>
    end

    test "string literal encodes as plain msgpack str (no particle prefix)" do
      # "hi" -> fixstr(2) = <<0xA2, 0x68, 0x69>>
      assert Exp.encode(%{val: {:string, "hi"}}) == <<0xA2, 0x68, 0x69>>
    end

    test "blob literal encodes as msgpack bin (0xC4 format)" do
      # <<1, 2, 3>> -> bin8(3) = <<0xC4, 0x03, 0x01, 0x02, 0x03>>
      assert Exp.encode(%{val: {:blob, <<1, 2, 3>>}}) == <<0xC4, 0x03, 0x01, 0x02, 0x03>>
    end
  end

  describe "pre-encoded bytes" do
    test "bytes node writes bytes directly" do
      raw = <<0xAB, 0xCD, 0xEF>>
      assert Exp.encode(%{bytes: raw}) == raw
    end
  end

  describe "bin reads" do
    test "int_bin encodes as [81, 2, raw_str_name]" do
      # Verified against Go: expOpBIN=81, ExpTypeINT=2, packRawString("age")=fixstr(3,"age")
      # fixarray(3) = 0x93
      # fixint(81)  = 0x51
      # fixint(2)   = 0x02
      # fixstr("age") = 0xA3, 0x61, 0x67, 0x65
      expected = <<0x93, 0x51, 0x02, 0xA3, 0x61, 0x67, 0x65>>
      assert Exp.encode(%{cmd: :bin, val: "age", type: :int}) == expected
    end

    test "str_bin encodes as [81, 3, raw_str_name]" do
      # ExpTypeSTRING=3
      expected = <<0x93, 0x51, 0x03, 0xA4, 0x6E, 0x61, 0x6D, 0x65>>
      assert Exp.encode(%{cmd: :bin, val: "name", type: :string}) == expected
    end

    test "float_bin encodes as [81, 7, raw_str_name]" do
      # ExpTypeFLOAT=7
      expected = <<0x93, 0x51, 0x07, 0xA5, 0x70, 0x72, 0x69, 0x63, 0x65>>
      assert Exp.encode(%{cmd: :bin, val: "price", type: :float}) == expected
    end

    test "bool_bin encodes as [81, 1, raw_str_name]" do
      # ExpTypeBOOL=1
      expected = <<0x93, 0x51, 0x01, 0xA6, 0x61, 0x63, 0x74, 0x69, 0x76, 0x65>>
      assert Exp.encode(%{cmd: :bin, val: "active", type: :bool}) == expected
    end
  end

  describe "metadata" do
    test "ttl() encodes as [69]" do
      # expOpTTL=69, fixarray(1)=0x91, fixint(69)=0x45
      assert Exp.encode(%{cmd: :ttl}) == <<0x91, 0x45>>
    end

    test "void_time() encodes as [68]" do
      # expOpVOID_TIME=68 = 0x44
      assert Exp.encode(%{cmd: :void_time}) == <<0x91, 0x44>>
    end

    test "last_update() encodes as [66]" do
      # expOpLAST_UPDATE=66 = 0x42
      assert Exp.encode(%{cmd: :last_update}) == <<0x91, 0x42>>
    end

    test "key_exists() encodes as [71]" do
      # expOpKEY_EXISTS=71 = 0x47
      assert Exp.encode(%{cmd: :key_exists}) == <<0x91, 0x47>>
    end

    test "set_name() encodes as [70]" do
      # expOpSET_NAME=70 = 0x46
      assert Exp.encode(%{cmd: :set_name}) == <<0x91, 0x46>>
    end

    test "is_tombstone() encodes as [72]" do
      # expOpIS_TOMBSTONE=72 = 0x48
      assert Exp.encode(%{cmd: :is_tombstone}) == <<0x91, 0x48>>
    end

    test "record_size() encodes as [74]" do
      # expOpRECORD_SIZE=74 = 0x4A
      assert Exp.encode(%{cmd: :record_size}) == <<0x91, 0x4A>>
    end

    test "device_size() encodes as [65]" do
      # expOpDEVICE_SIZE=65 = 0x41
      assert Exp.encode(%{cmd: :device_size}) == <<0x91, 0x41>>
    end

    test "digest_modulo encodes as [64, value]" do
      # expOpDIGEST_MODULO=64 = 0x40, fixint(3)=0x03
      # fixarray(2) = 0x92
      assert Exp.encode(%{cmd: :digest_modulo, val: 3}) == <<0x92, 0x40, 0x03>>
    end
  end

  describe "comparison expressions" do
    # For compound tests: sub-expressions are pre-encoded bytes.
    # int_bin("age") wire = <<0x93, 0x51, 0x02, 0xA3, 0x61, 0x67, 0x65>>
    # int_val(21) wire    = <<21>> = <<0x15>>
    #
    # eq([sub1, sub2]) -> [1, sub1_bytes, sub2_bytes]
    # fixarray(3)=0x93, expOpEQ=1=0x01
    # Full: 0x93 0x01 <int_bin_age> <int_val_21>

    @int_bin_age <<0x93, 0x51, 0x02, 0xA3, 0x61, 0x67, 0x65>>
    @int_val_21 <<21>>
    @int_val_0 <<0>>

    test "eq encodes as [1, sub1, sub2]" do
      # expOpEQ=1=0x01; fixarray(3)=0x93
      expected = <<0x93, 0x01>> <> @int_bin_age <> @int_val_21
      result = Exp.encode(%{cmd: :eq, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]})
      assert result == expected
    end

    test "gt encodes as [3, sub1, sub2]" do
      # expOpGT=3=0x03
      expected = <<0x93, 0x03>> <> @int_bin_age <> @int_val_0
      result = Exp.encode(%{cmd: :gt, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_0}]})
      assert result == expected
    end

    test "gte encodes as [4, sub1, sub2]" do
      expected = <<0x93, 0x04>> <> @int_bin_age <> @int_val_21
      result = Exp.encode(%{cmd: :gte, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]})
      assert result == expected
    end

    test "lt encodes as [5, sub1, sub2]" do
      expected = <<0x93, 0x05>> <> @int_bin_age <> @int_val_21
      result = Exp.encode(%{cmd: :lt, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]})
      assert result == expected
    end

    test "lte encodes as [6, sub1, sub2]" do
      expected = <<0x93, 0x06>> <> @int_bin_age <> @int_val_21
      result = Exp.encode(%{cmd: :lte, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]})
      assert result == expected
    end

    test "ne encodes as [2, sub1, sub2]" do
      expected = <<0x93, 0x02>> <> @int_bin_age <> @int_val_21
      result = Exp.encode(%{cmd: :ne, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]})
      assert result == expected
    end
  end

  describe "boolean expressions" do
    # eq_bytes = [1, int_bin_age, 21]
    # gt_bytes = [3, 5, 3]  (gt(int_val(5), int_val(3)) = 0x93, 0x03, 0x05, 0x03)
    @eq_bytes <<0x93, 0x01, 0x93, 0x51, 0x02, 0xA3, 0x61, 0x67, 0x65, 0x15>>
    @gt_bytes <<0x93, 0x03, 0x05, 0x03>>

    test "and_ encodes as [16, sub1, sub2]" do
      # expOpAND=16=0x10; fixarray(3)=0x93
      expected = <<0x93, 0x10>> <> @eq_bytes <> @gt_bytes

      result =
        Exp.encode(%{cmd: :and_, exps: [%{bytes: @eq_bytes}, %{bytes: @gt_bytes}]})

      assert result == expected
    end

    test "or_ encodes as [17, sub1, sub2]" do
      # expOpOR=17=0x11; fixarray(3)=0x93
      expected = <<0x93, 0x11>> <> @eq_bytes <> @gt_bytes

      result =
        Exp.encode(%{cmd: :or_, exps: [%{bytes: @eq_bytes}, %{bytes: @gt_bytes}]})

      assert result == expected
    end

    test "not_ encodes as [18, sub_expr]" do
      # expOpNOT=18=0x12; fixarray(2)=0x92
      expected = <<0x92, 0x12>> <> @eq_bytes
      result = Exp.encode(%{cmd: :not_, exps: [%{bytes: @eq_bytes}]})
      assert result == expected
    end

    test "and_ with 3 sub-expressions uses fixarray(4)" do
      sub = <<0x01>>
      # fixarray(4) = 0x94
      expected = <<0x94, 0x10, 0x01, 0x01, 0x01>>
      result = Exp.encode(%{cmd: :and_, exps: [%{bytes: sub}, %{bytes: sub}, %{bytes: sub}]})
      assert result == expected
    end
  end

  describe "nested expression" do
    test "eq(int_bin(age), int_val(21)) full byte sequence" do
      # This is the primary cross-reference test against the Go reference.
      # int_bin("age") -> [81, 2, "age"] -> <<0x93, 0x51, 0x02, 0xA3, 0x61, 0x67, 0x65>>
      # int_val(21)    -> <<21>> = <<0x15>>
      # eq(left, right) -> [1, left_bytes, right_bytes]
      #   -> fixarray(3)=0x93, expOpEQ=0x01, left, right
      #   -> <<0x93, 0x01, 0x93, 0x51, 0x02, 0xA3, 0x61, 0x67, 0x65, 0x15>>

      int_bin_age = Exp.encode(%{cmd: :bin, val: "age", type: :int})
      int_val_21 = Exp.encode(%{val: 21})

      eq_result =
        Exp.encode(%{cmd: :eq, exps: [%{bytes: int_bin_age}, %{bytes: int_val_21}]})

      expected = <<0x93, 0x01, 0x93, 0x51, 0x02, 0xA3, 0x61, 0x67, 0x65, 0x15>>
      assert eq_result == expected
    end
  end
end
