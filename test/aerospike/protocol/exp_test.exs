defmodule Aerospike.Protocol.ExpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.Exp

  @int_bin_age <<0x93, 0x51, 0x02, 0xA3, 0x61, 0x67, 0x65>>
  @int_val_21 <<0x15>>
  @eq_age_21 <<0x93, 0x01>> <> @int_bin_age <> @int_val_21

  describe "literal values" do
    test "encodes scalar literals as MessagePack values" do
      assert Exp.encode(%{val: nil}) == <<0xC0>>
      assert Exp.encode(%{val: false}) == <<0xC2>>
      assert Exp.encode(%{val: true}) == <<0xC3>>
      assert Exp.encode(%{val: 0}) == <<0x00>>
      assert Exp.encode(%{val: 42}) == <<0x2A>>
      assert Exp.encode(%{val: -1}) == <<0xFF>>
      assert Exp.encode(%{val: 1.5}) == <<0xCB, 1.5::64-float-big>>
    end

    test "encodes string literals as raw MessagePack strings" do
      assert Exp.encode(%{val: {:string, "hi"}}) == <<0xA2, 0x68, 0x69>>
    end

    test "encodes compound and special literals" do
      assert Exp.encode(%{val: {:geo, "{}"}}) == <<0xD9, 0x03, 0x17, "{}">>
      assert Exp.encode(%{val: {:list, [1, 2]}}) == <<0x92, 0x7E, 0x92, 0x01, 0x02>>
      assert Exp.encode(%{val: {:map, %{"a" => 1}}}) == <<0x81, 0xA1, "a", 0x01>>
      assert Exp.encode(%{val: :infinity}) == <<0xD4, 0xFF, 0x01>>
      assert Exp.encode(%{val: :wildcard}) == <<0xD4, 0xFF, 0x00>>
    end

    test "encodes blob literals as MessagePack bin payloads" do
      assert Exp.encode(%{val: {:blob, <<1, 2, 3>>}}) == <<0xC4, 0x03, 0x01, 0x02, 0x03>>

      bin16 = :binary.copy(<<1>>, 256)
      assert <<0xC5, 0x01, 0x00, _::binary-size(256)>> = Exp.encode(%{val: {:blob, bin16}})

      bin32 = :binary.copy(<<1>>, 65_536)

      assert <<0xC6, 0x00, 0x01, 0x00, 0x00, _::binary-size(65_536)>> =
               Exp.encode(%{val: {:blob, bin32}})
    end
  end

  describe "pre-encoded bytes" do
    test "returns bytes nodes unchanged" do
      assert Exp.encode(%{bytes: <<0xAB, 0xCD, 0xEF>>}) == <<0xAB, 0xCD, 0xEF>>
    end
  end

  describe "bin reads" do
    test "encodes typed bin reads with raw MessagePack string names" do
      assert Exp.encode(%{cmd: :key, type: :int}) == <<0x92, 0x50, 0x02>>

      assert Exp.encode(%{cmd: :bin, val: "age", type: :int}) == @int_bin_age

      assert Exp.encode(%{cmd: :bin, val: "name", type: :string}) ==
               <<0x93, 0x51, 0x03, 0xA4, "name">>

      assert Exp.encode(%{cmd: :bin, val: "price", type: :float}) ==
               <<0x93, 0x51, 0x07, 0xA5, "price">>

      assert Exp.encode(%{cmd: :bin, val: "active", type: :bool}) ==
               <<0x93, 0x51, 0x01, 0xA6, "active">>

      assert Exp.encode(%{cmd: :bin, val: "payload", type: :blob}) ==
               <<0x93, 0x51, 0x06, 0xA7, "payload">>

      assert Exp.encode(%{cmd: :bin, val: "items", type: :list}) ==
               <<0x93, 0x51, 0x04, 0xA5, "items">>

      assert Exp.encode(%{cmd: :bin, val: "prefs", type: :map}) ==
               <<0x93, 0x51, 0x05, 0xA5, "prefs">>

      assert Exp.encode(%{cmd: :bin, val: "region", type: :geo}) ==
               <<0x93, 0x51, 0x08, 0xA6, "region">>

      assert Exp.encode(%{cmd: :bin, val: "hll", type: :hll}) == <<0x93, 0x51, 0x09, 0xA3, "hll">>

      assert Exp.encode(%{cmd: :bin, val: "missing", type: nil}) ==
               <<0x93, 0x51, 0x00, 0xA7, "missing">>

      assert Exp.encode(%{cmd: :bin_type, val: "age"}) == <<0x92, 0x52, 0xA3, "age">>
      assert Exp.encode(%{cmd: :loop_var, type: :int, val: 1}) == <<0x93, 0x7A, 0x02, 0x01>>
    end
  end

  describe "metadata" do
    test "encodes no-argument metadata nodes" do
      assert Exp.encode(%{cmd: :device_size}) == <<0x91, 0x41>>
      assert Exp.encode(%{cmd: :last_update}) == <<0x91, 0x42>>
      assert Exp.encode(%{cmd: :since_update}) == <<0x91, 0x43>>
      assert Exp.encode(%{cmd: :void_time}) == <<0x91, 0x44>>
      assert Exp.encode(%{cmd: :ttl}) == <<0x91, 0x45>>
      assert Exp.encode(%{cmd: :set_name}) == <<0x91, 0x46>>
      assert Exp.encode(%{cmd: :key_exists}) == <<0x91, 0x47>>
      assert Exp.encode(%{cmd: :is_tombstone}) == <<0x91, 0x48>>
      assert Exp.encode(%{cmd: :record_size}) == <<0x91, 0x4A>>
      assert Exp.encode(%{cmd: :remove_result}) == <<0x91, 0x64>>
      assert Exp.encode(%{cmd: :unknown}) == <<0x91, 0x00>>
    end

    test "encodes digest modulo with its integer argument" do
      assert Exp.encode(%{cmd: :digest_modulo, val: 3}) == <<0x92, 0x40, 0x03>>
    end
  end

  describe "comparisons" do
    test "encodes comparison nodes around already encoded sub-expressions" do
      assert Exp.encode(%{cmd: :eq, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]}) ==
               @eq_age_21

      assert Exp.encode(%{cmd: :ne, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]}) ==
               <<0x93, 0x02>> <> @int_bin_age <> @int_val_21

      assert Exp.encode(%{cmd: :gt, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]}) ==
               <<0x93, 0x03>> <> @int_bin_age <> @int_val_21

      assert Exp.encode(%{cmd: :gte, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]}) ==
               <<0x93, 0x04>> <> @int_bin_age <> @int_val_21

      assert Exp.encode(%{cmd: :lt, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]}) ==
               <<0x93, 0x05>> <> @int_bin_age <> @int_val_21

      assert Exp.encode(%{cmd: :lte, exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]}) ==
               <<0x93, 0x06>> <> @int_bin_age <> @int_val_21
    end
  end

  describe "boolean combinators" do
    @gt_5_3 <<0x93, 0x03, 0x05, 0x03>>

    test "encodes boolean nodes around already encoded sub-expressions" do
      assert Exp.encode(%{cmd: :and_, exps: [%{bytes: @eq_age_21}, %{bytes: @gt_5_3}]}) ==
               <<0x93, 0x10>> <> @eq_age_21 <> @gt_5_3

      assert Exp.encode(%{cmd: :or_, exps: [%{bytes: @eq_age_21}, %{bytes: @gt_5_3}]}) ==
               <<0x93, 0x11>> <> @eq_age_21 <> @gt_5_3

      assert Exp.encode(%{cmd: :not_, exps: [%{bytes: @eq_age_21}]}) ==
               <<0x92, 0x12>> <> @eq_age_21

      assert Exp.encode(%{cmd: :exclusive, exps: [%{bytes: @eq_age_21}, %{bytes: @gt_5_3}]}) ==
               <<0x93, 0x13>> <> @eq_age_21 <> @gt_5_3
    end

    test "uses array16 for compound nodes with more than fifteen elements" do
      nodes = List.duplicate(%{bytes: <<0x01>>}, 16)

      assert Exp.encode(%{cmd: :and_, exps: nodes}) ==
               <<0xDC, 0x00, 0x11, 0x10>> <> :binary.copy(<<0x01>>, 16)
    end
  end

  describe "core expression operators" do
    test "encodes regex and geo comparisons" do
      assert Exp.encode(%{cmd: :regex, val: {"^a", 2}, exps: [%{bytes: @int_bin_age}]}) ==
               <<0x94, 0x07, 0x02, 0xA2, "^a">> <> @int_bin_age

      assert Exp.encode(%{
               cmd: :geo_compare,
               exps: [%{bytes: @int_bin_age}, %{bytes: @int_val_21}]
             }) ==
               <<0x93, 0x08>> <> @int_bin_age <> @int_val_21
    end

    test "encodes numeric operator nodes" do
      assert Exp.encode(%{cmd: :add, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x14, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :sub, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x15, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :mul, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x16, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :div_, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x17, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :pow, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x18, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :log, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x19, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :mod, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x1A, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :abs, exps: [%{bytes: <<1>>}]}) == <<0x92, 0x1B, 0x01>>
      assert Exp.encode(%{cmd: :floor, exps: [%{bytes: <<1>>}]}) == <<0x92, 0x1C, 0x01>>
      assert Exp.encode(%{cmd: :ceil, exps: [%{bytes: <<1>>}]}) == <<0x92, 0x1D, 0x01>>
      assert Exp.encode(%{cmd: :to_int, exps: [%{bytes: <<1>>}]}) == <<0x92, 0x1E, 0x01>>
      assert Exp.encode(%{cmd: :to_float, exps: [%{bytes: <<1>>}]}) == <<0x92, 0x1F, 0x01>>

      assert Exp.encode(%{cmd: :min, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x32, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :max, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x33, 0x01, 0x02>>
    end

    test "encodes integer bitwise operator nodes" do
      assert Exp.encode(%{cmd: :int_and, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x20, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :int_or, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x21, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :int_xor, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x22, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :int_not, exps: [%{bytes: <<1>>}]}) == <<0x92, 0x23, 0x01>>

      assert Exp.encode(%{cmd: :int_lshift, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x24, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :int_rshift, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x25, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :int_arshift, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x26, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :int_count, exps: [%{bytes: <<1>>}]}) == <<0x92, 0x27, 0x01>>

      assert Exp.encode(%{cmd: :int_lscan, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x28, 0x01, 0x02>>

      assert Exp.encode(%{cmd: :int_rscan, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}]}) ==
               <<0x93, 0x29, 0x01, 0x02>>
    end

    test "encodes conditional and variable nodes" do
      assert Exp.encode(%{cmd: :cond, exps: [%{bytes: <<1>>}, %{bytes: <<2>>}, %{bytes: <<3>>}]}) ==
               <<0x94, 0x7B, 0x01, 0x02, 0x03>>

      assert Exp.encode(%{cmd: :var, val: "x"}) == <<0x92, 0x7C, 0xA1, "x">>

      def_node = %{cmd: :def, val: "x", exps: [%{bytes: <<1>>}]}
      var_node = %{cmd: :var, val: "x"}

      assert Exp.encode(%{cmd: :let, exps: [def_node, var_node]}) ==
               <<0x94, 0x7D, 0xA1, "x", 0x01, 0x92, 0x7C, 0xA1, "x">>
    end
  end

  describe "module calls" do
    test "encodes expression module payloads as CALL nodes" do
      assert Exp.encode(%{
               cmd: :call,
               type: :int,
               module: 1,
               payload: <<0x93, 0x33, 0x00, 0x08>>,
               bin: %{bytes: @int_bin_age}
             }) ==
               <<0x95, 0x7F, 0x02, 0x01, 0x93, 0x33, 0x00, 0x08>> <>
                 @int_bin_age
    end

    test "encodes module payload arguments with raw expression bytes" do
      assert Exp.module_payload(19, [7, %{bytes: <<0x00>>}]) == <<0x93, 0x13, 0x07, 0x00>>
    end
  end
end
