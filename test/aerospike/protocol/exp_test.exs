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
    end
  end

  describe "metadata" do
    test "encodes no-argument metadata nodes" do
      assert Exp.encode(%{cmd: :device_size}) == <<0x91, 0x41>>
      assert Exp.encode(%{cmd: :last_update}) == <<0x91, 0x42>>
      assert Exp.encode(%{cmd: :void_time}) == <<0x91, 0x44>>
      assert Exp.encode(%{cmd: :ttl}) == <<0x91, 0x45>>
      assert Exp.encode(%{cmd: :set_name}) == <<0x91, 0x46>>
      assert Exp.encode(%{cmd: :key_exists}) == <<0x91, 0x47>>
      assert Exp.encode(%{cmd: :is_tombstone}) == <<0x91, 0x48>>
      assert Exp.encode(%{cmd: :record_size}) == <<0x91, 0x4A>>
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
    end

    test "uses array16 for compound nodes with more than fifteen elements" do
      nodes = List.duplicate(%{bytes: <<0x01>>}, 16)

      assert Exp.encode(%{cmd: :and_, exps: nodes}) ==
               <<0xDC, 0x00, 0x11, 0x10>> <> :binary.copy(<<0x01>>, 16)
    end
  end
end
