defmodule Aerospike.Exp.CDTTest do
  use ExUnit.Case, async: true

  alias Aerospike.Exp
  alias Aerospike.Exp.Bit, as: BitExp
  alias Aerospike.Exp.HLL, as: HLLExp
  alias Aerospike.Exp.List, as: ListExp
  alias Aerospike.Exp.Map, as: MapExp

  @list_bin <<0x93, 0x51, 0x04, 0xA5, "items">>
  @map_bin <<0x93, 0x51, 0x05, 0xA5, "prefs">>
  @blob_bin <<0x93, 0x51, 0x06, 0xA4, "bits">>
  @hll_bin <<0x93, 0x51, 0x09, 0xA3, "hll">>

  describe "list expressions" do
    test "size/1 creates an integer CDT module call" do
      expected = <<0x95, 0x7F, 0x02, 0x00, 0x91, 0x10>> <> @list_bin

      assert %Exp{wire: ^expected} = ListExp.size(Exp.list_bin("items"))
    end

    test "append/3 creates a modify call with expression arguments" do
      expected =
        <<0x95, 0x7F, 0x04, 0x40, 0x94, 0x01, 0x2A, 0x02, 0x00>> <>
          @list_bin

      assert %Exp{wire: ^expected} =
               ListExp.append(Exp.list_bin("items"), Exp.int(42),
                 policy: %{order: ListExp.return_reverse_index(), flags: 0}
               )
    end

    test "single-value selectors use the caller supplied value type" do
      expected =
        <<0x95, 0x7F, 0x03, 0x00, 0x93, 0x13, 0x07, 0x00>> <> @list_bin

      assert %Exp{wire: ^expected} =
               ListExp.get_by_index(Exp.list_bin("items"), Exp.int(0), :string)
    end
  end

  describe "map expressions" do
    test "get_by_key/4 creates a typed read call" do
      expected =
        <<0x95, 0x7F, 0x02, 0x00, 0x93, 0x61, 0x07, 0xA5, "score">> <>
          @map_bin

      assert %Exp{wire: ^expected} =
               MapExp.get_by_key(Exp.map_bin("prefs"), Exp.str("score"), :int)
    end

    test "relative index selectors are available for expression paths" do
      expected =
        <<0x95, 0x7F, 0x05, 0x00, 0x94, 0x6D, 0x08, 0xA1, "b", 0x00>> <>
          @map_bin

      assert %Exp{wire: ^expected} =
               MapExp.get_by_key_rel_index_range(Exp.map_bin("prefs"), Exp.str("b"), Exp.int(0))
    end
  end

  describe "bit expressions" do
    test "count/3 creates an integer bit module call" do
      expected =
        <<0x95, 0x7F, 0x02, 0x01, 0x93, 0x33, 0x00, 0x08>> <> @blob_bin

      assert %Exp{wire: ^expected} = BitExp.count(Exp.blob_bin("bits"), Exp.int(0), Exp.int(8))
    end

    test "set/5 creates a blob modify call" do
      expected =
        <<0x95, 0x7F, 0x06, 0x41, 0x95, 0x03, 0x00, 0x08, 0xC4, 0x01, 0xFF, 0x00>> <>
          @blob_bin

      assert %Exp{wire: ^expected} =
               BitExp.set(Exp.blob_bin("bits"), Exp.int(0), Exp.int(8), Exp.blob(<<0xFF>>))
    end
  end

  describe "HLL expressions" do
    test "get_count/1 creates an integer HLL module call" do
      expected = <<0x95, 0x7F, 0x02, 0x02, 0x91, 0x32>> <> @hll_bin

      assert %Exp{wire: ^expected} = HLLExp.get_count(Exp.hll_bin("hll"))
    end

    test "add/5 creates an HLL modify call with expression bit counts" do
      expected =
        <<0x95, 0x7F, 0x09, 0x42, 0x95, 0x01, 0x92, 0x7E, 0x91, 0xA1, "a", 0x0A, 0xFF, 0x00>> <>
          @hll_bin

      assert %Exp{wire: ^expected} =
               HLLExp.add(Exp.hll_bin("hll"), Exp.list(["a"]), Exp.int(10))
    end
  end
end
