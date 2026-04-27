defmodule Aerospike.ExpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Exp

  @int_bin_age <<0x93, 0x51, 0x02, 0xA3, "age">>
  @int_21 <<0x15>>
  @int_65 <<0x41>>
  @eq_age_21 <<0x93, 0x01>> <> @int_bin_age <> @int_21

  describe "from_wire/1" do
    test "wraps raw bytes unchanged" do
      raw = <<0xAB, 0xCD>>

      assert %Exp{wire: ^raw} = Exp.from_wire(raw)
    end
  end

  describe "base64/1" do
    test "encodes non-empty expression bytes" do
      assert Exp.base64(Exp.from_wire(<<1, 2, 3>>)) == {:ok, "AQID"}
    end

    test "rejects empty expression bytes" do
      assert Exp.base64(Exp.from_wire("")) == {:error, :empty}
    end
  end

  describe "literal builders" do
    test "int/1 returns integer expression bytes" do
      assert %Exp{wire: <<0x2A>>} = Exp.int(42)
      assert %Exp{wire: <<0x00>>} = Exp.int(0)
      assert %Exp{wire: <<0xFF>>} = Exp.int(-1)
    end

    test "float/1 returns float64 expression bytes" do
      assert %Exp{wire: <<0xCB, 1.5::64-float-big>>} = Exp.float(1.5)
    end

    test "str/1 returns MessagePack string bytes" do
      assert %Exp{wire: <<0xA2, "hi">>} = Exp.str("hi")
    end

    test "bool/1 returns boolean expression bytes" do
      assert %Exp{wire: <<0xC3>>} = Exp.bool(true)
      assert %Exp{wire: <<0xC2>>} = Exp.bool(false)
    end

    test "nil_/0 returns nil expression bytes" do
      assert %Exp{wire: <<0xC0>>} = Exp.nil_()
    end

    test "blob/1 returns MessagePack binary bytes" do
      assert %Exp{wire: <<0xC4, 0x03, 0x01, 0x02, 0x03>>} = Exp.blob(<<1, 2, 3>>)
    end

    test "compound and special value builders return expression bytes" do
      assert %Exp{wire: <<0xD9, 0x03, 0x17, "{}">>} = Exp.geo("{}")
      assert %Exp{wire: <<0x92, 0x7E, 0x92, 0x01, 0x02>>} = Exp.list([1, 2])
      assert %Exp{wire: <<0x81, 0xA1, "a", 0x01>>} = Exp.map(%{"a" => 1})
      assert %Exp{wire: <<0xD4, 0xFF, 0x01>>} = Exp.infinity()
      assert %Exp{wire: <<0xD4, 0xFF, 0x00>>} = Exp.wildcard()
    end
  end

  describe "val/1" do
    test "maps supported Elixir values to typed literals" do
      assert Exp.val(42).wire == Exp.int(42).wire
      assert Exp.val(3.14).wire == Exp.float(3.14).wire
      assert Exp.val("foo").wire == Exp.str("foo").wire
      assert Exp.val(true).wire == Exp.bool(true).wire
      assert Exp.val(false).wire == Exp.bool(false).wire
      assert Exp.val(nil).wire == Exp.nil_().wire
      assert Exp.val([1, 2]).wire == Exp.list([1, 2]).wire
      assert Exp.val(%{"a" => 1}).wire == Exp.map(%{"a" => 1}).wire
    end
  end

  describe "bin reads" do
    test "key/1 returns typed key read bytes" do
      assert %Exp{wire: <<0x92, 0x50, 0x02>>} = Exp.key(:int)
    end

    test "int_bin/1 returns typed bin read bytes" do
      assert %Exp{wire: @int_bin_age} = Exp.int_bin("age")
    end

    test "str_bin/1 returns typed bin read bytes" do
      expected = <<0x93, 0x51, 0x03, 0xA4, "name">>

      assert %Exp{wire: ^expected} = Exp.str_bin("name")
    end

    test "float_bin/1 returns typed bin read bytes" do
      expected = <<0x93, 0x51, 0x07, 0xA5, "price">>

      assert %Exp{wire: ^expected} = Exp.float_bin("price")
    end

    test "bool_bin/1 returns typed bin read bytes" do
      expected = <<0x93, 0x51, 0x01, 0xA6, "active">>

      assert %Exp{wire: ^expected} = Exp.bool_bin("active")
    end

    test "blob_bin/1 returns typed bin read bytes" do
      expected = <<0x93, 0x51, 0x06, 0xA7, "payload">>

      assert %Exp{wire: ^expected} = Exp.blob_bin("payload")
    end

    test "CDT and geo bin builders return typed bin read bytes" do
      assert %Exp{wire: <<0x93, 0x51, 0x08, 0xA6, "region">>} = Exp.geo_bin("region")
      assert %Exp{wire: <<0x93, 0x51, 0x04, 0xA5, "items">>} = Exp.list_bin("items")
      assert %Exp{wire: <<0x93, 0x51, 0x05, 0xA5, "prefs">>} = Exp.map_bin("prefs")
      assert %Exp{wire: <<0x93, 0x51, 0x09, 0xA3, "hll">>} = Exp.hll_bin("hll")
    end

    test "bin type helpers return expression bytes" do
      assert Exp.particle_type(:null) == 0
      assert Exp.particle_type(:list) == 20
      assert %Exp{wire: <<0x92, 0x52, 0xA3, "age">>} = Exp.bin_type("age")

      expected = <<0x93, 0x02, 0x92, 0x52, 0xA3, "age", 0x00>>

      assert %Exp{wire: ^expected} = Exp.bin_exists("age")
    end
  end

  describe "metadata reads" do
    test "return expression bytes" do
      assert %Exp{wire: <<0x91, 0x45>>} = Exp.ttl()
      assert %Exp{wire: <<0x91, 0x44>>} = Exp.void_time()
      assert %Exp{wire: <<0x91, 0x42>>} = Exp.last_update()
      assert %Exp{wire: <<0x91, 0x43>>} = Exp.since_update()
      assert %Exp{wire: <<0x91, 0x47>>} = Exp.key_exists()
      assert %Exp{wire: <<0x91, 0x46>>} = Exp.set_name()
      assert %Exp{wire: <<0x91, 0x48>>} = Exp.tombstone?()
      assert %Exp{wire: <<0x91, 0x4A>>} = Exp.record_size()
      assert %Exp{wire: <<0x92, 0x40, 0x03>>} = Exp.digest_modulo(3)
      assert %Exp{wire: <<0x91, 0x00>>} = Exp.unknown()
      assert %Exp{wire: <<0x91, 0x64>>} = Exp.remove_result()
    end
  end

  describe "comparisons" do
    test "eq/2 composes two expressions" do
      assert %Exp{wire: @eq_age_21} = Exp.eq(Exp.int_bin("age"), Exp.int(21))
    end

    test "ne/2 composes two expressions" do
      expected = <<0x93, 0x02>> <> @int_bin_age <> @int_21

      assert %Exp{wire: ^expected} = Exp.ne(Exp.int_bin("age"), Exp.int(21))
    end

    test "gt/2 composes two expressions" do
      expected = <<0x93, 0x03>> <> @int_bin_age <> @int_65

      assert %Exp{wire: ^expected} = Exp.gt(Exp.int_bin("age"), Exp.int(65))
    end

    test "gte/2 composes two expressions" do
      expected = <<0x93, 0x04>> <> @int_bin_age <> @int_21

      assert %Exp{wire: ^expected} = Exp.gte(Exp.int_bin("age"), Exp.int(21))
    end

    test "lt/2 composes two expressions" do
      expected = <<0x93, 0x05>> <> @int_bin_age <> @int_21

      assert %Exp{wire: ^expected} = Exp.lt(Exp.int_bin("age"), Exp.int(21))
    end

    test "lte/2 composes two expressions" do
      expected = <<0x93, 0x06>> <> @int_bin_age <> @int_21

      assert %Exp{wire: ^expected} = Exp.lte(Exp.int_bin("age"), Exp.int(21))
    end
  end

  describe "boolean combinators" do
    test "and_/1 composes two or more expressions" do
      gt_age_65 = <<0x93, 0x03>> <> @int_bin_age <> @int_65
      expected = <<0x93, 0x10>> <> @eq_age_21 <> gt_age_65

      result =
        Exp.and_([
          Exp.eq(Exp.int_bin("age"), Exp.int(21)),
          Exp.gt(Exp.int_bin("age"), Exp.int(65))
        ])

      assert %Exp{wire: ^expected} = result

      assert %Exp{wire: <<0x94, 0x10, 0x01, 0x01, 0x01>>} =
               Exp.and_([Exp.int(1), Exp.int(1), Exp.int(1)])
    end

    test "or_/1 composes two or more expressions" do
      gt_age_65 = <<0x93, 0x03>> <> @int_bin_age <> @int_65
      expected = <<0x93, 0x11>> <> @eq_age_21 <> gt_age_65

      result =
        Exp.or_([
          Exp.eq(Exp.int_bin("age"), Exp.int(21)),
          Exp.gt(Exp.int_bin("age"), Exp.int(65))
        ])

      assert %Exp{wire: ^expected} = result
    end

    test "not_/1 wraps one expression" do
      expected = <<0x92, 0x12>> <> @eq_age_21

      assert %Exp{wire: ^expected} = Exp.not_(Exp.eq(Exp.int_bin("age"), Exp.int(21)))
    end

    test "exclusive/1 composes two or more expressions" do
      gt_age_65 = <<0x93, 0x03>> <> @int_bin_age <> @int_65
      expected = <<0x93, 0x13>> <> @eq_age_21 <> gt_age_65

      result =
        Exp.exclusive([
          Exp.eq(Exp.int_bin("age"), Exp.int(21)),
          Exp.gt(Exp.int_bin("age"), Exp.int(65))
        ])

      assert %Exp{wire: ^expected} = result
    end
  end

  describe "core expression operators" do
    test "regex and geo comparisons return expression bytes" do
      assert Exp.regex_flag(:icase) == 2
      assert Exp.regex_flags([:icase, :newline]) == 10

      assert %Exp{wire: <<0x94, 0x07, 0x02, 0xA2, "^a">> <> @int_bin_age} =
               Exp.regex_compare("^a", Exp.regex_flag(:icase), Exp.int_bin("age"))

      assert %Exp{wire: <<0x93, 0x08>> <> @int_bin_age <> <<0xD9, 0x03, 0x17, "{}">>} =
               Exp.geo_compare(Exp.int_bin("age"), Exp.geo("{}"))
    end

    test "numeric operators return expression bytes" do
      assert %Exp{wire: <<0x93, 0x14, 0x01, 0x02>>} = Exp.add([Exp.int(1), Exp.int(2)])
      assert %Exp{wire: <<0x93, 0x15, 0x01, 0x02>>} = Exp.sub([Exp.int(1), Exp.int(2)])
      assert %Exp{wire: <<0x93, 0x16, 0x01, 0x02>>} = Exp.mul([Exp.int(1), Exp.int(2)])
      assert %Exp{wire: <<0x93, 0x17, 0x01, 0x02>>} = Exp.div_([Exp.int(1), Exp.int(2)])
      assert %Exp{wire: <<0x93, 0x18, 0x01, 0x02>>} = Exp.pow(Exp.int(1), Exp.int(2))
      assert %Exp{wire: <<0x93, 0x19, 0x01, 0x02>>} = Exp.log(Exp.int(1), Exp.int(2))
      assert %Exp{wire: <<0x93, 0x1A, 0x01, 0x02>>} = Exp.mod(Exp.int(1), Exp.int(2))
      assert %Exp{wire: <<0x92, 0x1B, 0x01>>} = Exp.abs(Exp.int(1))
      assert %Exp{wire: <<0x92, 0x1C, 0x01>>} = Exp.floor(Exp.int(1))
      assert %Exp{wire: <<0x92, 0x1D, 0x01>>} = Exp.ceil(Exp.int(1))
      assert %Exp{wire: <<0x92, 0x1E, 0x01>>} = Exp.to_int(Exp.int(1))
      assert %Exp{wire: <<0x92, 0x1F, 0x01>>} = Exp.to_float(Exp.int(1))
      assert %Exp{wire: <<0x93, 0x32, 0x01, 0x02>>} = Exp.min([Exp.int(1), Exp.int(2)])
      assert %Exp{wire: <<0x93, 0x33, 0x01, 0x02>>} = Exp.max([Exp.int(1), Exp.int(2)])
    end

    test "integer bitwise operators return expression bytes" do
      assert %Exp{wire: <<0x93, 0x20, 0x01, 0x02>>} = Exp.int_and([Exp.int(1), Exp.int(2)])
      assert %Exp{wire: <<0x93, 0x21, 0x01, 0x02>>} = Exp.int_or([Exp.int(1), Exp.int(2)])
      assert %Exp{wire: <<0x93, 0x22, 0x01, 0x02>>} = Exp.int_xor([Exp.int(1), Exp.int(2)])
      assert %Exp{wire: <<0x92, 0x23, 0x01>>} = Exp.int_not(Exp.int(1))
      assert %Exp{wire: <<0x93, 0x24, 0x01, 0x02>>} = Exp.int_lshift(Exp.int(1), Exp.int(2))
      assert %Exp{wire: <<0x93, 0x25, 0x01, 0x02>>} = Exp.int_rshift(Exp.int(1), Exp.int(2))
      assert %Exp{wire: <<0x93, 0x26, 0x01, 0x02>>} = Exp.int_arshift(Exp.int(1), Exp.int(2))
      assert %Exp{wire: <<0x92, 0x27, 0x01>>} = Exp.int_count(Exp.int(1))
      assert %Exp{wire: <<0x93, 0x28, 0x01, 0xC3>>} = Exp.int_lscan(Exp.int(1), Exp.bool(true))
      assert %Exp{wire: <<0x93, 0x29, 0x01, 0xC3>>} = Exp.int_rscan(Exp.int(1), Exp.bool(true))
    end

    test "conditional and scoped variable helpers return expression bytes" do
      assert %Exp{wire: <<0x94, 0x7B, 0x01, 0x02, 0x03>>} =
               Exp.cond_([Exp.int(1), Exp.int(2), Exp.int(3)])

      assert %Exp{wire: <<0x92, 0x7C, 0xA1, "x">>} = Exp.var("x")
      assert %Exp{wire: <<0xA1, "x", 0x01>>} = Exp.def_("x", Exp.int(1))

      expected = <<0x94, 0x7D, 0xA1, "x", 0x01, 0x92, 0x7C, 0xA1, "x">>

      assert %Exp{wire: ^expected} = Exp.let([Exp.def_("x", Exp.int(1)), Exp.var("x")])
    end

    test "loop variable helpers return expression bytes" do
      assert Exp.loop_var_part(:value) == 1
      assert %Exp{wire: <<0x93, 0x7A, 0x02, 0x01>>} = Exp.loop_var(:int, :value)
      assert %Exp{wire: <<0x93, 0x7A, 0x00, 0x00>>} = Exp.nil_loop_var(:map_key)
      assert %Exp{wire: <<0x93, 0x7A, 0x01, 0x01>>} = Exp.bool_loop_var(:value)
      assert %Exp{wire: <<0x93, 0x7A, 0x02, 0x02>>} = Exp.int_loop_var(:index)
      assert %Exp{wire: <<0x93, 0x7A, 0x07, 0x01>>} = Exp.float_loop_var(:value)
      assert %Exp{wire: <<0x93, 0x7A, 0x03, 0x01>>} = Exp.str_loop_var(:value)
      assert %Exp{wire: <<0x93, 0x7A, 0x06, 0x01>>} = Exp.blob_loop_var(:value)
      assert %Exp{wire: <<0x93, 0x7A, 0x04, 0x01>>} = Exp.list_loop_var(:value)
      assert %Exp{wire: <<0x93, 0x7A, 0x05, 0x00>>} = Exp.map_loop_var(:map_key)
      assert %Exp{wire: <<0x93, 0x7A, 0x08, 0x01>>} = Exp.geo_loop_var(:value)
      assert %Exp{wire: <<0x93, 0x7A, 0x09, 0x01>>} = Exp.hll_loop_var(:value)
    end
  end

  describe "composition" do
    test "val/1 and typed literals produce the same nested expression bytes" do
      typed =
        Exp.and_([
          Exp.gte(Exp.int_bin("age"), Exp.int(18)),
          Exp.lt(Exp.int_bin("age"), Exp.int(65))
        ])

      inferred =
        Exp.and_([
          Exp.gte(Exp.int_bin("age"), Exp.val(18)),
          Exp.lt(Exp.int_bin("age"), Exp.val(65))
        ])

      assert typed.wire == inferred.wire
    end
  end
end
