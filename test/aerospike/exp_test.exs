defmodule Aerospike.ExpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Exp

  # Wire byte references (independently computed from Protocol.ExpTest).
  # int_bin("age")  -> fixarray(3), expOpBIN=81, ExpTypeINT=2, fixstr("age")
  @int_bin_age <<0x93, 0x51, 0x02, 0xA3, 0x61, 0x67, 0x65>>
  # int(21)         -> fixint(21) = <<0x15>>
  @int_21 <<0x15>>
  # int(65)         -> fixint(65) = <<0x41>>
  @int_65 <<0x41>>
  # eq(int_bin_age, int_21) -> fixarray(3), expOpEQ=1, int_bin_age, int_21
  @eq_age_21 <<0x93, 0x01>> <> @int_bin_age <> @int_21

  describe "from_wire/1 regression" do
    test "wraps raw bytes unchanged" do
      raw = <<0xAB, 0xCD>>
      e = Exp.from_wire(raw)
      assert %Exp{wire: ^raw} = e
    end
  end

  describe "integer literals" do
    test "int/1 returns Exp struct with wire bytes" do
      e = Exp.int(42)
      assert %Exp{wire: <<0x2A>>} = e
    end

    test "int/1 handles 0" do
      assert %Exp{wire: <<0x00>>} = Exp.int(0)
    end

    test "int/1 handles negative fixint" do
      assert %Exp{wire: <<0xFF>>} = Exp.int(-1)
    end
  end

  describe "float literals" do
    test "float/1 returns Exp struct with msgpack float64 bytes" do
      e = Exp.float(1.5)
      assert %Exp{wire: <<0xCB, 1.5::64-float-big>>} = e
    end
  end

  describe "string literals" do
    test "str/1 encodes as plain msgpack str" do
      # "hi" -> fixstr(2) = <<0xA2, 0x68, 0x69>>
      assert %Exp{wire: <<0xA2, 0x68, 0x69>>} = Exp.str("hi")
    end
  end

  describe "bool literals" do
    test "bool/1 true" do
      assert %Exp{wire: <<0xC3>>} = Exp.bool(true)
    end

    test "bool/1 false" do
      assert %Exp{wire: <<0xC2>>} = Exp.bool(false)
    end
  end

  describe "nil_ literal" do
    test "nil_/0 encodes as msgpack nil" do
      assert %Exp{wire: <<0xC0>>} = Exp.nil_()
    end
  end

  describe "blob literals" do
    test "blob/1 encodes as msgpack bin format" do
      # <<1, 2, 3>> -> bin8(3) = <<0xC4, 0x03, 0x01, 0x02, 0x03>>
      assert %Exp{wire: <<0xC4, 0x03, 0x01, 0x02, 0x03>>} = Exp.blob(<<1, 2, 3>>)
    end
  end

  describe "val/1 convenience" do
    test "val/1 integer maps to int/1 wire bytes" do
      assert Exp.val(42).wire == Exp.int(42).wire
    end

    test "val/1 float maps to float/1 wire bytes" do
      assert Exp.val(3.14).wire == Exp.float(3.14).wire
    end

    test "val/1 binary maps to str/1 wire bytes" do
      assert Exp.val("foo").wire == Exp.str("foo").wire
    end

    test "val/1 true maps to bool/1 wire bytes" do
      assert Exp.val(true).wire == Exp.bool(true).wire
    end

    test "val/1 false maps to bool/1 wire bytes" do
      assert Exp.val(false).wire == Exp.bool(false).wire
    end

    test "val/1 nil maps to nil_/0 wire bytes" do
      assert Exp.val(nil).wire == Exp.nil_().wire
    end
  end

  describe "bin reads" do
    test "int_bin/1 encodes as [81, 2, name]" do
      assert %Exp{wire: @int_bin_age} = Exp.int_bin("age")
    end

    test "str_bin/1 encodes as [81, 3, name]" do
      # fixstr("name") = <<0xA4, 0x6E, 0x61, 0x6D, 0x65>>; ExpTypeSTRING=3
      expected = <<0x93, 0x51, 0x03, 0xA4, 0x6E, 0x61, 0x6D, 0x65>>
      assert %Exp{wire: ^expected} = Exp.str_bin("name")
    end

    test "float_bin/1 encodes as [81, 7, name]" do
      expected = <<0x93, 0x51, 0x07, 0xA5, 0x70, 0x72, 0x69, 0x63, 0x65>>
      assert %Exp{wire: ^expected} = Exp.float_bin("price")
    end

    test "bool_bin/1 encodes as [81, 1, name]" do
      expected = <<0x93, 0x51, 0x01, 0xA6, 0x61, 0x63, 0x74, 0x69, 0x76, 0x65>>
      assert %Exp{wire: ^expected} = Exp.bool_bin("active")
    end
  end

  describe "metadata" do
    test "ttl/0 returns Exp struct with wire bytes" do
      e = Exp.ttl()
      assert %Exp{wire: <<0x91, 0x45>>} = e
    end

    test "void_time/0 encodes as [68]" do
      assert %Exp{wire: <<0x91, 0x44>>} = Exp.void_time()
    end

    test "last_update/0 encodes as [66]" do
      assert %Exp{wire: <<0x91, 0x42>>} = Exp.last_update()
    end

    test "key_exists/0 encodes as [71]" do
      assert %Exp{wire: <<0x91, 0x47>>} = Exp.key_exists()
    end

    test "set_name/0 encodes as [70]" do
      assert %Exp{wire: <<0x91, 0x46>>} = Exp.set_name()
    end

    test "tombstone?/0 encodes as [72]" do
      assert %Exp{wire: <<0x91, 0x48>>} = Exp.tombstone?()
    end

    test "record_size/0 encodes as [74]" do
      assert %Exp{wire: <<0x91, 0x4A>>} = Exp.record_size()
    end
  end

  describe "comparisons" do
    test "eq/2 composes int_bin and int literal" do
      result = Exp.eq(Exp.int_bin("age"), Exp.int(21))
      assert %Exp{wire: @eq_age_21} = result
    end

    test "gt/2 encodes as [3, left, right]" do
      result = Exp.gt(Exp.int_bin("age"), Exp.int(65))
      expected = <<0x93, 0x03>> <> @int_bin_age <> @int_65
      assert %Exp{wire: ^expected} = result
    end

    test "gte/2 encodes as [4, left, right]" do
      result = Exp.gte(Exp.int_bin("age"), Exp.int(21))
      expected = <<0x93, 0x04>> <> @int_bin_age <> @int_21
      assert %Exp{wire: ^expected} = result
    end

    test "lt/2 encodes as [5, left, right]" do
      result = Exp.lt(Exp.int_bin("age"), Exp.int(21))
      expected = <<0x93, 0x05>> <> @int_bin_age <> @int_21
      assert %Exp{wire: ^expected} = result
    end

    test "lte/2 encodes as [6, left, right]" do
      result = Exp.lte(Exp.int_bin("age"), Exp.int(21))
      expected = <<0x93, 0x06>> <> @int_bin_age <> @int_21
      assert %Exp{wire: ^expected} = result
    end

    test "ne/2 encodes as [2, left, right]" do
      result = Exp.ne(Exp.int_bin("age"), Exp.int(21))
      expected = <<0x93, 0x02>> <> @int_bin_age <> @int_21
      assert %Exp{wire: ^expected} = result
    end
  end

  describe "boolean combinators" do
    test "and_/1 composes two sub-expressions" do
      # eq(int_bin("age"), int(21)) already validated above
      # gt(int_bin("age"), int(65)) = [3, int_bin_age, int_65]
      gt_wire = <<0x93, 0x03>> <> @int_bin_age <> @int_65

      result =
        Exp.and_([
          Exp.eq(Exp.int_bin("age"), Exp.int(21)),
          Exp.gt(Exp.int_bin("age"), Exp.int(65))
        ])

      # and_ -> [16, eq_wire, gt_wire]; fixarray(3)=0x93, expOpAND=16=0x10
      expected = <<0x93, 0x10>> <> @eq_age_21 <> gt_wire
      assert %Exp{wire: ^expected} = result
    end

    test "and_/1 with three sub-expressions uses fixarray(4)" do
      sub = Exp.int(1)
      result = Exp.and_([sub, sub, sub])
      # fixarray(4)=0x94, expOpAND=0x10, fixint(1)=0x01 three times
      assert %Exp{wire: <<0x94, 0x10, 0x01, 0x01, 0x01>>} = result
    end

    test "or_/1 composes two sub-expressions" do
      gt_wire = <<0x93, 0x03>> <> @int_bin_age <> @int_65

      result =
        Exp.or_([
          Exp.eq(Exp.int_bin("age"), Exp.int(21)),
          Exp.gt(Exp.int_bin("age"), Exp.int(65))
        ])

      # or_ -> [17, eq_wire, gt_wire]; expOpOR=17=0x11
      expected = <<0x93, 0x11>> <> @eq_age_21 <> gt_wire
      assert %Exp{wire: ^expected} = result
    end

    test "not_/1 wraps a single sub-expression" do
      eq = Exp.eq(Exp.int_bin("age"), Exp.int(21))
      result = Exp.not_(eq)
      # not_ -> [18, eq_wire]; fixarray(2)=0x92, expOpNOT=18=0x12
      expected = <<0x92, 0x12>> <> @eq_age_21
      assert %Exp{wire: ^expected} = result
    end
  end

  describe "deep composition" do
    test "and_ of multiple comparisons using val/1 produces same wire as typed constructors" do
      expr_typed =
        Exp.and_([
          Exp.gte(Exp.int_bin("age"), Exp.int(18)),
          Exp.lt(Exp.int_bin("age"), Exp.int(65))
        ])

      expr_val =
        Exp.and_([
          Exp.gte(Exp.int_bin("age"), Exp.val(18)),
          Exp.lt(Exp.int_bin("age"), Exp.val(65))
        ])

      assert expr_typed.wire == expr_val.wire
    end
  end
end
