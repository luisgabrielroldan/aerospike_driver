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
  end

  describe "val/1" do
    test "maps supported Elixir values to typed literals" do
      assert Exp.val(42).wire == Exp.int(42).wire
      assert Exp.val(3.14).wire == Exp.float(3.14).wire
      assert Exp.val("foo").wire == Exp.str("foo").wire
      assert Exp.val(true).wire == Exp.bool(true).wire
      assert Exp.val(false).wire == Exp.bool(false).wire
      assert Exp.val(nil).wire == Exp.nil_().wire
    end
  end

  describe "bin reads" do
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
  end

  describe "metadata reads" do
    test "return expression bytes" do
      assert %Exp{wire: <<0x91, 0x45>>} = Exp.ttl()
      assert %Exp{wire: <<0x91, 0x44>>} = Exp.void_time()
      assert %Exp{wire: <<0x91, 0x42>>} = Exp.last_update()
      assert %Exp{wire: <<0x91, 0x47>>} = Exp.key_exists()
      assert %Exp{wire: <<0x91, 0x46>>} = Exp.set_name()
      assert %Exp{wire: <<0x91, 0x48>>} = Exp.tombstone?()
      assert %Exp{wire: <<0x91, 0x4A>>} = Exp.record_size()
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
