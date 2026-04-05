defmodule Aerospike.KeyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Aerospike.Key

  doctest Key

  describe "digest matches known reference values" do
    test "integer 0" do
      k = Key.new("namespace", "set", 0)
      assert Base.encode16(k.digest, case: :lower) == "93d943aae37b017ad7e011b0c1d2e2143c2fb37d"
    end

    test "integer -1" do
      k = Key.new("namespace", "set", -1)
      assert Base.encode16(k.digest, case: :lower) == "22116d253745e29fc63fdf760b6e26f7e197e01d"
    end

    test "integer MinInt64" do
      k = Key.new("namespace", "set", -9_223_372_036_854_775_808)
      assert Base.encode16(k.digest, case: :lower) == "7185c2a47fb02c996daed26b4e01b83240aee9d4"
    end

    test "integer MaxInt64" do
      k = Key.new("namespace", "set", 9_223_372_036_854_775_807)
      assert Base.encode16(k.digest, case: :lower) == "1698328974afa62c8e069860c1516f780d63dbb8"
    end

    test "empty string user key" do
      k = Key.new("namespace", "set", "")
      assert Base.encode16(k.digest, case: :lower) == "2819b1ff6e346a43b4f5f6b77a88bc3eaac22a83"
    end

    test "single character string" do
      k = Key.new("namespace", "set", "s")
      assert Base.encode16(k.digest, case: :lower) == "607cddba7cd111745ef0a3d783d57f0e83c8f311"
    end

    test "empty set name with integer 0" do
      k = Key.new("namespace", "", 0)
      assert Base.encode16(k.digest, case: :lower) == "b346137ba9189579da520162dfdcd61a10fe37d6"
    end
  end

  test "digest is always 20 bytes" do
    k = Key.new("test", "users", "user:42")
    assert byte_size(k.digest) == 20
  end

  describe "partition_id/1" do
    test "matches first digest bytes (namespace,set,0)" do
      k = Key.new("namespace", "set", 0)
      assert Key.partition_id(k) == 2451
    end

    @tag :property
    property "partition_id is always in 0..4095" do
      check all(
              ns <- string(:ascii),
              ns != "",
              set <- string(:ascii),
              uk <- one_of([string(:ascii), integer(-1000..1000)]),
              max_runs: 100
            ) do
        k = Key.new(ns, set, uk)
        pid = Key.partition_id(k)
        assert pid >= 0 and pid <= 4095
      end
    end
  end

  test "raises on empty namespace" do
    assert_raise ArgumentError, fn -> Key.new("", "set", 1) end
  end

  test "raises when user_key integer out of int64 range" do
    assert_raise ArgumentError, fn -> Key.new("ns", "set", 9_223_372_036_854_775_808) end
  end

  test "from_digest raises on empty namespace" do
    d = :crypto.hash(:ripemd160, <<>>)
    assert_raise ArgumentError, fn -> Key.from_digest("", "set", d) end
  end

  test "from_digest raises on wrong digest size" do
    assert_raise ArgumentError, fn -> Key.from_digest("ns", "set", <<0::80>>) end
  end

  test "int64? returns false for non-integer" do
    refute Key.int64?("hello")
    refute Key.int64?(3.14)
  end

  describe "property" do
    @tag :property
    property "digest is deterministic and 20 bytes for string keys" do
      check all(
              ns <- string(:ascii),
              ns != "",
              set <- string(:ascii),
              uk <- string(:ascii),
              max_runs: 50
            ) do
        k1 = Key.new(ns, set, uk)
        k2 = Key.new(ns, set, uk)
        assert k1.digest == k2.digest
        assert byte_size(k1.digest) == 20
      end
    end

    @tag :property
    property "digest is deterministic for integers in range" do
      check all(
              ns <- string(:ascii),
              ns != "",
              set <- string(:ascii),
              uk <- integer(-1000..1000),
              max_runs: 50
            ) do
        k1 = Key.new(ns, set, uk)
        k2 = Key.new(ns, set, uk)
        assert k1.digest == k2.digest
        assert byte_size(k1.digest) == 20
      end
    end
  end
end
