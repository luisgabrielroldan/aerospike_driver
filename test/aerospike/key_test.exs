defmodule Aerospike.KeyTest do
  use ExUnit.Case, async: true

  alias Aerospike.Key

  describe "validate_int64!/2" do
    test "accepts signed int64 bounds" do
      assert Key.validate_int64!(-9_223_372_036_854_775_808, "value") ==
               -9_223_372_036_854_775_808

      assert Key.validate_int64!(9_223_372_036_854_775_807, "value") ==
               9_223_372_036_854_775_807
    end

    test "rejects integers outside the signed int64 range" do
      assert_raise ArgumentError, ~r/value must fit in signed int64/, fn ->
        Key.validate_int64!(9_223_372_036_854_775_808, "value")
      end
    end

    test "rejects non-integer values" do
      assert_raise ArgumentError, ~r/value must be an integer that fits in signed int64/, fn ->
        Key.validate_int64!("42", "value")
      end
    end
  end

  describe "new/3" do
    test "builds integer keys with the Aerospike integer digest layout" do
      assert %Key{namespace: "test", set: "users", user_key: 42, digest: digest} =
               Key.new("test", "users", 42)

      assert digest == :crypto.hash(:ripemd160, <<"users", 1, 42::64-signed-big>>)
    end

    test "builds string keys with the Aerospike string digest layout" do
      assert %Key{namespace: "test", set: "users", user_key: "user:42", digest: digest} =
               Key.new("test", "users", "user:42")

      assert digest == :crypto.hash(:ripemd160, <<"users", 3, "user:42">>)
    end

    test "rejects invalid key inputs" do
      assert_raise ArgumentError, ~r/namespace must be a non-empty string/, fn ->
        Key.new("", "users", "user:42")
      end
    end
  end

  test "from_digest/3 builds a digest-only key" do
    digest = :crypto.hash(:ripemd160, "digest-only")

    assert %Key{
             namespace: "test",
             set: "users",
             user_key: nil,
             digest: ^digest
           } = Key.from_digest("test", "users", digest)
  end

  test "from_digest/3 rejects invalid digests" do
    assert_raise ArgumentError, ~r/digest must be a 20-byte binary/, fn ->
      Key.from_digest("test", "users", <<1, 2, 3>>)
    end
  end

  describe "partition_id/1" do
    test "uses the first 4 digest bytes as a little-endian partition hash" do
      digest = <<1, 2, 3, 4, 0::128>>
      key = Key.from_digest("test", "users", digest)

      assert Key.partition_id(key) == Bitwise.band(0x04030201, 4095)
    end
  end

  describe "coerce!/1" do
    test "passes Aerospike.Key structs through unchanged" do
      key = Key.new("test", "users", "user:42")

      assert Key.coerce!(key) == key
    end

    test "accepts tuple key inputs" do
      assert %Key{namespace: "test", set: "users", user_key: "user:42"} =
               Key.coerce!({"test", "users", "user:42"})
    end

    test "rejects non-key inputs" do
      assert_raise ArgumentError, ~r/expected %Aerospike.Key/, fn ->
        Key.coerce!(:bad)
      end
    end
  end
end
