defmodule Aerospike.KeyTest do
  use ExUnit.Case, async: true

  alias Aerospike.Key

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
end
