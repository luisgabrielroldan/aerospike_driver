defmodule Aerospike.TxnTest do
  use ExUnit.Case, async: true

  alias Aerospike.Txn

  doctest Txn

  @min_int64 -9_223_372_036_854_775_808
  @max_int64 9_223_372_036_854_775_807

  test "new/0 returns a Txn with non-zero id and default timeout" do
    txn = Txn.new()
    assert %Txn{} = txn
    assert is_integer(txn.id)
    refute txn.id == 0
    assert txn.timeout == 0
  end

  test "new/0 id is in signed int64 range" do
    txn = Txn.new()
    assert txn.id >= @min_int64
    assert txn.id <= @max_int64
  end

  test "new/1 sets the timeout" do
    txn = Txn.new(timeout: 5_000)
    assert txn.timeout == 5_000
  end

  test "new/1 defaults timeout to 0 when not provided" do
    txn = Txn.new([])
    assert txn.timeout == 0
  end

  test "two successive new/0 calls produce different IDs" do
    txn1 = Txn.new()
    txn2 = Txn.new()
    # Probability of collision is 1 / 2^64 — effectively impossible
    refute txn1.id == txn2.id
  end
end
