defmodule Aerospike.TxnTest do
  use ExUnit.Case, async: true

  alias Aerospike.Txn

  test "new/0 returns a transaction handle with a signed int64 id" do
    txn = Txn.new()

    assert %Txn{} = txn
    assert is_integer(txn.id)
    assert txn.timeout == 0
  end

  test "new/1 stores the timeout" do
    assert %Txn{timeout: 5_000} = Txn.new(timeout: 5_000)
  end

  test "new/0 IDs stay within the signed int64 range" do
    txn = Txn.new()

    assert txn.id >= -9_223_372_036_854_775_808
    assert txn.id <= 9_223_372_036_854_775_807
  end
end
