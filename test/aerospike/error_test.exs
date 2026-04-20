defmodule Aerospike.ErrorTest do
  use ExUnit.Case, async: true

  doctest Aerospike.Error

  alias Aerospike.Error

  describe "from_result_code/2" do
    test "builds an error from a known result code atom" do
      err = Error.from_result_code(:key_not_found)

      assert %Error{code: :key_not_found, in_doubt: false, node: nil} = err
      assert err.message == "Key not found"
    end

    test "applies :message, :node, and :in_doubt overrides" do
      err =
        Error.from_result_code(:timeout,
          message: "read deadline exceeded",
          node: "BB9",
          in_doubt: true
        )

      assert err.code == :timeout
      assert err.message == "read deadline exceeded"
      assert err.node == "BB9"
      assert err.in_doubt == true
    end
  end

  describe "rebalance?/1" do
    test "returns true for :partition_unavailable" do
      err = Error.from_result_code(:partition_unavailable)
      assert Error.rebalance?(err) == true
    end

    test "returns false for common server errors" do
      refute Error.rebalance?(Error.from_result_code(:key_not_found))
      refute Error.rebalance?(Error.from_result_code(:generation_error))
      refute Error.rebalance?(Error.from_result_code(:record_too_big))
    end

    test "returns false for transport-class errors" do
      refute Error.rebalance?(Error.from_result_code(:timeout))
      refute Error.rebalance?(Error.from_result_code(:network_error))
      refute Error.rebalance?(Error.from_result_code(:cluster_not_ready))
    end

    test "returns false for non-Error values so callers can match uniformly" do
      refute Error.rebalance?(:cluster_not_ready)
      refute Error.rebalance?(:no_master)
      refute Error.rebalance?(:unknown_node)
      refute Error.rebalance?(nil)
      refute Error.rebalance?({:error, :whatever})
    end

    test "is unaffected by :node and :in_doubt fields" do
      err = Error.from_result_code(:partition_unavailable, node: "BB9", in_doubt: true)
      assert Error.rebalance?(err) == true

      err = Error.from_result_code(:timeout, node: "BB9", in_doubt: true)
      refute Error.rebalance?(err)
    end
  end

  describe "Exception.message/1" do
    test "renders code and message" do
      err = Error.from_result_code(:partition_unavailable)

      assert Exception.message(err) ==
               "Aerospike error partition_unavailable: Partition not available"
    end
  end
end
