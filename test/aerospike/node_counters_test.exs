defmodule Aerospike.NodeCountersTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.NodeCounters

  describe "new/0" do
    test "returns a counters reference with every slot at zero" do
      ref = NodeCounters.new()

      assert NodeCounters.in_flight(ref) == 0
      assert NodeCounters.queued(ref) == 0
      assert NodeCounters.failed(ref) == 0
    end

    test "independent refs do not share state" do
      ref_a = NodeCounters.new()
      ref_b = NodeCounters.new()

      NodeCounters.incr_in_flight(ref_a)

      assert NodeCounters.in_flight(ref_a) == 1
      assert NodeCounters.in_flight(ref_b) == 0
    end
  end

  describe "in_flight slot" do
    test "incr/decr are inverses" do
      ref = NodeCounters.new()

      NodeCounters.incr_in_flight(ref)
      NodeCounters.incr_in_flight(ref)
      assert NodeCounters.in_flight(ref) == 2

      NodeCounters.decr_in_flight(ref)
      assert NodeCounters.in_flight(ref) == 1

      NodeCounters.decr_in_flight(ref)
      assert NodeCounters.in_flight(ref) == 0
    end
  end

  describe "failed slot" do
    test "incr bumps the slot" do
      ref = NodeCounters.new()

      NodeCounters.incr_failed(ref)
      NodeCounters.incr_failed(ref)
      NodeCounters.incr_failed(ref)

      assert NodeCounters.failed(ref) == 3
    end

    test "reset_failed zeroes the slot without touching in_flight" do
      ref = NodeCounters.new()

      NodeCounters.incr_in_flight(ref)
      NodeCounters.incr_failed(ref)
      NodeCounters.incr_failed(ref)

      assert NodeCounters.failed(ref) == 2
      assert NodeCounters.in_flight(ref) == 1

      NodeCounters.reset_failed(ref)

      assert NodeCounters.failed(ref) == 0
      # in_flight survives so an in-progress checkout is still accounted for.
      assert NodeCounters.in_flight(ref) == 1
    end
  end

  describe "failure?/1 classification" do
    test "transport-class error codes are failures" do
      for code <- [:network_error, :timeout, :connection_error] do
        err = %Error{code: code, message: "x"}

        assert NodeCounters.failure?({:error, err}),
               "expected #{inspect(code)} to be a transport failure"
      end
    end

    test "rebalance-class and server-side errors are not failures" do
      refute NodeCounters.failure?({:error, Error.from_result_code(:partition_unavailable)})
      refute NodeCounters.failure?({:error, Error.from_result_code(:key_not_found)})
      refute NodeCounters.failure?({:error, Error.from_result_code(:generation_error)})
      refute NodeCounters.failure?({:error, %Error{code: :parse_error, message: "bad reply"}})
    end

    test "pool-level errors are not failures" do
      # Checkout-path failures (no node-health signal — fun never ran).
      refute NodeCounters.failure?({:error, Error.from_result_code(:pool_timeout)})
      refute NodeCounters.failure?({:error, Error.from_result_code(:invalid_node)})
    end

    test "non-error shapes are not failures" do
      refute NodeCounters.failure?(:ok)
      refute NodeCounters.failure?({:ok, "body"})
      refute NodeCounters.failure?({:error, :cluster_not_ready})
      refute NodeCounters.failure?(nil)
    end
  end
end
