defmodule Aerospike.RetryPolicyTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.RetryPolicy

  describe "defaults/0" do
    test "returns the Tier 2 default policy" do
      assert RetryPolicy.defaults() == %{
               max_retries: 2,
               sleep_between_retries_ms: 0,
               replica_policy: :sequence
             }
    end
  end

  describe "from_opts/1" do
    test "overlays caller keywords on top of defaults" do
      policy =
        RetryPolicy.from_opts(
          max_retries: 5,
          sleep_between_retries_ms: 25,
          replica_policy: :master
        )

      assert policy == %{
               max_retries: 5,
               sleep_between_retries_ms: 25,
               replica_policy: :master
             }
    end

    test "ignores unknown keys so future knobs can coexist with Tier 2" do
      policy = RetryPolicy.from_opts(future_knob: 42, max_retries: 1)
      assert policy.max_retries == 1
      assert policy.replica_policy == :sequence
    end

    test "rejects a negative :max_retries" do
      assert_raise ArgumentError, ~r/:max_retries/, fn ->
        RetryPolicy.from_opts(max_retries: -1)
      end
    end

    test "rejects an unknown :replica_policy" do
      assert_raise ArgumentError, ~r/:replica_policy/, fn ->
        RetryPolicy.from_opts(replica_policy: :any)
      end
    end
  end

  describe "put/2 + load/1" do
    test "write-then-read round-trips the policy through ETS" do
      tab = :"retry_policy_meta_#{System.unique_integer([:positive])}"
      :ets.new(tab, [:set, :public, :named_table])

      try do
        policy = %{max_retries: 3, sleep_between_retries_ms: 5, replica_policy: :master}
        assert RetryPolicy.put(tab, policy) == true
        assert RetryPolicy.load(tab) == policy
      after
        :ets.delete(tab)
      end
    end

    test "load/1 falls back to defaults when the slot is absent" do
      tab = :"retry_policy_empty_#{System.unique_integer([:positive])}"
      :ets.new(tab, [:set, :public, :named_table])

      try do
        assert RetryPolicy.load(tab) == RetryPolicy.defaults()
      after
        :ets.delete(tab)
      end
    end
  end

  describe "merge/2" do
    test "applies per-call overrides on top of the base policy" do
      base = %{max_retries: 2, sleep_between_retries_ms: 0, replica_policy: :sequence}

      merged = RetryPolicy.merge(base, max_retries: 7, replica_policy: :master)

      assert merged == %{
               max_retries: 7,
               sleep_between_retries_ms: 0,
               replica_policy: :master
             }
    end

    test "returns the base policy verbatim when opts is empty" do
      base = %{max_retries: 1, sleep_between_retries_ms: 50, replica_policy: :master}
      assert RetryPolicy.merge(base, []) == base
    end
  end

  describe "rebalance?/1" do
    test "delegates to Aerospike.Error.rebalance?/1 for PARTITION_UNAVAILABLE" do
      err = Error.from_result_code(:partition_unavailable)
      assert RetryPolicy.rebalance?({:error, err})
      # The classification helper accepts either a bare Error struct or
      # the `{:error, _}` tuple form; Aerospike.Error.rebalance?/1 unwraps
      # the struct path. Both must agree.
      assert RetryPolicy.rebalance?(err)
    end

    test "returns false for transport, server-logical, and routing atoms" do
      refute RetryPolicy.rebalance?({:error, Error.from_result_code(:network_error)})
      refute RetryPolicy.rebalance?({:error, Error.from_result_code(:key_not_found)})
      refute RetryPolicy.rebalance?({:error, :cluster_not_ready})
      refute RetryPolicy.rebalance?(:no_master)
    end
  end

  describe "transport?/1" do
    test "matches the transport-class result codes the pool and breaker emit" do
      for code <- [
            :network_error,
            :timeout,
            :connection_error,
            :pool_timeout,
            :invalid_node,
            :circuit_open
          ] do
        assert RetryPolicy.transport?({:error, Error.from_result_code(code)}),
               "expected #{inspect(code)} to be transport-class"
      end
    end

    test "returns false for rebalance and server-logical codes" do
      refute RetryPolicy.transport?({:error, Error.from_result_code(:partition_unavailable)})
      refute RetryPolicy.transport?({:error, Error.from_result_code(:key_not_found)})
    end

    test "returns false for routing atoms and non-error terms" do
      refute RetryPolicy.transport?({:error, :cluster_not_ready})
      refute RetryPolicy.transport?(:ok)
      refute RetryPolicy.transport?({:ok, %{}})
    end
  end
end
