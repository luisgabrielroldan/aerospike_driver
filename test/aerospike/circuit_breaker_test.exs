defmodule Aerospike.Cluster.CircuitBreakerTest do
  use ExUnit.Case, async: true

  alias Aerospike.Cluster.CircuitBreaker
  alias Aerospike.Cluster.NodeCounters
  alias Aerospike.Error

  @default_opts %{
    circuit_open_threshold: 3,
    max_concurrent_ops_per_node: 5
  }

  describe "allow?/2 happy path" do
    test "below every threshold returns :ok" do
      ref = NodeCounters.new()

      assert :ok = CircuitBreaker.allow?(ref, @default_opts)
    end

    test "does not mutate counter slots" do
      ref = NodeCounters.new()
      NodeCounters.incr_in_flight(ref)
      NodeCounters.incr_failed(ref)

      :ok = CircuitBreaker.allow?(ref, @default_opts)

      assert NodeCounters.in_flight(ref) == 1
      assert NodeCounters.failed(ref) == 1
    end
  end

  describe "allow?/2 failure threshold" do
    test "refuses when `:failed` reaches the threshold" do
      ref = NodeCounters.new()

      for _ <- 1..@default_opts.circuit_open_threshold do
        NodeCounters.incr_failed(ref)
      end

      assert {:error, %Error{code: :circuit_open, message: msg}} =
               CircuitBreaker.allow?(ref, @default_opts)

      # The message names the failing dimension so downstream logs can
      # discriminate between "too many recent failures" and "too many
      # concurrent commands" without re-reading counter slots.
      assert msg =~ "failure counter"
      assert msg =~ "circuit_open_threshold"
    end

    test "allows again once the Tender resets the slot" do
      ref = NodeCounters.new()

      for _ <- 1..@default_opts.circuit_open_threshold do
        NodeCounters.incr_failed(ref)
      end

      assert {:error, %Error{code: :circuit_open}} =
               CircuitBreaker.allow?(ref, @default_opts)

      # `reset_failed/1` is the Tender's "successful tend cycle" hook;
      # the breaker must be lock-step with that reset so the next caller
      # sees a usable node immediately.
      NodeCounters.reset_failed(ref)

      assert :ok = CircuitBreaker.allow?(ref, @default_opts)
    end

    test "a threshold of zero refuses any attempt" do
      ref = NodeCounters.new()
      opts = %{@default_opts | circuit_open_threshold: 0}

      assert {:error, %Error{code: :circuit_open}} = CircuitBreaker.allow?(ref, opts)
    end
  end

  describe "allow?/2 concurrency threshold" do
    test "refuses when `in_flight` reaches the concurrency cap" do
      ref = NodeCounters.new()

      for _ <- 1..@default_opts.max_concurrent_ops_per_node do
        NodeCounters.incr_in_flight(ref)
      end

      assert {:error, %Error{code: :circuit_open, message: msg}} =
               CircuitBreaker.allow?(ref, @default_opts)

      assert msg =~ "in_flight"
      assert msg =~ "max_concurrent_ops_per_node"
    end

    test "draining in-flight opens the circuit again" do
      ref = NodeCounters.new()

      for _ <- 1..@default_opts.max_concurrent_ops_per_node do
        NodeCounters.incr_in_flight(ref)
      end

      assert {:error, %Error{}} = CircuitBreaker.allow?(ref, @default_opts)

      NodeCounters.decr_in_flight(ref)

      assert :ok = CircuitBreaker.allow?(ref, @default_opts)
    end
  end

  describe "allow?/2 combined caps" do
    test "reports the failure cap first when both are breached" do
      # Assertion pins the reporting order so downstream logs are stable:
      # the failure window is the stronger signal (node-health) than the
      # concurrency window (load-shedding), so it wins the `cond` branch.
      ref = NodeCounters.new()

      for _ <- 1..@default_opts.circuit_open_threshold do
        NodeCounters.incr_failed(ref)
      end

      for _ <- 1..@default_opts.max_concurrent_ops_per_node do
        NodeCounters.incr_in_flight(ref)
      end

      assert {:error, %Error{code: :circuit_open, message: msg}} =
               CircuitBreaker.allow?(ref, @default_opts)

      assert msg =~ "failure counter"
      refute msg =~ "in_flight"
    end
  end
end
