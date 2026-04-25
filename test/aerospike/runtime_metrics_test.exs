defmodule Aerospike.RuntimeMetricsTest do
  use ExUnit.Case, async: false

  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Error
  alias Aerospike.RuntimeMetrics

  describe "unavailable clusters" do
    test "return default stats and typed enable/disable errors" do
      name = unique_name()

      assert RuntimeMetrics.stats(name).metrics_enabled == false
      assert RuntimeMetrics.stats(name).commands_total == 0

      assert {:error, %Error{code: :cluster_not_ready}} = RuntimeMetrics.enable(name)
      assert {:error, %Error{code: :cluster_not_ready}} = RuntimeMetrics.disable(name)
      refute RuntimeMetrics.metrics_enabled?(name)
    end
  end

  describe "counter recording" do
    test "ignores record calls while disabled and preserves nil configs" do
      name = start_table_owner!()

      assert :ok = RuntimeMetrics.init(name)
      assert :ok = RuntimeMetrics.record_command(name, Aerospike.Command.Get, now(), :ok)
      assert :ok = RuntimeMetrics.record_connection_attempt(name, nil)

      stats = RuntimeMetrics.stats(name)

      assert stats.cluster.config == %{pool_size: nil, tend_interval_ms: nil}
      assert stats.commands_total == 0
      assert stats.cluster.connections.attempts == 0
    end

    test "tracks command result shapes and clears runtime counters on reset" do
      name = start_table_owner!()

      assert :ok = RuntimeMetrics.init(name, pool_size: 2, tend_interval_ms: 500)
      assert :ok = RuntimeMetrics.enable(name)

      assert :ok = RuntimeMetrics.record_command(name, Aerospike.Command.Get, now(), :ok)

      assert :ok =
               RuntimeMetrics.record_command(
                 name,
                 Aerospike.Command.Put,
                 now(),
                 {:error, :timeout}
               )

      assert :ok =
               RuntimeMetrics.record_command(
                 name,
                 Aerospike.Command.Delete,
                 now(),
                 Error.from_result_code(:key_not_found)
               )

      assert :ok = RuntimeMetrics.record_command(name, Aerospike.Command.Touch, now(), :ignored)

      stats = RuntimeMetrics.stats(name)

      assert stats.commands_total == 4
      assert stats.commands_ok == 1
      assert stats.commands_error == 2
      assert stats.cluster.commands.errors_by_code[:timeout] == 1
      assert stats.cluster.commands.errors_by_code[:key_not_found] == 1

      assert :ok = RuntimeMetrics.enable(name, reset: true)
      reset_stats = RuntimeMetrics.stats(name)

      assert reset_stats.metrics_enabled
      assert reset_stats.cluster.config == %{pool_size: 2, tend_interval_ms: 500}
      assert reset_stats.commands_total == 0
      assert reset_stats.cluster.commands.by_command == %{}
    end

    test "tracks tend, retry, node, and connection counters" do
      name = start_table_owner!()

      assert :ok = RuntimeMetrics.init(name)
      assert :ok = RuntimeMetrics.enable(name)

      assert :ok = RuntimeMetrics.record_tend(name, :ok)
      assert :ok = RuntimeMetrics.record_tend(name, :error)
      assert :ok = RuntimeMetrics.record_partition_map_update(name)
      assert :ok = RuntimeMetrics.record_retry_attempt(name, :rebalance)
      assert :ok = RuntimeMetrics.record_retry_attempt(name, :circuit_open)
      assert :ok = RuntimeMetrics.record_node_added(name, "A1")
      assert :ok = RuntimeMetrics.record_node_removed(name, "A1")
      assert :ok = RuntimeMetrics.record_connection_attempt(name, "A1")
      assert :ok = RuntimeMetrics.record_connection_success(name, "A1")
      assert :ok = RuntimeMetrics.record_connection_failure(name, "A1")
      assert :ok = RuntimeMetrics.record_connection_drop(name, "A1", :idle)
      assert :ok = RuntimeMetrics.record_connection_drop(name, "A1", :dead)
      assert :ok = RuntimeMetrics.record_connection_closed(name, "A1")
      assert :ok = RuntimeMetrics.record_checkout_failure(name, "A1", :pool_timeout)
      assert :ok = RuntimeMetrics.record_checkout_failure(name, "A1", :network_error)
      assert :ok = RuntimeMetrics.record_checkout_failure(nil, "A1", :network_error)

      stats = RuntimeMetrics.stats(name)

      assert stats.cluster.tends == %{total: 2, successful: 1, failed: 1}
      assert stats.cluster.nodes == %{added: 1, removed: 1}
      assert stats.cluster.partition_map_updates == 1
      assert stats.cluster.retries.rebalance == 1
      assert stats.cluster.retries.circuit_open == 1
      assert stats.cluster.connections.failed == 1
      assert stats.cluster.connections.idle_dropped == 1
      assert stats.cluster.connections.dead_dropped == 1
      assert stats.cluster.connections.pool_timeouts == 1
      assert stats.cluster.connections.network_errors == 1
      assert stats.nodes["A1"].connections.open == 0
      assert stats.nodes["A1"].connections.failed == 1
    end
  end

  defp start_table_owner! do
    name = unique_name()
    {:ok, owner} = TableOwner.start_link(name: name)

    on_exit(fn ->
      if Process.alive?(owner) do
        GenServer.stop(owner)
      end
    end)

    name
  end

  defp unique_name, do: :"runtime_metrics_#{System.unique_integer([:positive])}"
  defp now, do: System.monotonic_time()
end
