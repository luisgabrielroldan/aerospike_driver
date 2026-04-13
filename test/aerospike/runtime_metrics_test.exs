defmodule Aerospike.RuntimeMetricsTest do
  use ExUnit.Case, async: false

  alias Aerospike.Tables

  setup do
    conn = :"runtime_metrics_#{System.unique_integer([:positive, :monotonic])}"
    meta = Tables.meta(conn)
    nodes = Tables.nodes(conn)
    parts = Tables.partitions(conn)

    :ets.new(meta, [:set, :public, :named_table])
    :ets.new(nodes, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(parts, [:set, :public, :named_table, read_concurrency: true])
    Aerospike.RuntimeMetrics.init(conn, pool_size: 3, tend_interval: 1_000)
    :ets.insert(meta, {Tables.ready_key(), true})
    :ets.insert(nodes, {"node-a", %{host: "127.0.0.1", port: 3000, active: true}})

    on_exit(fn ->
      for table <- [meta, nodes, parts] do
        try do
          :ets.delete(table)
        catch
          :error, :badarg -> :ok
        end
      end
    end)

    {:ok, conn: conn}
  end

  test "metrics remain disabled until explicitly enabled", %{conn: conn} do
    emit_command_stop(conn, :get, :ok, "node-a", 42)

    stats = Aerospike.stats(conn)

    refute Aerospike.metrics_enabled?(conn)
    assert stats.commands_total == 0
    assert stats.cluster.commands.by_command == %{}
  end

  test "enable_metrics and disable_metrics toggle collection", %{conn: conn} do
    assert :ok = Aerospike.enable_metrics(conn)
    assert Aerospike.metrics_enabled?(conn)

    emit_command_stop(conn, :get, :ok, "node-a", 25)

    enabled_stats = Aerospike.stats(conn)
    assert enabled_stats.commands_total == 1
    assert enabled_stats.commands_ok == 1
    assert enabled_stats.cluster.commands.by_command.get.total == 1
    assert enabled_stats.nodes["node-a"].commands.total == 1

    assert :ok = Aerospike.disable_metrics(conn)
    refute Aerospike.metrics_enabled?(conn)

    emit_command_stop(conn, :get, {:error, :timeout}, "node-a", 10)

    disabled_stats = Aerospike.stats(conn)
    assert disabled_stats.commands_total == 1
    assert disabled_stats.commands_error == 0
  end

  test "stats expose runtime summary and nested sections", %{conn: conn} do
    Aerospike.RuntimeMetrics.record_tend(conn, :ok)
    Aerospike.RuntimeMetrics.record_partition_map_update(conn)
    Aerospike.RuntimeMetrics.record_node_added(conn, "node-a")
    Aerospike.RuntimeMetrics.record_connection_attempt(conn, "node-a")
    Aerospike.RuntimeMetrics.record_connection_success(conn, "node-a")
    Aerospike.RuntimeMetrics.record_checkout_failure(conn, "node-a", :pool_timeout)

    stats = Aerospike.stats(conn)

    assert stats.metrics_enabled == false
    assert stats.cluster_ready == true
    assert stats.nodes_total == 1
    assert stats.nodes_active == 1
    assert stats.open_connections == 1
    assert stats.cluster.config.pool_size == 3
    assert stats.cluster.config.tend_interval_ms == 1_000
    assert stats.cluster.tends.total == 1
    assert stats.cluster.tends.successful == 1
    assert stats.cluster.partition_map_updates == 1
    assert stats.cluster.connections.attempts == 1
    assert stats.cluster.connections.successful == 1
    assert stats.cluster.connections.pool_timeouts == 1
    assert stats.nodes["node-a"].connections.open == 1
  end

  defp emit_command_stop(conn, command, result, node_name, _duration_us) do
    start_mono = System.monotonic_time()
    Aerospike.RuntimeMetrics.record(conn, command, start_mono, result, node_name)
  end
end
