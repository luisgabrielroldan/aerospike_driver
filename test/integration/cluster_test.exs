defmodule Aerospike.Integration.ClusterTest do
  use ExUnit.Case, async: false

  alias Aerospike.Cluster
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Response
  alias Aerospike.Router
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  @moduletag :integration

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"cluster_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      tend_interval: 60_000
    ]

    {:ok, sup} = start_supervised({Aerospike.Supervisor, opts})
    await_cluster_ready(name)

    {:ok, name: name, sup: sup, host: host, port: port}
  end

  test "node appears in ETS and cluster becomes ready", %{name: name} do
    assert [{_, true}] = :ets.lookup(Tables.meta(name), Tables.ready_key())
    assert :ets.first(Tables.nodes(name)) != :"$end_of_table"
  end

  test "partition map is populated for default namespace", %{name: name} do
    assert :ets.first(Tables.partitions(name)) != :"$end_of_table"
    assert :ets.info(Tables.partitions(name), :size) >= 1
  end

  test "put and get through Router", %{name: name, host: host, port: port} do
    key = Helpers.unique_key("test", "cluster_itest")

    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    wire_put = Helpers.put_wire(key, %{"x" => 1})

    assert {:ok, body, _node} = Router.run(name, key, wire_put, pool_checkout_timeout: 5_000)

    assert {:ok, msg} = AsmMsg.decode(body)
    assert :ok = Response.parse_write_response(msg)

    wire_get = Helpers.get_wire(key)

    assert {:ok, body2, _node} = Router.run(name, key, wire_get, pool_checkout_timeout: 5_000)
    assert {:ok, msg2} = AsmMsg.decode(body2)
    assert {:ok, record} = Response.parse_record_response(msg2, key)
    assert record.bins["x"] == 1
  end

  test "Router returns cluster_not_ready when meta flag missing", %{name: name} do
    :ets.delete(Tables.meta(name), Tables.ready_key())

    key = Key.new("test", "x", "y")
    wire = Helpers.get_wire(key)

    assert {:error, %{code: :cluster_not_ready}} = Router.run(name, key, wire)
  end

  test "pool recovers after connection socket is closed", %{name: name, host: host, port: port} do
    key = Helpers.unique_key("test", "cluster_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    [{_node_name, %{pool_pid: pool_pid}} | _] = :ets.tab2list(Tables.nodes(name))

    NimblePool.checkout!(pool_pid, :checkout, fn _from, conn ->
      {mod, socket} = conn.transport
      _ = mod.close(socket)
      {:ok, :close}
    end)

    wire_put = Helpers.put_wire(key, %{"x" => 42})
    assert {:ok, body, _node} = Router.run(name, key, wire_put, pool_checkout_timeout: 5_000)
    assert {:ok, msg} = AsmMsg.decode(body)
    assert :ok = Response.parse_write_response(msg)
  end

  test "ETS tables survive Cluster GenServer restart", %{name: name, host: host, port: port} do
    key = Helpers.unique_key("test", "cluster_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    cluster_pid = Process.whereis(Cluster.cluster_name(name))
    assert is_pid(cluster_pid)
    assert :ets.info(Tables.nodes(name), :size) >= 1

    ref = Process.monitor(cluster_pid)
    Process.exit(cluster_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^cluster_pid, :killed}, 5_000

    await_process_restart(Cluster.cluster_name(name))

    assert :ets.info(Tables.nodes(name)) != :undefined
    assert :ets.info(Tables.partitions(name)) != :undefined
    assert :ets.info(Tables.meta(name)) != :undefined

    await_cluster_ready(name)

    wire_put = Helpers.put_wire(key, %{"x" => 99})
    assert {:ok, body, _node} = Router.run(name, key, wire_put, pool_checkout_timeout: 5_000)
    assert {:ok, msg} = AsmMsg.decode(body)
    assert :ok = Response.parse_write_response(msg)
  end

  test "periodic tend fires and cluster remains healthy", %{host: host, port: port} do
    name = :"cluster_tend_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      tend_interval: 100
    ]

    {:ok, _sup} = start_supervised({Aerospike.Supervisor, opts})
    await_cluster_ready(name)

    node_count_before = :ets.info(Tables.nodes(name), :size)
    part_count_before = :ets.info(Tables.partitions(name), :size)

    Process.sleep(800)

    assert [{_, true}] = :ets.lookup(Tables.meta(name), Tables.ready_key())
    assert :ets.info(Tables.nodes(name), :size) >= node_count_before
    assert :ets.info(Tables.partitions(name), :size) >= part_count_before
  end

  test "cluster connects when host has no explicit port (defaults to 3000)", %{host: host} do
    name = :"cluster_noport_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: [host],
      pool_size: 1,
      connect_timeout: 5_000,
      tend_interval: 60_000
    ]

    {:ok, _sup} = start_supervised({Aerospike.Supervisor, opts})
    await_cluster_ready(name)
    assert [{_, true}] = :ets.lookup(Tables.meta(name), Tables.ready_key())
  end

  test "stats expose runtime counters and metrics toggle", %{name: name, host: host, port: port} do
    initial_stats = Aerospike.stats(name)

    refute initial_stats.metrics_enabled
    assert initial_stats.cluster_ready
    assert initial_stats.nodes_total >= 1
    assert initial_stats.cluster.tends.total >= 1
    assert initial_stats.cluster.partition_map_updates >= 1

    assert :ok = Aerospike.enable_metrics(name)
    assert Aerospike.metrics_enabled?(name)

    key = Helpers.unique_key("test", "cluster_stats")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(name, key, %{"x" => 1})
    assert {:ok, record} = Aerospike.get(name, key)
    assert record.bins["x"] == 1

    stats = Aerospike.stats(name)

    assert stats.commands_total >= 2
    assert stats.commands_ok >= 2
    assert stats.open_connections >= 1
    assert stats.cluster.commands.by_command.put.total >= 1
    assert stats.cluster.commands.by_command.get.total >= 1
    assert map_size(stats.nodes) >= 1

    assert :ok = Aerospike.disable_metrics(name)
    refute Aerospike.metrics_enabled?(name)
  end

  defp await_cluster_ready(name, timeout \\ 5_000) do
    poll_until(timeout, "cluster not ready", fn ->
      match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key()))
    end)
  end

  defp await_process_restart(registered_name, timeout \\ 5_000) do
    poll_until(timeout, "process #{registered_name} not restarted", fn ->
      is_pid(Process.whereis(registered_name))
    end)
  end

  defp poll_until(timeout, message, check_fn) do
    deadline = System.monotonic_time(:millisecond) + timeout
    poll_loop(deadline, timeout, message, check_fn)
  end

  defp poll_loop(deadline, timeout, message, check_fn) do
    cond do
      check_fn.() ->
        :done

      System.monotonic_time(:millisecond) > deadline ->
        flunk("#{message} within #{timeout}ms")

      true ->
        Process.sleep(50)
        poll_loop(deadline, timeout, message, check_fn)
    end
  end
end
