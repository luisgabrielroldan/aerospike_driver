defmodule Aerospike.ClusterTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias Aerospike.Cluster
  alias Aerospike.NodeSupervisor
  alias Aerospike.Supervisor
  alias Aerospike.Tables
  alias Aerospike.Test.MockTcpServer

  setup do
    name = :"cluster_unit_#{System.unique_integer([:positive])}"
    {:ok, name: name}
  end

  describe "cluster state transitions" do
    test "ready gate is set only after initial tend completes", %{name: name} do
      seed = start_info_server("SEED_A", delay_first_node_ms: 250)
      on_exit(fn -> stop_info_server(seed) end)

      {:ok, _sup} = start_supervised({Supervisor, base_opts(name, seed.port)})

      assert [] = :ets.lookup(Tables.meta(name), Tables.ready_key())

      assert_await(fn ->
        match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key()))
      end)
    end

    test "initial tend populates node registry and partition map", %{name: name} do
      seed = start_info_server("SEED_A", partition_generation: 1, partitions: [0])
      on_exit(fn -> stop_info_server(seed) end)

      {:ok, _sup} = start_supervised({Supervisor, base_opts(name, seed.port)})
      await_cluster_ready(name)

      assert [{"SEED_A", node_row}] = :ets.lookup(Tables.nodes(name), "SEED_A")
      assert node_row.host == "127.0.0.1"
      assert node_row.port == seed.port
      assert is_pid(node_row.pool_pid)

      assert [{{"test", 0, 0}, "SEED_A"}] = :ets.lookup(Tables.partitions(name), {"test", 0, 0})
    end

    test "tend cycle adds newly discovered peers", %{name: name} do
      peer = start_info_server("PEER_B", partition_generation: 1, partitions: [1])
      on_exit(fn -> stop_info_server(peer) end)

      seed = start_info_server("SEED_A", peers: peers_payload(1, []), partitions: [0])
      on_exit(fn -> stop_info_server(seed) end)

      {:ok, _sup} = start_supervised({Supervisor, base_opts(name, seed.port)})
      await_cluster_ready(name)
      assert [] = :ets.lookup(Tables.nodes(name), "PEER_B")

      update_info_server(seed, peers: peers_payload(2, [{"PEER_B", "127.0.0.1", peer.port}]))
      send_tend(name)

      assert_await(fn -> match?([{"PEER_B", _}], :ets.lookup(Tables.nodes(name), "PEER_B")) end)
    end

    test "tend refresh updates partition table when generation changes", %{name: name} do
      seed = start_info_server("SEED_A", partition_generation: 1, partitions: [0])
      on_exit(fn -> stop_info_server(seed) end)

      {:ok, _sup} = start_supervised({Supervisor, base_opts(name, seed.port)})
      await_cluster_ready(name)

      assert [{{"test", 0, 0}, "SEED_A"}] = :ets.lookup(Tables.partitions(name), {"test", 0, 0})
      assert [] = :ets.lookup(Tables.partitions(name), {"test", 1, 0})

      update_info_server(seed, partition_generation: 2, partitions: [0, 1])
      send_tend(name)

      assert_await(fn ->
        match?([{{"test", 1, 0}, "SEED_A"}], :ets.lookup(Tables.partitions(name), {"test", 1, 0}))
      end)
    end

    @tag :known_bug
    test "missing peers are not removed during tend", %{name: name} do
      peer = start_info_server("PEER_B", partition_generation: 1, partitions: [1])
      on_exit(fn -> stop_info_server(peer) end)

      seed =
        start_info_server(
          "SEED_A",
          peers: peers_payload(1, [{"PEER_B", "127.0.0.1", peer.port}]),
          partitions: [0]
        )

      on_exit(fn -> stop_info_server(seed) end)

      {:ok, _sup} = start_supervised({Supervisor, base_opts(name, seed.port)})
      await_cluster_ready(name)
      assert_await(fn -> match?([{"PEER_B", _}], :ets.lookup(Tables.nodes(name), "PEER_B")) end)

      update_info_server(seed, peers: peers_payload(2, []))
      send_tend(name)
      Process.sleep(100)

      # Current behavior: peer rows/pools are never pruned once discovered.
      assert match?([{"PEER_B", _}], :ets.lookup(Tables.nodes(name), "PEER_B"))
    end

    test "unreachable seeds keep cluster not ready", %{name: name} do
      free_port = reserve_free_port()

      opts = [
        name: name,
        hosts: ["127.0.0.1:#{free_port}"],
        pool_size: 1,
        connect_timeout: 100,
        recv_timeout: 100,
        tend_interval: 50
      ]

      {:ok, sup} = start_supervised({Supervisor, opts})
      assert is_pid(sup)
      assert is_pid(Process.whereis(NodeSupervisor.sup_name(name)))

      # Current behavior: supervisor starts, but cluster crashes/retries while seeds are unreachable.
      refute match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key()))

      assert_await(fn ->
        cluster_pid = Process.whereis(Cluster.cluster_name(name))
        cluster_pid == nil or not Process.alive?(cluster_pid)
      end)
    end
  end

  defp base_opts(name, port) do
    [
      name: name,
      hosts: ["127.0.0.1:#{port}"],
      pool_size: 1,
      connect_timeout: 1_000,
      recv_timeout: 1_000,
      tend_interval: 100
    ]
  end

  defp send_tend(name) do
    name
    |> Cluster.cluster_name()
    |> Process.whereis()
    |> Kernel.send(:tend)
  end

  defp await_cluster_ready(name, timeout \\ 2_000) do
    assert_await(
      fn -> match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key())) end,
      timeout
    )
  end

  defp assert_await(check_fn, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    assert_await_loop(check_fn, deadline, timeout)
  end

  defp assert_await_loop(check_fn, deadline, timeout) do
    cond do
      check_fn.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("condition was not met within #{timeout}ms")

      true ->
        Process.sleep(20)
        assert_await_loop(check_fn, deadline, timeout)
    end
  end

  defp start_info_server(node_name, opts) do
    {:ok, lsock, port} = MockTcpServer.start()

    state =
      %{
        node_name: node_name,
        peers: Keyword.get(opts, :peers, peers_payload(1, [])),
        partition_generation: Keyword.get(opts, :partition_generation, 1),
        partitions: Keyword.get(opts, :partitions, [0]),
        delay_first_node_ms: Keyword.get(opts, :delay_first_node_ms, 0),
        delay_done?: false
      }

    {:ok, state_pid} = Agent.start_link(fn -> state end)

    {:ok, acceptor} =
      Task.start_link(fn ->
        accept_loop(lsock, state_pid)
      end)

    %{acceptor: acceptor, lsock: lsock, port: port, state: state_pid}
  end

  defp stop_info_server(%{lsock: lsock, state: state_pid}) do
    :gen_tcp.close(lsock)
    if Process.alive?(state_pid), do: Agent.stop(state_pid)
    :ok
  end

  defp update_info_server(%{state: state_pid}, updates) do
    Agent.update(state_pid, fn state ->
      Enum.reduce(updates, state, fn
        {:partition_generation, gen}, acc -> %{acc | partition_generation: gen}
        {:partitions, parts}, acc -> %{acc | partitions: parts}
        {:peers, peers}, acc -> %{acc | peers: peers}
        {_, _}, acc -> acc
      end)
    end)
  end

  defp accept_loop(lsock, state_pid) do
    case :gen_tcp.accept(lsock, 100) do
      {:ok, client} ->
        Task.start(fn -> client_loop(client, state_pid) end)
        accept_loop(lsock, state_pid)

      {:error, :timeout} ->
        accept_loop(lsock, state_pid)

      {:error, :closed} ->
        :ok

      {:error, _} ->
        :ok
    end
  end

  defp client_loop(client, state_pid) do
    case MockTcpServer.recv_message(client) do
      {:ok, header, body} ->
        respond_to_wire_message(client, state_pid, header, body)
        client_loop(client, state_pid)
    end
  rescue
    _ -> :ok
  after
    :gen_tcp.close(client)
  end

  defp respond_to_wire_message(client, _state_pid, <<_version::8, 2::8, _::binary>>, _body) do
    MockTcpServer.send_admin_response(client, :binary.copy(<<0>>, 16))
  end

  defp respond_to_wire_message(client, state_pid, <<_version::8, 1::8, _::binary>>, body) do
    commands =
      body
      |> String.trim_trailing("\n")
      |> case do
        "" -> []
        data -> String.split(data, "\n", trim: true)
      end

    maybe_delay_node_response(state_pid, commands)

    lines =
      Enum.map_join(commands, "\n", fn command ->
        "#{command}\t#{lookup_info_value(state_pid, command)}"
      end)

    payload = if lines == "", do: "", else: lines <> "\n"
    MockTcpServer.send_info_response(client, payload)
  end

  defp respond_to_wire_message(_client, _state_pid, _header, _body), do: :ok

  defp maybe_delay_node_response(state_pid, commands) do
    if Enum.member?(commands, "node"), do: maybe_sleep(state_pid)
  end

  defp maybe_sleep(state_pid) do
    case Agent.get_and_update(state_pid, &consume_delay/1) do
      delay_ms when delay_ms > 0 -> Process.sleep(delay_ms)
      _ -> :ok
    end
  end

  defp consume_delay(%{delay_done?: true} = state), do: {0, state}
  defp consume_delay(%{delay_first_node_ms: delay} = state) when delay <= 0, do: {0, state}

  defp consume_delay(%{delay_first_node_ms: delay} = state) do
    {delay, %{state | delay_done?: true}}
  end

  defp lookup_info_value(state_pid, "node"), do: Agent.get(state_pid, & &1.node_name)
  defp lookup_info_value(_state_pid, "build"), do: "7.0.0.0"
  defp lookup_info_value(state_pid, "peers-clear-std"), do: Agent.get(state_pid, & &1.peers)

  defp lookup_info_value(state_pid, "partition-generation"),
    do: Agent.get(state_pid, &Integer.to_string(&1.partition_generation))

  defp lookup_info_value(state_pid, "replicas"),
    do: Agent.get(state_pid, &replicas_for_partitions("test", &1.partitions))

  defp lookup_info_value(_state_pid, _), do: ""

  defp peers_payload(generation, peers) do
    entries =
      Enum.map_join(peers, ",", fn {node_name, host, port} ->
        "[#{node_name},,[#{host}:#{port}]]"
      end)

    "#{generation},3000,[#{entries}]"
  end

  defp replicas_for_partitions(namespace, partitions) do
    "#{namespace}:1,#{bitmap_for_partitions(partitions)}"
  end

  defp bitmap_for_partitions(partitions) do
    bitmap =
      Enum.reduce(partitions, :binary.copy(<<0>>, 512), fn pid, acc ->
        set_partition_bit(acc, pid)
      end)

    Base.encode64(bitmap)
  end

  defp set_partition_bit(bitmap, pid) when is_integer(pid) and pid >= 0 and pid < 4_096 do
    byte_idx = div(pid, 8)
    bit_idx = rem(pid, 8)

    <<prefix::binary-size(byte_idx), current::8, suffix::binary>> = bitmap
    updated = bor(current, 0x80 >>> bit_idx)
    <<prefix::binary, updated::8, suffix::binary>>
  end

  defp reserve_free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, true}])
    {:ok, {_, port}} = :inet.sockname(socket)
    :gen_tcp.close(socket)
    port
  end
end
