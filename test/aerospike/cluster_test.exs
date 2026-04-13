defmodule Aerospike.ClusterTest do
  use ExUnit.Case, async: false

  import Bitwise
  import ExUnit.CaptureLog

  alias Aerospike.Cluster
  alias Aerospike.NodeSupervisor
  alias Aerospike.Supervisor
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers
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

    test "tend refresh re-fetches each node when its per-node generation advances",
         %{name: name} do
      # Regression for the scalar `partition_generation` bug: a sibling node
      # can advance the shared scalar to value X earlier in a reduce pass,
      # then the next node whose own generation is also X has its refetch
      # short-circuited because the scalar matches — even though *this*
      # node's partitions were stale under the old entry.
      #
      # Repro: two nodes start stable at gen=1. Advance NODE_A to gen=5 first
      # and confirm its new partition is picked up (this also forces the
      # scalar to stop being 1). Then advance NODE_B to the *same* gen=5 with
      # a new partition. Under the scalar code, NODE_B's refetch is skipped
      # because `check_partition_generation/4` sees B.gen == state.scalar.
      peer = start_info_server("NODE_B", partition_generation: 1, partitions: [1])
      on_exit(fn -> stop_info_server(peer) end)

      seed =
        start_info_server(
          "NODE_A",
          partition_generation: 1,
          peers: peers_payload(1, [{"NODE_B", "127.0.0.1", peer.port}]),
          partitions: [0]
        )

      on_exit(fn -> stop_info_server(seed) end)

      {:ok, _sup} = start_supervised({Supervisor, base_opts(name, seed.port)})
      await_cluster_ready(name)

      # Bootstrap must have populated both nodes' base partitions.
      Helpers.poll_until(
        fn ->
          :ets.lookup(Tables.partitions(name), {"test", 0, 0}) == [{{"test", 0, 0}, "NODE_A"}] and
            :ets.lookup(Tables.partitions(name), {"test", 1, 0}) == [{{"test", 1, 0}, "NODE_B"}]
        end,
        between: fn -> send_tend(name) end,
        timeout: 2_000
      )

      # Advance NODE_A's gen to 5, adding partition 2. Under either the buggy
      # or fixed code, this should be picked up (A's gen differs from the
      # stored value of 1). This poll is the precondition setup, not the
      # bug repro itself.
      update_info_server(seed, partition_generation: 5, partitions: [0, 2])

      Helpers.poll_until(
        fn ->
          :ets.lookup(Tables.partitions(name), {"test", 2, 0}) == [{{"test", 2, 0}, "NODE_A"}]
        end,
        between: fn -> send_tend(name) end,
        timeout: 2_000
      )

      # Now advance NODE_B to the SAME gen=5 with new partition 3. This is
      # the bug trigger: the scalar code ends the previous tend pass with
      # state.partition_generation == 5 (because NODE_A's refetch just wrote
      # that value), so when NODE_B is probed and reports gen=5, the scalar
      # comparison says "unchanged" and skips the refetch. The per-node fix
      # keeps NODE_B's stored gen at 1 (its last successful refetch), so
      # 5 != 1 triggers the needed refetch.
      update_info_server(peer, partition_generation: 5, partitions: [1, 3])

      Helpers.poll_until(
        fn ->
          :ets.lookup(Tables.partitions(name), {"test", 3, 0}) == [{{"test", 3, 0}, "NODE_B"}]
        end,
        between: fn -> send_tend(name) end,
        timeout: 2_000
      )
    end

    test "tend removes peers no longer reported by peers-clear-std", %{name: name} do
      peer = start_info_server("B_PEER", partition_generation: 1, partitions: [1])
      on_exit(fn -> stop_info_server(peer) end)

      seed =
        start_info_server(
          "A_SEED",
          peers: peers_payload(1, [{"B_PEER", "127.0.0.1", peer.port}]),
          partitions: [0]
        )

      on_exit(fn -> stop_info_server(seed) end)

      {:ok, _sup} = start_supervised({Supervisor, base_opts(name, seed.port)})
      await_cluster_ready(name)
      assert_await(fn -> match?([{"B_PEER", _}], :ets.lookup(Tables.nodes(name), "B_PEER")) end)
      assert [{{"test", 1, 0}, "B_PEER"}] = :ets.lookup(Tables.partitions(name), {"test", 1, 0})
      assert_tend_connection_present(name, "B_PEER")

      update_info_server(seed, peers: peers_payload(2, []))
      send_tend(name)

      assert_await(fn ->
        [] == :ets.lookup(Tables.nodes(name), "B_PEER") and
          [] == :ets.lookup(Tables.partitions(name), {"test", 1, 0}) and
          not tend_connection_present?(name, "B_PEER")
      end)
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

      log =
        capture_log(fn ->
          {:ok, sup} = start_supervised({Supervisor, opts})
          assert is_pid(sup)
          assert is_pid(Process.whereis(NodeSupervisor.sup_name(name)))

          # Cluster stays alive but never becomes ready while seeds are unreachable.
          cluster_pid = Process.whereis(Cluster.cluster_name(name))
          assert is_pid(cluster_pid)
          refute match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key()))

          # Wait through several tend cycles; cluster remains alive and not-ready.
          Process.sleep(200)
          assert Process.alive?(cluster_pid)
          refute match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key()))
        end)

      assert log =~ "seed connection failed"
    end

    test "rotate_auth_credential updates cluster state and restarted pools use the new credential",
         %{name: name} do
      test_pid = self()

      seed =
        start_info_server("SEED_A",
          notify_login_to: test_pid
        )

      on_exit(fn -> stop_info_server(seed) end)

      opts =
        base_opts(name, seed.port)
        |> Keyword.put(:auth_opts, user: "alice", credential: "old-credential")

      {:ok, _sup} = start_supervised({Supervisor, opts})
      await_cluster_ready(name)

      assert_receive {:login_credential, "old-credential"}, 1_000

      [{"SEED_A", %{pool_pid: old_pool_pid}}] = :ets.lookup(Tables.nodes(name), "SEED_A")

      assert :ok = Cluster.rotate_auth_credential(name, "alice", "new-credential")

      assert_receive {:login_credential, "new-credential"}, 1_000

      assert [{:auth_opts, auth_opts}] = :ets.lookup(Tables.meta(name), :auth_opts)
      assert Keyword.fetch!(auth_opts, :credential) == "new-credential"

      cluster_state =
        name
        |> Cluster.cluster_name()
        |> Process.whereis()
        |> :sys.get_state()

      assert Keyword.fetch!(cluster_state.auth_opts, :credential) == "new-credential"

      assert_await(fn ->
        case :ets.lookup(Tables.nodes(name), "SEED_A") do
          [{"SEED_A", %{pool_pid: new_pool_pid}}] -> new_pool_pid != old_pool_pid
          _ -> false
        end
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

  defp assert_tend_connection_present(name, node_name) do
    assert tend_connection_present?(name, node_name)
  end

  defp tend_connection_present?(name, node_name) do
    name
    |> Cluster.cluster_name()
    |> Process.whereis()
    |> :sys.get_state()
    |> Map.fetch!(:tend_conns)
    |> Map.has_key?(node_name)
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
        notify_login_to: Keyword.get(opts, :notify_login_to),
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

  defp respond_to_wire_message(client, state_pid, <<_version::8, 2::8, _::binary>>, body) do
    maybe_notify_login_credential(state_pid, body)
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

  defp maybe_notify_login_credential(state_pid, body) do
    with {:ok, pid} <- fetch_login_notifier(state_pid),
         20 <- command_id(body),
         fields <- decode_admin_fields(body),
         credential when is_binary(credential) <- Map.get(fields, 3) do
      send(pid, {:login_credential, credential})
    else
      _ -> :ok
    end
  end

  defp fetch_login_notifier(state_pid) do
    case Agent.get(state_pid, & &1.notify_login_to) do
      pid when is_pid(pid) -> {:ok, pid}
      _ -> :error
    end
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

  defp command_id(<<_status::8, _result::8, command::8, _field_count::8, _rest::binary>>),
    do: command

  defp decode_admin_fields(<<_admin::binary-size(16), rest::binary>>) do
    decode_admin_fields(rest, %{})
  end

  defp decode_admin_fields(<<>>, acc), do: acc

  defp decode_admin_fields(
         <<len::32-big, id::8, value::binary-size(len - 1), rest::binary>>,
         acc
       ) do
    decode_admin_fields(rest, Map.put(acc, id, value))
  end

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
