defmodule Aerospike.ScanOpsTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.PartitionFilter
  alias Aerospike.Scan
  alias Aerospike.ScanOps
  alias Aerospike.Tables

  describe "all/3" do
    test "returns max_records_required when max_records is unset" do
      scan = Scan.new("ns")

      assert {:error, %Error{code: :max_records_required}} = ScanOps.all(:test_conn, scan, [])
    end
  end

  describe "distribute_record_max/2" do
    test "splits evenly when total divides by node count" do
      assert ScanOps.distribute_record_max(9, 3) == [3, 3, 3]
    end

    test "assigns the entire remainder to the first node" do
      assert ScanOps.distribute_record_max(10, 3) == [4, 3, 3]
      assert ScanOps.distribute_record_max(11, 3) == [5, 3, 3]
    end

    test "single node receives the full budget" do
      assert ScanOps.distribute_record_max(7, 1) == [7]
    end
  end

  # These tests validate the spawn_link + monitor + unlink-before-kill pattern
  # used by ScanOps.stream/3 to manage the producer process. The pattern must
  # satisfy two constraints:
  #
  # 1. Consumer crash -> producer dies (link, for pool connection safety)
  # 2. Early halt -> producer killed without crashing the consumer (unlink first)
  #
  # We build a Stream.resource that mirrors the exact pattern from ScanOps
  # (spawn_link, Process.monitor, EXIT/DOWN in next, demonitor + unlink+kill
  # cleanup) so these tests validate the pattern in isolation without needing a
  # live Aerospike cluster.

  describe "stream producer cleanup pattern" do
    test "caller process survives after Enum.take halts the stream" do
      stream = build_test_stream(emit: 10)
      _records = Enum.take(stream, 3)

      Process.sleep(20)
      assert Process.alive?(self())
    end

    test "producer is dead after stream cleanup" do
      parent = self()

      stream = build_test_stream(emit: 10, report_producer: parent)
      _records = Enum.take(stream, 3)

      producer_pid = receive_producer_pid()
      Process.sleep(20)
      refute Process.alive?(producer_pid)
    end

    test "caller can execute more code after early termination" do
      stream = build_test_stream(emit: 10)
      records = Enum.take(stream, 3)
      assert length(records) == 3

      Process.sleep(20)

      result = Enum.sum(1..10)
      assert result == 55
    end

    test "sequential early terminations work without leaking state" do
      for i <- 1..5 do
        stream = build_test_stream(emit: 10)
        records = Enum.take(stream, 2)
        assert length(records) == 2, "iteration #{i} failed"
      end

      Process.sleep(20)
      assert Process.alive?(self())
    end

    test "cleanup is safe when producer has already exited" do
      stream = build_test_stream(emit: 2)
      records = Enum.to_list(stream)
      assert length(records) == 2

      Process.sleep(20)
      assert Process.alive?(self())
    end

    test "cleanup is safe when producer finished before halt" do
      stream = build_test_stream(emit: 3)
      records = Enum.take(stream, 3)
      assert length(records) == 3

      Process.sleep(20)
      assert Process.alive?(self())
    end

    test "consumer death kills the linked producer" do
      test_pid = self()

      consumer =
        spawn(fn ->
          stream = build_test_stream(emit: 100, report_producer: test_pid, delay: 50)
          # Start consuming but never finish — we'll kill this process
          Enum.take(stream, 1)
          Process.sleep(:infinity)
        end)

      producer_pid = receive_producer_pid()
      assert Process.alive?(producer_pid)

      Process.exit(consumer, :kill)
      Process.sleep(50)

      refute Process.alive?(consumer)
      refute Process.alive?(producer_pid)
    end

    test "stream raises on abnormal producer crash instead of hanging" do
      prev_trap = Process.flag(:trap_exit, true)

      try do
        err =
          assert_raise Error, ~r/stream producer crashed/, fn ->
            stream =
              build_test_stream(
                producer_fn: fn parent ->
                  send(parent, {:test_record, :before_crash})
                  Process.sleep(10)
                  exit(:boom)
                end
              )

            Enum.to_list(stream)
          end

        assert %Error{code: :network_error} = err
      after
        Process.flag(:trap_exit, prev_trap)
      end
    end

    test "no EXIT messages leak into caller mailbox after cleanup" do
      stream = build_test_stream(emit: 10)
      _records = Enum.take(stream, 3)

      Process.sleep(50)

      receive do
        {:EXIT, _pid, _reason} -> flunk("leaked EXIT message in mailbox")
      after
        0 -> :ok
      end
    end

    test "producer kill without done raises Error when caller traps exits" do
      prev_trap = Process.flag(:trap_exit, true)

      try do
        stream = build_test_stream(crash_before_done: true)

        err =
          assert_raise Error, ~r/stream producer crashed/, fn ->
            Enum.to_list(stream)
          end

        assert %Error{code: :network_error} = err
      after
        Process.flag(:trap_exit, prev_trap)
      end
    end

    test "stream stops after first-node error — second node is never attempted" do
      prev_trap = Process.flag(:trap_exit, true)

      try do
        node2_attempted = :atomics.new(1, [])

        stream =
          build_test_stream(
            producer_fn: fn parent ->
              send(parent, {:test_error, :node1_checkout_failed})
              Process.sleep(200)
              :atomics.put(node2_attempted, 1, 1)
              send(parent, {:test_record, :node2_rec})
              send(parent, :test_done)
            end
          )

        assert_raise Error, ~r/node1_checkout_failed/, fn ->
          Enum.to_list(stream)
        end

        Process.sleep(300)

        assert :atomics.get(node2_attempted, 1) == 0,
               "producer should have been killed before attempting node 2"
      after
        Process.flag(:trap_exit, prev_trap)
      end
    end

    @tag timeout: 5_000
    test "concurrent nodes can interleave records" do
      test_pid = self()

      nodes = [
        {:node1,
         fn consumer ->
           send(test_pid, {:node_ready, :node1, self()})
           receive do: (:emit_first -> :ok)
           send(consumer, {:test_record, {:node1, 1}})
           send(test_pid, :node1_blocked)
           receive do: (:emit_second -> :ok)
           send(consumer, {:test_record, {:node1, 2}})
           :ok
         end},
        {:node2,
         fn consumer ->
           send(test_pid, {:node_ready, :node2, self()})
           receive do: (:emit_now -> :ok)
           send(consumer, {:test_record, {:node2, 1}})
           send(test_pid, :node2_emitted)
           :ok
         end},
        {:node3,
         fn consumer ->
           send(test_pid, {:node_ready, :node3, self()})
           receive do: (:emit_now -> :ok)
           send(consumer, {:test_record, {:node3, 1}})
           send(test_pid, :node3_emitted)
           :ok
         end}
      ]

      stream = build_concurrent_test_stream(nodes)
      stream_task = Task.async(fn -> Enum.to_list(stream) end)

      node1 = receive_node_ready(:node1)
      node2 = receive_node_ready(:node2)
      node3 = receive_node_ready(:node3)

      send(node1, :emit_first)
      assert_receive :node1_blocked, 1_000

      send(node2, :emit_now)
      send(node3, :emit_now)
      assert_receive :node2_emitted, 1_000
      assert_receive :node3_emitted, 1_000

      send(node1, :emit_second)

      records = Task.await(stream_task, 1_000)
      assert records == [{:node1, 1}, {:node2, 1}, {:node3, 1}, {:node1, 2}]
    end

    @tag timeout: 5_000
    test "max_concurrent_nodes: 1 preserves sequential node order" do
      nodes = [
        {:node1,
         fn consumer ->
           send(consumer, {:test_record, :node1})
           :ok
         end},
        {:node2,
         fn consumer ->
           send(consumer, {:test_record, :node2})
           :ok
         end},
        {:node3,
         fn consumer ->
           send(consumer, {:test_record, :node3})
           :ok
         end}
      ]

      stream = build_concurrent_test_stream(nodes, max_concurrent_nodes: 1)
      assert Enum.to_list(stream) == [:node1, :node2, :node3]
    end

    @tag timeout: 5_000
    test "max_concurrent_nodes: 2 never exceeds two active workers" do
      test_pid = self()
      counters = :atomics.new(2, [])

      nodes =
        Enum.map(1..4, fn idx ->
          {idx,
           fn consumer ->
             current = :atomics.add_get(counters, 1, 1)
             record_max_atomic(counters, current)
             send(test_pid, {:worker_started, idx, self()})

             try do
               receive do: ({:release_worker, ^idx} -> :ok)
               send(consumer, {:test_record, idx})
               :ok
             after
               :atomics.sub_get(counters, 1, 1)
             end
           end}
        end)

      stream = build_concurrent_test_stream(nodes, max_concurrent_nodes: 2)
      stream_task = Task.async(fn -> Enum.to_list(stream) end)

      started =
        1..2
        |> Enum.map(fn _ -> receive_worker_started() end)
        |> Map.new(fn {idx, pid} -> {idx, pid} end)

      refute_receive {:worker_started, _, _}, 100
      assert :atomics.get(counters, 2) <= 2

      send(Map.fetch!(started, 1), {:release_worker, 1})
      send(Map.fetch!(started, 2), {:release_worker, 2})

      started3 = receive_worker_started()
      started4 = receive_worker_started()

      send(elem(started3, 1), {:release_worker, elem(started3, 0)})
      send(elem(started4, 1), {:release_worker, elem(started4, 0)})

      records = Task.await(stream_task, 1_000)
      assert Enum.sort(records) == [1, 2, 3, 4]
      assert :atomics.get(counters, 2) == 2
    end

    @tag timeout: 5_000
    test "worker error causes stream failure and remaining workers are terminated" do
      test_pid = self()

      nodes = [
        {:node1,
         fn consumer ->
           send(test_pid, {:node_ready, :node1, self()})
           receive do: (:trigger_error -> :ok)
           send(consumer, {:test_record, :before_error})
           send(consumer, {:test_error, :node1_failed})
           :error_sent
         end},
        {:node2,
         fn _consumer ->
           send(test_pid, {:node_ready, :node2, self()})
           receive do: (:never -> :ok)
           :ok
         end}
      ]

      stream = build_concurrent_test_stream(nodes)

      stream_task =
        Task.async(fn ->
          try do
            Enum.to_list(stream)
            :ok
          rescue
            e in Error -> {:raised, e}
          end
        end)

      node1 = receive_node_ready(:node1)
      node2 = receive_node_ready(:node2)
      send(node1, :trigger_error)

      assert {:raised, %Error{code: :network_error}} = Task.await(stream_task, 1_000)

      node2_ref = Process.monitor(node2)
      assert_receive {:DOWN, ^node2_ref, :process, ^node2, _reason}, 1_000
    end

    @tag timeout: 5_000
    test "early halt terminates all concurrent workers" do
      test_pid = self()

      nodes =
        Enum.map(1..3, fn idx ->
          {idx,
           fn consumer ->
             send(test_pid, {:worker_started, idx, self()})
             receive do: ({:emit_record, ^idx} -> :ok)
             send(consumer, {:test_record, idx})
             receive do: (:never -> :ok)
             :ok
           end}
        end)

      stream = build_concurrent_test_stream(nodes)
      stream_task = Task.async(fn -> Enum.take(stream, 2) end)

      worker_pids =
        1..3
        |> Enum.map(fn _ -> receive_worker_started() end)
        |> Map.new(fn {idx, pid} -> {idx, pid} end)

      send(Map.fetch!(worker_pids, 1), {:emit_record, 1})
      send(Map.fetch!(worker_pids, 2), {:emit_record, 2})

      records = Task.await(stream_task, 1_000)
      assert length(records) == 2

      Enum.each(worker_pids, fn {_idx, pid} ->
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000
      end)
    end
  end

  # Builds a Stream.resource that mirrors the ScanOps pattern:
  # - init: spawn_link a producer that sends {:record, value} messages
  # - next: receive messages, halt on :done, EXIT/DOWN for producer termination
  # - cleanup: demonitor, unlink, kill, flush (same as ScanOps.cleanup_stream_producer)
  defp build_test_stream(opts) do
    emit_count = Keyword.get(opts, :emit, 5)
    report_to = Keyword.get(opts, :report_producer, nil)
    delay = Keyword.get(opts, :delay, 0)
    producer_fn = Keyword.get(opts, :producer_fn, nil)
    crash_before_done? = Keyword.get(opts, :crash_before_done, false)

    init_fn = fn ->
      parent = self()

      producer =
        spawn_test_stream_producer(
          parent,
          emit_count,
          delay,
          report_to,
          producer_fn,
          crash_before_done?
        )

      monitor_ref = Process.monitor(producer)
      %{producer: producer, monitor_ref: monitor_ref, done: false}
    end

    Stream.resource(
      init_fn,
      &next_test_stream/1,
      &cleanup_test_stream/1
    )
  end

  defp build_concurrent_test_stream(nodes, opts \\ []) when is_list(nodes) do
    max_concurrent = Keyword.get(opts, :max_concurrent_nodes, 0)

    init_fn = fn ->
      parent = self()
      producer = spawn_link(fn -> run_test_stream_coordinator(nodes, parent, max_concurrent) end)
      monitor_ref = Process.monitor(producer)
      %{producer: producer, monitor_ref: monitor_ref, done: false}
    end

    Stream.resource(
      init_fn,
      &next_test_stream/1,
      &cleanup_test_stream/1
    )
  end

  defp spawn_test_stream_producer(_parent, _emit, _delay, _report_to, _fn, true) do
    spawn_link(fn -> Process.exit(self(), :kill) end)
  end

  defp spawn_test_stream_producer(parent, _emit, _delay, _report_to, producer_fn, false)
       when is_function(producer_fn, 1) do
    spawn_link(fn -> producer_fn.(parent) end)
  end

  defp spawn_test_stream_producer(parent, emit_count, delay, report_to, _fn, false) do
    spawn_link_test_producer(parent, emit_count, delay, report_to)
  end

  defp next_test_stream(%{done: true} = state), do: {:halt, state}

  defp next_test_stream(state) do
    receive do
      {:EXIT, pid, reason} when pid == state.producer ->
        test_stream_producer_terminated(reason, state)

      {:DOWN, ref, :process, _pid, reason} when ref == state.monitor_ref ->
        test_stream_producer_terminated(reason, state)

      {:test_record, value} ->
        {[value], state}

      {:test_error, %Error{} = e} ->
        raise e

      {:test_error, reason} ->
        raise Error.from_result_code(:network_error, message: inspect(reason))

      :test_done ->
        {:halt, %{state | done: true}}
    end
  end

  defp test_stream_producer_terminated(reason, state) do
    case reason do
      :normal ->
        {:halt, state}

      :shutdown ->
        {:halt, state}

      _ ->
        raise Error.from_result_code(:network_error,
                message: "stream producer crashed: #{inspect(reason)}"
              )
    end
  end

  defp cleanup_test_stream(%{producer: pid} = state) do
    case Map.get(state, :monitor_ref) do
      ref when is_reference(ref) -> Process.demonitor(ref, [:flush])
      _ -> :ok
    end

    Process.unlink(pid)
    if Process.alive?(pid), do: Process.exit(pid, :shutdown)

    receive do
      {:EXIT, ^pid, _} -> :ok
    after
      0 -> :ok
    end
  end

  defp spawn_link_test_producer(parent, emit_count, delay, report_to) do
    spawn_link(fn ->
      if report_to, do: send(report_to, {:producer_pid, self()})
      send_records(parent, emit_count, delay)
    end)
  end

  defp send_records(parent, 0, _delay) do
    send(parent, :test_done)
  end

  defp send_records(parent, remaining, delay) do
    if delay > 0, do: Process.sleep(delay)
    send(parent, {:test_record, remaining})
    send_records(parent, remaining - 1, delay)
  end

  defp receive_producer_pid do
    receive do
      {:producer_pid, pid} -> pid
    after
      1_000 -> flunk("did not receive producer pid")
    end
  end

  defp run_test_stream_coordinator([], consumer, _max_concurrent) do
    send(consumer, :test_done)
  end

  defp run_test_stream_coordinator(nodes, consumer, max_concurrent) do
    initial = initial_test_workers(max_concurrent, length(nodes))
    {to_start, pending} = Enum.split(nodes, initial)
    active = spawn_test_concurrent_workers(to_start, consumer, self(), 0)
    await_test_concurrent_workers(pending, active, consumer)
  end

  defp initial_test_workers(max_concurrent, total)
       when max_concurrent == 0 or max_concurrent >= total do
    total
  end

  defp initial_test_workers(max_concurrent, _total), do: max_concurrent

  defp spawn_test_concurrent_workers([], _consumer, _coordinator, acc), do: acc

  defp spawn_test_concurrent_workers([{_node, node_fn} | rest], consumer, coordinator, acc) do
    spawn_link(fn ->
      case node_fn.(consumer) do
        :ok -> send(coordinator, {:worker_done, self()})
        :error_sent -> :ok
      end
    end)

    spawn_test_concurrent_workers(rest, consumer, coordinator, acc + 1)
  end

  defp await_test_concurrent_workers(_pending, 0, consumer) do
    send(consumer, :test_done)
  end

  defp await_test_concurrent_workers(pending, active, consumer) do
    receive do
      {:worker_done, _pid} ->
        active2 = active - 1
        {pending2, active3} = maybe_spawn_next_test_worker(pending, consumer, self(), active2)
        await_test_concurrent_workers(pending2, active3, consumer)
    end
  end

  defp maybe_spawn_next_test_worker([{_node, node_fn} | rest], consumer, coordinator, active) do
    spawn_link(fn ->
      case node_fn.(consumer) do
        :ok -> send(coordinator, {:worker_done, self()})
        :error_sent -> :ok
      end
    end)

    {rest, active + 1}
  end

  defp maybe_spawn_next_test_worker([], _consumer, _coordinator, active), do: {[], active}

  defp receive_node_ready(node_name) do
    receive do
      {:node_ready, ^node_name, pid} -> pid
    after
      1_000 -> flunk("did not receive ready signal for #{inspect(node_name)}")
    end
  end

  defp receive_worker_started do
    receive do
      {:worker_started, idx, pid} -> {idx, pid}
    after
      1_000 -> flunk("did not receive worker_started signal")
    end
  end

  defp record_max_atomic(counters, candidate) do
    max_seen = :atomics.get(counters, 2)

    if candidate > max_seen do
      case :atomics.compare_exchange(counters, 2, max_seen, candidate) do
        ^max_seen -> :ok
        _ -> record_max_atomic(counters, candidate)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Fault-injection tests for ScanOps.stream/3
  #
  # These tests exercise the error paths in `run_stream_producer/5` and
  # `init_stream_state/3` by setting up bare ETS tables with various fault
  # conditions (missing partition map, dead pool, slow pool, unexpected pool
  # exit). Each test uses a unique connection name to stay async-safe.
  # ---------------------------------------------------------------------------

  describe "stream fault injection" do
    setup do
      name = :"scan_fault_#{:erlang.unique_integer([:positive])}"
      :ets.new(Tables.nodes(name), [:set, :public, :named_table, read_concurrency: true])
      :ets.new(Tables.partitions(name), [:set, :public, :named_table, read_concurrency: true])
      :ets.new(Tables.meta(name), [:set, :public, :named_table])

      on_exit(fn ->
        for t <- [Tables.nodes(name), Tables.partitions(name), Tables.meta(name)] do
          try do
            :ets.delete(t)
          catch
            :error, :badarg -> :ok
          end
        end
      end)

      {:ok, name: name}
    end

    @tag timeout: 5_000
    test "raises cluster_not_ready when cluster meta is absent", %{name: name} do
      # ready_key is NOT inserted — cluster never completed first tend
      scan = %{Scan.new("fault_ns") | partition_filter: PartitionFilter.by_id(0)}
      stream = ScanOps.stream(name, scan, [])
      err = assert_raise Error, fn -> Enum.to_list(stream) end
      assert %Error{code: :cluster_not_ready} = err
    end

    @tag timeout: 5_000
    test "raises invalid_cluster_partition_map when namespace has no partition entries", %{
      name: name
    } do
      # Cluster is ready but no partition rows exist for this namespace
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
      scan = %{Scan.new("fault_ns") | partition_filter: PartitionFilter.by_id(0)}
      stream = ScanOps.stream(name, scan, [])
      err = assert_raise Error, fn -> Enum.to_list(stream) end
      assert %Error{code: :invalid_cluster_partition_map} = err
    end

    @tag timeout: 5_000
    test "raises pool_timeout when pool checkout exceeds timeout", %{name: name} do
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})

      {:ok, pool} =
        NimblePool.start_link(worker: {Aerospike.Test.SlowPoolWorker, []}, pool_size: 1)

      on_exit(fn ->
        try do
          GenServer.stop(pool, :normal, 100)
        catch
          :exit, _ -> :ok
        end
      end)

      # partition 42 -> "node1" -> SlowPoolWorker (sleeps forever on checkout)
      :ets.insert(Tables.partitions(name), {{"fault_ns", 42, 0}, "node1"})
      :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: pool}})

      scan = %{Scan.new("fault_ns") | partition_filter: PartitionFilter.by_id(42)}
      # 1 ms checkout timeout guarantees timeout before SlowPoolWorker responds
      stream = ScanOps.stream(name, scan, pool_checkout_timeout: 1)
      err = assert_raise Error, fn -> Enum.to_list(stream) end
      assert %Error{code: :pool_timeout} = err
    end

    @tag timeout: 5_000
    test "raises invalid_node when pool process is dead", %{name: name} do
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})

      dead_pid = spawn(fn -> :ok end)
      ref = Process.monitor(dead_pid)
      receive do: ({:DOWN, ^ref, _, _, _} -> :ok)

      :ets.insert(Tables.partitions(name), {{"fault_ns", 42, 0}, "node1"})
      :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: dead_pid}})

      scan = %{Scan.new("fault_ns") | partition_filter: PartitionFilter.by_id(42)}
      stream = ScanOps.stream(name, scan, pool_checkout_timeout: 100)
      err = assert_raise Error, fn -> Enum.to_list(stream) end
      assert %Error{code: :invalid_node} = err
    end

    @tag timeout: 5_000
    @tag capture_log: true
    test "raises network_error when pool exits with unexpected reason", %{name: name} do
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})

      # An Agent is not a NimblePool — calling checkout! on it causes an
      # unexpected exit that is caught by the generic `catch :exit, reason`
      # clause in run_stream_producer, yielding :network_error.
      {:ok, fake_pool} = Agent.start(fn -> :ok end)

      on_exit(fn ->
        try do
          Agent.stop(fake_pool, :normal, 100)
        catch
          :exit, _ -> :ok
        end
      end)

      :ets.insert(Tables.partitions(name), {{"fault_ns", 42, 0}, "node1"})
      :ets.insert(Tables.nodes(name), {"node1", %{pool_pid: fake_pool}})

      scan = %{Scan.new("fault_ns") | partition_filter: PartitionFilter.by_id(42)}
      stream = ScanOps.stream(name, scan, pool_checkout_timeout: 5_000)
      err = assert_raise Error, fn -> Enum.to_list(stream) end
      assert %Error{code: :network_error} = err
    end
  end
end
