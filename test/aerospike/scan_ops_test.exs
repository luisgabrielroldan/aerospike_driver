defmodule Aerospike.ScanOpsTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Scan
  alias Aerospike.ScanOps

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
  # 1. Consumer crash → producer dies (link, for pool connection safety)
  # 2. Early halt → producer killed without crashing the consumer (unlink first)
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
end
