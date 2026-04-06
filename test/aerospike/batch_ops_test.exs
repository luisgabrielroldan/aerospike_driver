defmodule Aerospike.BatchOpsTest do
  use ExUnit.Case, async: false

  alias Aerospike.Batch
  alias Aerospike.BatchOps
  alias Aerospike.BatchResult
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Tables
  alias Aerospike.Test.MockTcpServer

  # ── Setup ──

  # Each test gets its own uniquely named conn: ETS tables + Task.Supervisor.
  # The ready flag is inserted so router lookups succeed by default.
  setup do
    name = :"batch_ops_test_#{:erlang.unique_integer([:positive])}"

    :ets.new(Tables.nodes(name), [:set, :public, :named_table, read_concurrency: true])
    :ets.new(Tables.partitions(name), [:set, :public, :named_table, read_concurrency: true])
    :ets.new(Tables.meta(name), [:set, :public, :named_table])

    start_supervised!({Task.Supervisor, name: Tables.task_sup(name)})

    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})

    on_exit(fn ->
      for t <- [Tables.nodes(name), Tables.partitions(name), Tables.meta(name)] do
        try do
          :ets.delete(t)
        catch
          :error, :badarg -> :ok
        end
      end
    end)

    {:ok, conn: name}
  end

  # ── Helpers ──

  # Inserts one node entry and one partition entry per key, all pointing at pool_pid.
  defp register_pool(conn, pool_pid, keys, node_name \\ "test_node") do
    :ets.insert(Tables.nodes(conn), {node_name, %{pool_pid: pool_pid}})

    Enum.each(keys, fn key ->
      :ets.insert(Tables.partitions(conn), {{key.namespace, Key.partition_id(key), 0}, node_name})
    end)
  end

  # Starts a TCP listener whose server loop accepts one connection and responds to
  # every batch request with the given response_body wrapped in a single AS_MSG frame.
  # Returns {lsock, port} — caller must close lsock in on_exit.
  defp start_responding_server(response_body) do
    {:ok, lsock, port} = MockTcpServer.start()

    Task.start(fn ->
      case :gen_tcp.accept(lsock, 5_000) do
        {:ok, client} -> serve_loop(client, response_body)
        {:error, _} -> :ok
      end
    end)

    {lsock, port}
  end

  # Read and discard the proto header + body, then send the response.
  # Handles socket close gracefully without crashing the task.
  defp serve_loop(client, response_body) do
    case :gen_tcp.recv(client, 8, 5_000) do
      {:ok, <<_version::8, _type::8, length::48-big>>} ->
        if length > 0, do: :gen_tcp.recv(client, length, 5_000)
        :gen_tcp.send(client, Message.encode_as_msg(response_body))
        serve_loop(client, response_body)

      {:error, _} ->
        :ok
    end
  end

  # Starts a NimblePool backed by a NodePool worker pointing at the given port.
  # Supervised by the test — cleaned up automatically at end of test.
  defp start_mock_pool(port) do
    start_supervised!(
      {NimblePool,
       worker:
         {Aerospike.NodePool,
          connect_opts: [host: "127.0.0.1", port: port, timeout: 2_000, idle_timeout: 55_000],
          auth_opts: []},
       pool_size: 1}
    )
  end

  # Builds a 22-byte batch response AS_MSG header with default-zero fields.
  # Byte layout: header_size(1) i1(1) i2(1) i3(1) i4(1) rc(1) gen(4) exp(4) bidx(4) fc(2) oc(2)
  defp batch_hdr(opts) do
    i3 = Keyword.get(opts, :i3, 0)
    rc = Keyword.get(opts, :rc, 0)
    bidx = Keyword.get(opts, :bidx, 0)
    fc = Keyword.get(opts, :fc, 0)
    oc = Keyword.get(opts, :oc, 0)

    <<22::8, 0::8, 0::8, i3::8, 0::8, rc::8, 0::32-big, 0::32-big, bidx::32-big, fc::16-big,
      oc::16-big>>
  end

  # A terminal frame that signals end-of-stream (INFO3_LAST = 0x01).
  defp last_batch_hdr, do: batch_hdr(i3: AsmMsg.info3_last())

  # ── Empty-keys short-circuit ──

  describe "empty keys" do
    test "batch_get returns {:ok, []} immediately", %{conn: conn} do
      assert {:ok, []} = BatchOps.batch_get(conn, [], [])
    end

    test "batch_exists returns {:ok, []} immediately", %{conn: conn} do
      assert {:ok, []} = BatchOps.batch_exists(conn, [], [])
    end

    test "batch_operate returns {:ok, []} immediately", %{conn: conn} do
      assert {:ok, []} = BatchOps.batch_operate(conn, [], [])
    end
  end

  # ── Telemetry ──

  describe "telemetry" do
    test "batch_get emits :start and :stop telemetry events", %{conn: conn} do
      ref = make_ref()
      handler_id = "batch-ops-test-#{inspect(ref)}"
      events = [[:aerospike, :command, :start], [:aerospike, :command, :stop]]

      :telemetry.attach_many(
        handler_id,
        events,
        fn event, _measurements, meta, {pid, r} -> send(pid, {r, event, meta}) end,
        {self(), ref}
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      BatchOps.batch_get(conn, [], [])

      assert_receive {^ref, [:aerospike, :command, :start], %{command: :batch_get}}

      # telemetry 1.4.1 span/3 passes only the span function's returned map to the stop event
      # (plus telemetry_span_context). It does NOT merge start_metadata in (see telemetry.erl:365).
      # As a result, the stop event is missing command/conn/namespace/set, which limits handler
      # filtering. Fix: return Map.merge(meta, %{result: ...}) from with_telemetry instead of
      # a fresh map — but that is a lib change outside T2 scope.
      assert_receive {^ref, [:aerospike, :command, :stop], %{result: :ok, batch_size: 0}}
    end
  end

  # ── Cluster not ready ──

  describe "cluster not ready" do
    test "batch_get returns cluster_not_ready when ready flag is absent" do
      name = :"batch_ops_notready_#{:erlang.unique_integer([:positive])}"

      :ets.new(Tables.nodes(name), [:set, :public, :named_table, read_concurrency: true])
      :ets.new(Tables.partitions(name), [:set, :public, :named_table, read_concurrency: true])
      :ets.new(Tables.meta(name), [:set, :public, :named_table])
      start_supervised!({Task.Supervisor, name: Tables.task_sup(name)})

      on_exit(fn ->
        for t <- [Tables.nodes(name), Tables.partitions(name), Tables.meta(name)] do
          try do
            :ets.delete(t)
          catch
            :error, :badarg -> :ok
          end
        end
      end)

      key = Key.new("test", "s", "k")

      assert {:error, %Error{code: :cluster_not_ready}} =
               BatchOps.batch_get(name, [key], [])
    end
  end

  # ── Timeout path (finalize_yield_pairs nil) ──

  describe "timeout" do
    @tag timeout: 10_000
    test "batch_get returns {:error, :timeout} when node task stalls", %{conn: conn} do
      key = Key.new("test", "s", "slow-key")

      slow_pool =
        start_supervised!({NimblePool, worker: {Aerospike.Test.SlowPoolWorker, []}, pool_size: 1})

      register_pool(conn, slow_pool, [key])

      # `timeout:` drives Task.yield_many/2 wait inside merge_slot_results.
      assert {:error, %Error{code: :timeout}} =
               BatchOps.batch_get(conn, [key], timeout: 50)
    end
  end

  # ── Partial failure path (reduce_merged_slots error propagation) ──

  describe "partial failure" do
    test "batch_get returns first error when a node task yields an error result", %{conn: conn} do
      key = Key.new("test", "s", "fail-key")

      # A 3-byte body is too short for parse_batch_get to read a 22-byte header.
      # recv_stream treats the short body as terminal (lazy_stream_chunk_terminal? returns
      # true on parse error), so checkout_and_request_stream returns {:ok, <<1,2,3>>}.
      # parse_batch_get then returns {:error, :parse_error}, which propagates through
      # normalize_task_result → reduce_merged_slots finds it and returns the error.
      bad_response = <<1, 2, 3>>
      {lsock, port} = start_responding_server(bad_response)
      on_exit(fn -> :gen_tcp.close(lsock) end)

      pool = start_mock_pool(port)
      register_pool(conn, pool, [key])

      assert {:error, %Error{code: :parse_error}} =
               BatchOps.batch_get(conn, [key], [])
    end
  end

  # ── Single-node success paths ──

  describe "single node success" do
    test "batch_get returns nil for a key not found", %{conn: conn} do
      key = Key.new("test", "s", "missing-key")

      # rc=2 (KEY_NOT_FOUND) at bidx=0 → nil in parse_batch_get; LAST sentinel terminates.
      response = batch_hdr(rc: 2, bidx: 0) <> last_batch_hdr()
      {lsock, port} = start_responding_server(response)
      on_exit(fn -> :gen_tcp.close(lsock) end)

      pool = start_mock_pool(port)
      register_pool(conn, pool, [key])

      assert {:ok, [nil]} = BatchOps.batch_get(conn, [key], [])
    end

    test "batch_exists returns false for a key not found", %{conn: conn} do
      key = Key.new("test", "s", "absent-key")

      # rc=2 at bidx=0 → exists?=(rc==0)=false; map initialized to false defaults.
      response = batch_hdr(rc: 2, bidx: 0) <> last_batch_hdr()
      {lsock, port} = start_responding_server(response)
      on_exit(fn -> :gen_tcp.close(lsock) end)

      pool = start_mock_pool(port)
      register_pool(conn, pool, [key])

      assert {:ok, [false]} = BatchOps.batch_exists(conn, [key], [])
    end
  end

  # ── finalize_operate_slots: missing slot filled with :no_response ──

  describe "finalize_operate_slots" do
    test "missing batch_operate slots are filled with :no_response error", %{conn: conn} do
      k0 = Key.new("test", "s", "op0")
      k1 = Key.new("test", "s", "op1")
      op0 = Batch.put(k0, %{"a" => 1})
      op1 = Batch.put(k1, %{"b" => 2})

      # Server responds only for bidx=0 (k0); bidx=1 (k1) is absent.
      # parse_batch_operate is called with all_ops=[op0, op1], returns [{ok, nil}, nil].
      # zip_merge_slots merges into [BatchResult{ok}, nil].
      # finalize_operate_slots fills nil at index 1 with a :no_response BatchResult.
      response = batch_hdr(rc: 0, bidx: 0) <> last_batch_hdr()
      {lsock, port} = start_responding_server(response)
      on_exit(fn -> :gen_tcp.close(lsock) end)

      pool = start_mock_pool(port)
      register_pool(conn, pool, [k0, k1])

      assert {:ok,
              [
                %BatchResult{status: :ok},
                %BatchResult{status: :error, error: %Error{code: :no_response}}
              ]} = BatchOps.batch_operate(conn, [op0, op1], [])
    end
  end
end
