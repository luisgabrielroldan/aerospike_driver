defmodule Aerospike.NodePoolTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.NodeCounters
  alias Aerospike.NodePool
  alias Aerospike.Telemetry
  alias Aerospike.Transport.Fake

  @host "10.0.0.1"
  @port 3000
  @node_name "A1"

  defp start_pool!(pool_size, opts \\ []) do
    {:ok, fake} = Fake.start_link(nodes: [{@node_name, @host, @port}])

    if setup_fun = Keyword.get(opts, :before_start) do
      setup_fun.(fake)
    end

    worker_opts =
      [
        transport: Fake,
        host: @host,
        port: @port,
        connect_opts: [fake: fake],
        node_name: @node_name
      ]
      |> maybe_put(opts, :counters)

    pool_opts =
      [
        worker: {NodePool, worker_opts},
        pool_size: pool_size,
        lazy: false
      ]
      |> maybe_put(opts, :worker_idle_timeout)
      |> maybe_put(opts, :max_idle_pings)

    {:ok, pool} = NimblePool.start_link(pool_opts)

    ExUnit.Callbacks.on_exit(fn ->
      # Stop the pool first so `terminate_worker/3` can still call the
      # fake's `close/1`. Swallow shutdown exits since linked processes
      # may already be gone.
      stop_quietly(pool)
      stop_quietly(fake)
    end)

    %{fake: fake, pool: pool, counters: Keyword.get(opts, :counters)}
  end

  defp base_init_opts(extras \\ []) do
    [
      transport: Fake,
      host: @host,
      port: @port,
      connect_opts: [fake: nil],
      node_name: @node_name
    ]
    |> Keyword.merge(extras)
  end

  defp maybe_put(acc, opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> Keyword.put(acc, key, value)
      :error -> acc
    end
  end

  defp stop_quietly(pid) do
    if Process.alive?(pid) do
      Process.unlink(pid)
      ref = Process.monitor(pid)
      Process.exit(pid, :shutdown)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      after
        1_000 -> :ok
      end
    end
  end

  describe "init_pool/1 features stashing" do
    test "passes through a non-empty :features MapSet" do
      features = MapSet.new([:compression])

      opts = base_init_opts(features: features)

      assert {:ok, pool_state} = NodePool.init_pool(opts)
      assert Keyword.fetch!(pool_state, :features) == features
    end

    test "defaults :features to an empty MapSet when omitted" do
      opts = base_init_opts()

      assert {:ok, pool_state} = NodePool.init_pool(opts)
      assert Keyword.fetch!(pool_state, :features) == MapSet.new()
    end
  end

  describe "checkout!/3" do
    test "runs the fun with a pool-owned connection and returns its result" do
      %{fake: fake, pool: pool} = start_pool!(1)
      Fake.script_command(fake, @node_name, {:ok, <<1, 2, 3>>})

      assert {:ok, <<1, 2, 3>>} =
               NodePool.checkout!(
                 pool,
                 fn conn ->
                   {Fake.command(conn, <<"req">>, 1_000), conn}
                 end,
                 1_000
               )
    end

    test "second concurrent checkout waits until the first checks in" do
      %{fake: fake, pool: pool} = start_pool!(1)
      Fake.script_command(fake, @node_name, {:ok, <<"first">>})
      Fake.script_command(fake, @node_name, {:ok, <<"second">>})

      parent = self()

      # First caller holds the single worker for ~100ms before checking in.
      task_a =
        Task.async(fn ->
          NodePool.checkout!(
            pool,
            fn conn ->
              send(parent, {:a_has_worker, System.monotonic_time(:millisecond)})
              result = Fake.command(conn, <<"req">>, 1_000)
              Process.sleep(100)
              {result, conn}
            end,
            5_000
          )
        end)

      # Make sure task_a has the worker before task_b attempts checkout.
      assert_receive {:a_has_worker, t_a_start}, 500

      task_b =
        Task.async(fn ->
          NodePool.checkout!(
            pool,
            fn conn ->
              send(parent, {:b_has_worker, System.monotonic_time(:millisecond)})
              {Fake.command(conn, <<"req">>, 1_000), conn}
            end,
            5_000
          )
        end)

      assert {:ok, <<"first">>} = Task.await(task_a, 1_000)
      assert {:ok, <<"second">>} = Task.await(task_b, 1_000)

      # task_b could only check out after task_a returned; ensure the wait
      # was observable (>= 50ms of overlap with task_a's 100ms sleep).
      assert_receive {:b_has_worker, t_b_start}, 500
      assert t_b_start - t_a_start >= 50
    end

    test "checkin :close removes the worker and a fresh connect runs on next checkout" do
      %{fake: fake, pool: pool} = start_pool!(1)
      Fake.script_command(fake, @node_name, {:ok, <<"first">>})
      Fake.script_command(fake, @node_name, {:ok, <<"second">>})

      {:ok, first_ref, <<"first">>} =
        NodePool.checkout!(
          pool,
          fn conn ->
            {:ok, body} = Fake.command(conn, <<"req">>, 1_000)
            {{:ok, conn.ref, body}, :close}
          end,
          1_000
        )

      {:ok, second_ref, <<"second">>} =
        NodePool.checkout!(
          pool,
          fn conn ->
            {:ok, body} = Fake.command(conn, <<"req">>, 1_000)
            {{:ok, conn.ref, body}, conn}
          end,
          1_000
        )

      # The :close checkin replaces the worker, so the fake returns a new
      # opaque conn ref for the second checkout.
      assert first_ref != second_ref
    end

    test "checkout timeout returns :pool_timeout error without blocking indefinitely" do
      %{pool: pool} = start_pool!(1)

      parent = self()

      # Hold the only worker to force the next checkout into timeout.
      Task.start(fn ->
        NodePool.checkout!(
          pool,
          fn conn ->
            send(parent, :holding)
            Process.sleep(500)
            {:ok, conn}
          end,
          5_000
        )
      end)

      assert_receive :holding, 500

      assert {:error, %Error{code: :pool_timeout}} =
               NodePool.checkout!(pool, fn conn -> {:ok, conn} end, 50)
    end
  end

  describe "warm-up" do
    test "opens pool_size workers eagerly before any checkout" do
      pool_size = 3
      %{fake: fake} = start_pool!(pool_size)

      # NimblePool's eager init reduces over 1..pool_size in
      # `Supervisor.init/1`, so by the time `start_link/1` returns the
      # fake has observed exactly `pool_size` connect calls — no
      # checkout was needed to drive any of them.
      assert Fake.connect_count(fake, @node_name) == pool_size
    end

    test "pool_size: N supports N concurrent checkouts without queuing" do
      pool_size = 3
      %{fake: fake, pool: pool} = start_pool!(pool_size)

      for _ <- 1..pool_size do
        Fake.script_command(fake, @node_name, {:ok, <<"ok">>})
      end

      parent = self()

      # All workers were warmed up at start; concurrent checkouts must
      # not pay any extra connect cost. Capture connect_count before
      # and after to prove no fresh connect happened during checkout.
      before = Fake.connect_count(fake, @node_name)

      tasks =
        for i <- 1..pool_size do
          Task.async(fn ->
            NodePool.checkout!(
              pool,
              fn conn ->
                send(parent, {:holding, i})
                # Hold long enough that all `pool_size` checkouts are
                # in flight at once; if any of them queued, the test
                # would deadlock waiting for `:holding` messages.
                Process.sleep(50)
                {Fake.command(conn, <<"req">>, 1_000), conn}
              end,
              1_000
            )
          end)
        end

      for i <- 1..pool_size do
        assert_receive {:holding, ^i}, 500
      end

      Enum.each(tasks, fn t -> assert {:ok, <<"ok">>} = Task.await(t, 1_000) end)
      assert Fake.connect_count(fake, @node_name) == before
    end

    test "partial connect failure keeps the pool usable on the remaining workers" do
      # Script the first connect to fail; subsequent connects fall
      # through to the default success path. NimblePool's eager init
      # reduce will observe 1 failure + 2 successes for pool_size: 3,
      # leaving 2 workers immediately available.
      pool_size = 3

      %{fake: fake, pool: pool} =
        start_pool!(pool_size,
          before_start: fn fake ->
            Fake.script_connect(
              fake,
              @node_name,
              {:error, %Error{code: :connection_error, message: "scripted boot failure"}}
            )
          end
        )

      Fake.script_command(fake, @node_name, {:ok, <<"a">>})
      Fake.script_command(fake, @node_name, {:ok, <<"b">>})

      # 1 failed + 2 successful eager init calls before checkout is even
      # attempted. The failed worker triggers an async re-init via
      # `{:init_worker}`, but we do not depend on it here.
      assert Fake.connect_count(fake, @node_name) >= pool_size

      parent = self()

      tasks =
        for i <- 1..2 do
          Task.async(fn ->
            NodePool.checkout!(
              pool,
              fn conn ->
                send(parent, {:up, i})
                Process.sleep(30)
                {Fake.command(conn, <<"req">>, 1_000), conn}
              end,
              1_000
            )
          end)
        end

      assert_receive {:up, 1}, 500
      assert_receive {:up, 2}, 500

      results = Enum.map(tasks, &Task.await(&1, 1_000))
      assert Enum.all?(results, fn {:ok, body} -> body in [<<"a">>, <<"b">>] end)
    end

    test "failed worker eventually re-initialises so the pool returns to full size" do
      pool_size = 2

      %{fake: fake} =
        start_pool!(pool_size,
          before_start: fn fake ->
            # Fail the first connect only; the async re-init must
            # succeed against the default path.
            Fake.script_connect(
              fake,
              @node_name,
              {:error, %Error{code: :connection_error, message: "scripted boot failure"}}
            )
          end
        )

      # Eager init: 1 failure + 1 success → pool starts with 1 worker.
      # NimblePool sends itself `{:init_worker}` after the failure, so
      # connect_count climbs to >= pool_size + 1 once the async retry
      # lands. Poll briefly to avoid timing flakiness.
      deadline = System.monotonic_time(:millisecond) + 500

      :ok = wait_for_connect_count(fake, pool_size + 1, deadline)
    end
  end

  defp wait_for_connect_count(fake, target, deadline) do
    if Fake.connect_count(fake, @node_name) >= target do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now >= deadline do
        flunk(
          "connect_count never reached #{target} (last value: " <>
            "#{Fake.connect_count(fake, @node_name)})"
        )
      else
        Process.sleep(10)
        wait_for_connect_count(fake, target, deadline)
      end
    end
  end

  describe "idle eviction" do
    test "workers checked in before the deadline are kept" do
      # Idle timeout is long enough that a quick checkout + checkin
      # cycle finishes well within the window; ping should not evict.
      %{fake: fake, pool: pool} = start_pool!(1, worker_idle_timeout: 200)
      Fake.script_command(fake, @node_name, {:ok, <<"ok">>})

      first_ref =
        NodePool.checkout!(
          pool,
          fn conn ->
            {:ok, <<"ok">>} = Fake.command(conn, <<"req">>, 1_000)
            {conn.ref, conn}
          end,
          1_000
        )

      # Wait long enough for at least one ping cycle, but not long
      # enough for the worker to have sat idle past the deadline.
      Process.sleep(50)

      Fake.script_command(fake, @node_name, {:ok, <<"ok2">>})

      second_ref =
        NodePool.checkout!(
          pool,
          fn conn ->
            {:ok, <<"ok2">>} = Fake.command(conn, <<"req">>, 1_000)
            {conn.ref, conn}
          end,
          1_000
        )

      assert first_ref == second_ref
      assert Fake.close_count(fake, @node_name) == 0
    end

    test "workers that sit idle past the deadline are closed and replaced" do
      # Short deadline + quiet pool: the ping cycle must evict the
      # single worker, Fake observes a `{:close, ref}`, and the next
      # checkout opens a fresh connection with a new opaque ref.
      %{fake: fake, pool: pool} =
        start_pool!(1, worker_idle_timeout: 50, max_idle_pings: 2)

      Fake.script_command(fake, @node_name, {:ok, <<"first">>})

      first_ref =
        NodePool.checkout!(
          pool,
          fn conn ->
            {:ok, <<"first">>} = Fake.command(conn, <<"req">>, 1_000)
            {conn.ref, conn}
          end,
          1_000
        )

      connects_before = Fake.connect_count(fake, @node_name)

      # Let at least one ping cycle fire past the deadline. NimblePool
      # schedules the first `:check_idle` at `worker_idle_timeout`, so
      # 200 ms is comfortably past two cycles.
      :ok = wait_for_close_count(fake, 1, 500)

      assert Fake.close_count(fake, @node_name) == 1

      Fake.script_command(fake, @node_name, {:ok, <<"fresh">>})

      second_ref =
        NodePool.checkout!(
          pool,
          fn conn ->
            {:ok, <<"fresh">>} = Fake.command(conn, <<"req">>, 1_000)
            {conn.ref, conn}
          end,
          1_000
        )

      # Fresh worker: new opaque ref, one extra connect recorded.
      assert first_ref != second_ref
      assert Fake.connect_count(fake, @node_name) >= connects_before + 1
    end

    test "max_idle_pings bounds how many workers are evicted per cycle" do
      # pool_size: 4, max_idle_pings: 1 → a single verification cycle
      # closes at most one worker. Sample the first close then
      # assert at most one more could have slipped in before the
      # measurement — without the bound all four would close in the
      # same cycle.
      pool_size = 4

      %{fake: fake} =
        start_pool!(pool_size, worker_idle_timeout: 50, max_idle_pings: 1)

      :ok = wait_for_close_count(fake, 1, 300)
      observed = Fake.close_count(fake, @node_name)

      # Polling granularity is 10 ms and cycles fire every 50 ms, so
      # at most one extra cycle can land between the condition turning
      # true and the sample below.
      assert observed <= 2,
             "expected max_idle_pings: 1 to cap evictions per cycle, got #{observed}"
    end
  end

  defp wait_for_close_count(fake, target, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_close_count(fake, target, deadline)
  end

  defp do_wait_for_close_count(fake, target, deadline) do
    if Fake.close_count(fake, @node_name) >= target do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now >= deadline do
        flunk(
          "close_count never reached #{target} (last value: " <>
            "#{Fake.close_count(fake, @node_name)})"
        )
      else
        Process.sleep(10)
        do_wait_for_close_count(fake, target, deadline)
      end
    end
  end

  describe "node counters" do
    test "in_flight is incremented on checkout and decremented on checkin", ctx do
      _ = ctx
      counters = NodeCounters.new()
      %{fake: fake, pool: pool} = start_pool!(1, counters: counters)
      Fake.script_command(fake, @node_name, {:ok, <<"ok">>})

      parent = self()

      task =
        Task.async(fn ->
          NodePool.checkout!(
            pool,
            fn conn ->
              send(parent, {:in_flight, NodeCounters.in_flight(counters)})
              # Hold the worker long enough for the parent to observe the
              # incremented slot before we check in.
              Process.sleep(50)
              {Fake.command(conn, <<"req">>, 1_000), conn}
            end,
            1_000
          )
        end)

      assert_receive {:in_flight, 1}, 500

      assert {:ok, <<"ok">>} = Task.await(task, 1_000)

      # `Task.await/2` returns when the Task finishes; the task sends
      # the pool's checkin message just before returning, so it may not
      # have been processed yet. Force the pool to drain its mailbox.
      _ = :sys.get_state(pool)

      # After check-in the slot is back to zero; no transport failure was
      # observed, so :failed stays at zero.
      assert NodeCounters.in_flight(counters) == 0
      assert NodeCounters.failed(counters) == 0
    end

    test "close without :failure tag does not bump :failed", _ctx do
      counters = NodeCounters.new()
      %{fake: fake, pool: pool} = start_pool!(1, counters: counters)
      Fake.script_command(fake, @node_name, {:ok, <<"ok">>})

      # :close drops the worker but does not count a failure — the
      # caller used :close for a benign reason (e.g. a test forcing
      # reconnect).
      assert {:ok, <<"ok">>} =
               NodePool.checkout!(
                 pool,
                 fn conn ->
                   {Fake.command(conn, <<"req">>, 1_000), :close}
                 end,
                 1_000
               )

      # `NodePool.checkout!/3` sends the checkin message asynchronously
      # and returns; `:sys.get_state/1` forces the pool to process the
      # mailbox up to and including that message, so counters reflect
      # the checkin before we assert.
      _ = :sys.get_state(pool)

      assert NodeCounters.in_flight(counters) == 0
      assert NodeCounters.failed(counters) == 0
    end

    test "{:close, :failure} checkin bumps :failed and removes the worker", _ctx do
      counters = NodeCounters.new()
      %{fake: fake, pool: pool} = start_pool!(1, counters: counters)

      # First checkout: transport reply is a scripted network error.
      Fake.script_command(
        fake,
        @node_name,
        {:error, %Error{code: :network_error, message: "boom"}}
      )

      # Second checkout: a replacement worker connects fresh and the
      # command succeeds, proving the failing worker was removed.
      Fake.script_command(fake, @node_name, {:ok, <<"ok">>})

      assert {:error, %Error{code: :network_error}} =
               NodePool.checkout!(
                 pool,
                 fn conn ->
                   {Fake.command(conn, <<"req">>, 1_000), {:close, :failure}}
                 end,
                 1_000
               )

      # Synchronise with the pool so handle_checkin has run before we
      # assert on counter state (checkin is fire-and-forget from the
      # caller's perspective).
      _ = :sys.get_state(pool)

      assert NodeCounters.in_flight(counters) == 0
      assert NodeCounters.failed(counters) == 1

      # Next checkout succeeds against a fresh worker; :failed is not
      # bumped because no failure was signalled this time.
      assert {:ok, <<"ok">>} =
               NodePool.checkout!(
                 pool,
                 fn conn ->
                   {Fake.command(conn, <<"req">>, 1_000), conn}
                 end,
                 1_000
               )

      _ = :sys.get_state(pool)

      assert NodeCounters.failed(counters) == 1
      assert NodeCounters.in_flight(counters) == 0
    end

    test "caller crash while checked out decrements :in_flight", _ctx do
      counters = NodeCounters.new()
      %{pool: pool} = start_pool!(1, counters: counters)

      parent = self()

      {:ok, task_pid} =
        Task.start(fn ->
          NodePool.checkout!(
            pool,
            fn _conn ->
              send(parent, :checked_out)
              # Hang until killed so the pool observes a crashed caller.
              Process.sleep(:infinity)
            end,
            5_000
          )
        end)

      assert_receive :checked_out, 500

      # After the task crashes, NimblePool fires handle_cancelled(:checked_out, ...)
      # which must decrement :in_flight — otherwise the slot would be
      # stuck above zero until the pool restarts.
      Process.exit(task_pid, :kill)

      wait_until(
        fn -> NodeCounters.in_flight(counters) == 0 end,
        500,
        "in_flight never returned to zero after caller crash"
      )

      # No transport-class failure was signalled, so :failed stays zero.
      assert NodeCounters.failed(counters) == 0
    end

    test "idle worker eviction leaves counters unchanged", _ctx do
      counters = NodeCounters.new()

      %{fake: fake} =
        start_pool!(1,
          counters: counters,
          worker_idle_timeout: 10,
          max_idle_pings: 1
        )

      # The warmed worker was never checked out, so idle eviction must not
      # decrement `:in_flight` below zero while NimblePool tears down
      # idle workers through `terminate_worker/3`.
      :ok = wait_for_close_count(fake, 1, 500)

      assert NodeCounters.in_flight(counters) == 0
      assert NodeCounters.failed(counters) == 0
    end

    test "pool_timeout does not bump :failed", _ctx do
      counters = NodeCounters.new()
      %{pool: pool} = start_pool!(1, counters: counters)

      parent = self()

      # Hold the only worker so the next checkout hits pool_timeout.
      Task.start(fn ->
        NodePool.checkout!(
          pool,
          fn conn ->
            send(parent, :holding)
            Process.sleep(500)
            {:ok, conn}
          end,
          5_000
        )
      end)

      assert_receive :holding, 500

      assert {:error, %Error{code: :pool_timeout}} =
               NodePool.checkout!(pool, fn conn -> {:ok, conn} end, 50)

      # Pool-level timeout means the caller never ran fun against a
      # live connection, so it is not a node-health signal.
      assert NodeCounters.failed(counters) == 0
    end

    test "pool without counters still works (nil-safe callbacks)", _ctx do
      %{fake: fake, pool: pool} = start_pool!(1)
      Fake.script_command(fake, @node_name, {:ok, <<"ok">>})

      assert {:ok, <<"ok">>} =
               NodePool.checkout!(
                 pool,
                 fn conn -> {Fake.command(conn, <<"req">>, 1_000), conn} end,
                 1_000
               )

      # A later checkout that signals failure must not crash either.
      Fake.script_command(
        fake,
        @node_name,
        {:error, %Error{code: :network_error, message: "x"}}
      )

      assert {:error, %Error{code: :network_error}} =
               NodePool.checkout!(
                 pool,
                 fn conn ->
                   {Fake.command(conn, <<"req">>, 1_000), {:close, :failure}}
                 end,
                 1_000
               )
    end
  end

  describe "checkout telemetry" do
    test "emits :start and :stop with :node_name and :pool_pid on the happy path" do
      %{fake: fake, pool: pool} = start_pool!(1)
      Fake.script_command(fake, @node_name, {:ok, <<"ok">>})

      handler = attach_checkout_handler(:happy)

      try do
        assert {:ok, <<"ok">>} =
                 NodePool.checkout!(
                   @node_name,
                   pool,
                   fn conn -> {Fake.command(conn, <<"req">>, 1_000), conn} end,
                   1_000
                 )

        assert_receive {:event, [:aerospike, :pool, :checkout, :start],
                        %{system_time: _, monotonic_time: _},
                        %{node_name: @node_name, pool_pid: ^pool, telemetry_span_context: ctx}},
                       500

        assert_receive {:event, [:aerospike, :pool, :checkout, :stop], %{duration: _},
                        %{node_name: @node_name, pool_pid: ^pool, telemetry_span_context: ^ctx}},
                       500
      after
        :telemetry.detach(handler)
      end
    end

    test "emits a :stop event when the pool checkout times out" do
      %{pool: pool} = start_pool!(1)
      parent = self()

      # Hold the only worker so the next checkout hits pool_timeout.
      Task.start(fn ->
        NodePool.checkout!(
          @node_name,
          pool,
          fn conn ->
            send(parent, :holding)
            Process.sleep(300)
            {:ok, conn}
          end,
          5_000
        )
      end)

      assert_receive :holding, 500

      handler = attach_checkout_handler(:timeout)

      try do
        assert {:error, %Error{code: :pool_timeout}} =
                 NodePool.checkout!(
                   @node_name,
                   pool,
                   fn conn -> {:ok, conn} end,
                   50
                 )

        assert_receive {:event, [:aerospike, :pool, :checkout, :start], _m,
                        %{node_name: @node_name, pool_pid: ^pool, telemetry_span_context: ctx}},
                       500

        assert_receive {:event, [:aerospike, :pool, :checkout, :stop], %{duration: _},
                        %{node_name: @node_name, pool_pid: ^pool, telemetry_span_context: ^ctx}},
                       500
      after
        :telemetry.detach(handler)
      end
    end

    test "emits a :stop event when the pool is unavailable" do
      %{pool: pool} = start_pool!(1)
      handler = attach_checkout_handler(:invalid_node)

      stop_quietly(pool)

      try do
        assert {:error, %Error{code: :invalid_node}} =
                 NodePool.checkout!(
                   @node_name,
                   pool,
                   fn conn -> {:ok, conn} end,
                   50
                 )

        assert_receive {:event, [:aerospike, :pool, :checkout, :start], _m,
                        %{node_name: @node_name, pool_pid: ^pool}},
                       500

        assert_receive {:event, [:aerospike, :pool, :checkout, :stop], %{duration: _},
                        %{node_name: @node_name, pool_pid: ^pool}},
                       500
      after
        :telemetry.detach(handler)
      end
    end

    test "checkout!/3 degrades :node_name to nil in span metadata" do
      %{fake: fake, pool: pool} = start_pool!(1)
      Fake.script_command(fake, @node_name, {:ok, <<"ok">>})

      handler = attach_checkout_handler(:nil_node)

      try do
        assert {:ok, <<"ok">>} =
                 NodePool.checkout!(
                   pool,
                   fn conn -> {Fake.command(conn, <<"req">>, 1_000), conn} end,
                   1_000
                 )

        assert_receive {:event, [:aerospike, :pool, :checkout, :start], _m,
                        %{node_name: nil, pool_pid: ^pool, telemetry_span_context: ctx}},
                       500

        assert_receive {:event, [:aerospike, :pool, :checkout, :stop], _m,
                        %{node_name: nil, pool_pid: ^pool, telemetry_span_context: ^ctx}},
                       500
      after
        :telemetry.detach(handler)
      end
    end

    test "emits :exception when the checkout callback raises" do
      %{pool: pool} = start_pool!(1)
      handler = attach_checkout_handler(:exception)

      try do
        assert_raise RuntimeError, "boom", fn ->
          NodePool.checkout!(
            @node_name,
            pool,
            fn _conn -> raise "boom" end,
            1_000
          )
        end

        assert_receive {:event, [:aerospike, :pool, :checkout, :start], _m,
                        %{node_name: @node_name, pool_pid: ^pool, telemetry_span_context: ctx}},
                       500

        assert_receive {:event, [:aerospike, :pool, :checkout, :exception], %{duration: _},
                        %{node_name: @node_name, pool_pid: ^pool, telemetry_span_context: ^ctx}},
                       500

        refute_receive {:event, [:aerospike, :pool, :checkout, :stop], _,
                        %{node_name: @node_name, pool_pid: ^pool, telemetry_span_context: ^ctx}},
                       100
      after
        :telemetry.detach(handler)
      end
    end
  end

  # Captured forwarder to avoid the "local function handler" performance
  # warning from `:telemetry.attach/4` for anonymous captures.
  @doc false
  def forward(event, measurements, metadata, test_pid) do
    send(test_pid, {:event, event, measurements, metadata})
  end

  defp attach_checkout_handler(tag) do
    handler_id = {__MODULE__, tag, make_ref()}
    prefix = Telemetry.pool_checkout_span()
    events = Enum.map([:start, :stop, :exception], &(prefix ++ [&1]))

    :ok = :telemetry.attach_many(handler_id, events, &__MODULE__.forward/4, self())

    handler_id
  end

  defp wait_until(check, timeout_ms, message) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(check, deadline, message)
  end

  defp do_wait_until(check, deadline, message) do
    if check.() do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now >= deadline do
        flunk(message)
      else
        Process.sleep(10)
        do_wait_until(check, deadline, message)
      end
    end
  end
end
