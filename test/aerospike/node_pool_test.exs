defmodule Aerospike.NodePoolTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.NodePool
  alias Aerospike.Transport.Fake

  @host "10.0.0.1"
  @port 3000
  @node_name "A1"

  defp start_pool!(pool_size) do
    {:ok, fake} = Fake.start_link(nodes: [{@node_name, @host, @port}])

    {:ok, pool} =
      NimblePool.start_link(
        worker:
          {NodePool,
           transport: Fake,
           host: @host,
           port: @port,
           connect_opts: [fake: fake],
           node_name: @node_name},
        pool_size: pool_size
      )

    ExUnit.Callbacks.on_exit(fn ->
      # Stop the pool first so `terminate_worker/3` can still call the
      # fake's `close/1`. Swallow shutdown exits since linked processes
      # may already be gone.
      stop_quietly(pool)
      stop_quietly(fake)
    end)

    %{fake: fake, pool: pool}
  end

  defp stop_quietly(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :shutdown)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      after
        1_000 -> :ok
      end
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
                   {Fake.command(conn, <<"req">>), conn}
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
              result = Fake.command(conn, <<"req">>)
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
              {Fake.command(conn, <<"req">>), conn}
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
            {:ok, body} = Fake.command(conn, <<"req">>)
            {{:ok, conn.ref, body}, :close}
          end,
          1_000
        )

      {:ok, second_ref, <<"second">>} =
        NodePool.checkout!(
          pool,
          fn conn ->
            {:ok, body} = Fake.command(conn, <<"req">>)
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
end
