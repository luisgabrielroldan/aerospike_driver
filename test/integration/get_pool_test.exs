defmodule Aerospike.Integration.GetPoolTest do
  @moduledoc """
  Pool-path integration tests for `Aerospike.get/3`.

  These tests exercise the `Aerospike.Cluster.NodePool` checkout path end-to-end against a
  real Aerospike server: a serial batch that reuses the same pool, a
  concurrent batch that forces checkout queueing with a small pool size,
  and a smoke test that drives a `pool_size: 10` pool with 100 concurrent
  GETs while asserting eager warm-up via NimblePool's internal resource
  queue.

  A separate test starts an `Aerospike.Cluster.NodeSupervisor` directly and runs
  `Aerospike.Cluster.NodePool.checkout!/3` against the real server with a short
  `:idle_timeout_ms`, proving end-to-end that an idle socket is closed
  by `handle_ping/2` and that the next checkout re-opens a fresh
  worker. The Fake-based unit tests in `Aerospike.Cluster.NodePoolTest` cover
  the mechanics; this test only proves the TCP transport cooperates
  with eviction and re-init.

  The deep unit-level assertion that "only one connection is opened for
  N serial GETs" is covered by `Aerospike.Cluster.NodePoolTest` against
  `Transport.Fake`, which counts `connect/3` invocations directly. These
  integration tests assert only the user-visible outcome (every call
  returns `{:error, :key_not_found}` for a missing key) so that adding
  library-level instrumentation is not required.
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :ce

  alias Aerospike.Cluster.NodePool
  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Test.IntegrationSupport
  alias Aerospike.Transport.Tcp

  @host "localhost"
  @port 3000
  @namespace "test"
  @set "spike_pool"
  @concurrent_tasks 20
  @concurrent_pool_size 4
  @serial_gets 25
  @smoke_pool_size 10
  @smoke_concurrent_tasks 100

  setup context do
    IntegrationSupport.probe_aerospike!(@host, @port)
    IntegrationSupport.wait_for_seed_ready!(@host, @port, @namespace, 5_000)
    pool_size = Map.get(context, :pool_size, @concurrent_pool_size)
    name = IntegrationSupport.unique_atom("spike_get_pool_cluster")

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["#{@host}:#{@port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: pool_size
      )

    IntegrationSupport.wait_for_tender_ready!(name, 5_000)

    on_exit(fn ->
      IntegrationSupport.stop_supervisor_quietly(sup)
    end)

    %{cluster: name, pool_size: pool_size}
  end

  @tag pool_size: 1
  test "serial GETs for missing keys all return :key_not_found through one pool", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster)

    for i <- 1..@serial_gets do
      key = IntegrationSupport.unique_key(@namespace, @set, "missing_serial_#{i}")
      assert {:error, %Error{code: :key_not_found}} = Aerospike.get(cluster, key)
    end
  end

  test "concurrent GETs with a small pool size all return :key_not_found", %{cluster: cluster} do
    assert Tender.ready?(cluster)

    tasks =
      for i <- 1..@concurrent_tasks do
        Task.async(fn ->
          key =
            IntegrationSupport.unique_key(@namespace, @set, "missing_concurrent_#{i}")

          Aerospike.get(cluster, key)
        end)
      end

    results = Task.await_many(tasks, 10_000)

    assert length(results) == @concurrent_tasks

    Enum.each(results, fn result ->
      assert {:error, %Error{code: :key_not_found}} = result
    end)
  end

  @tag pool_size: @smoke_pool_size
  test "smoke: 100 concurrent GETs on a warmed pool_size: 10 pool", %{cluster: cluster} do
    assert Tender.ready?(cluster)

    # The cluster has exactly one seed node in the dev docker setup.
    # Eager warm-up (`lazy: false` pinned in `NodeSupervisor`) must have
    # opened every configured worker during `Supervisor.init/1`, so by
    # the time `Tender.ready?` returns true the pool's resource queue
    # holds all `@smoke_pool_size` workers. Reach into NimblePool's
    # GenServer state to prove it — assertions on wall-clock latency
    # would be flaky on a shared CI box.
    pool_pid = resolve_single_pool(cluster)
    assert available_workers(pool_pid) == @smoke_pool_size

    tasks =
      for i <- 1..@smoke_concurrent_tasks do
        Task.async(fn ->
          key =
            IntegrationSupport.unique_key(@namespace, @set, "missing_smoke_#{i}")

          Aerospike.get(cluster, key)
        end)
      end

    results = Task.await_many(tasks, 15_000)

    assert length(results) == @smoke_concurrent_tasks

    Enum.each(results, fn result ->
      assert {:error, %Error{code: :key_not_found}} = result
    end)
  end

  test "idle pool left sitting past idle_timeout_ms evicts workers and reopens on next checkout" do
    # This test bypasses `Aerospike.start_link` because the Tender does
    # not thread `:idle_timeout_ms` through to `NodeSupervisor.start_pool/2`.
    # Starting the supervisor + pool directly still exercises the real
    # `Transport.Tcp` transport against the docker server, which is the
    # end-to-end guarantee this test needs to prove: `handle_ping/2`
    # closes a real TCP socket and `init_worker/1` opens a fresh one on
    # the next checkout.
    name = IntegrationSupport.unique_atom("spike_idle_sup")
    {:ok, sup} = NodeSupervisor.start_link(name: name)

    on_exit(fn ->
      IntegrationSupport.stop_supervisor_quietly(sup)
    end)

    idle_timeout_ms = 100
    pool_size = 2

    {:ok, pool_pid} =
      NodeSupervisor.start_pool(NodeSupervisor.sup_name(name),
        node_name: "idle_smoke_node",
        transport: Tcp,
        host: @host,
        port: @port,
        connect_opts: [],
        pool_size: pool_size,
        idle_timeout_ms: idle_timeout_ms,
        max_idle_pings: pool_size
      )

    assert available_workers(pool_pid) == pool_size

    # Capture a worker's underlying socket so we can prove the very
    # socket the pool held gets torn down by the ping cycle. Returning
    # the conn at check-in keeps the worker in the pool; the idle
    # timer starts from the moment of check-in.
    first_socket =
      NodePool.checkout!(
        pool_pid,
        fn conn -> {conn.socket, conn} end,
        1_000
      )

    assert is_port(first_socket) or is_tuple(first_socket),
           "Transport.Tcp connection must expose a :gen_tcp socket"

    # Wait for NimblePool's `:check_idle` cycle to fire past
    # `idle_timeout_ms` and for the evicted socket to actually close on
    # the kernel side. The queue length is not a reliable signal because
    # `{:remove, :idle}` schedules an async `init_worker/1` that
    # replenishes the queue within the same tick — the observable
    # end-to-end fact is that the exact socket the pool held is gone.
    deadline = System.monotonic_time(:millisecond) + 2_000
    :ok = wait_for_socket_closed(first_socket, deadline)

    # Next checkout returns a worker whose socket differs from the
    # evicted one. On a `pool_size: 2` pool the replacement may be
    # either the async-initialised slot or the sibling worker; both
    # cases satisfy "the original socket is not reused". If the sibling
    # is returned we prove separately below that NimblePool eventually
    # re-initialises the evicted slot so the pool stays at full size.
    second_socket =
      NodePool.checkout!(
        pool_pid,
        fn conn -> {conn.socket, conn} end,
        1_000
      )

    refute first_socket == second_socket

    # The pool is back to `pool_size` available workers (including the
    # one we just checked back in). Polling avoids racing with the
    # async `init_worker/1` that `remove_worker/3` schedules.
    refill_deadline = System.monotonic_time(:millisecond) + 2_000
    :ok = wait_for_available_at_least(pool_pid, pool_size, refill_deadline)
  end

  defp resolve_single_pool(cluster) do
    # The dev docker compose exposes one Aerospike node on 3000, so
    # `Tender.tables/1` + a direct ETS read would return exactly one
    # owner, but reaching through `DynamicSupervisor.which_children/1`
    # keeps the test insulated from owner-table shape changes.
    sup_name = NodeSupervisor.sup_name(cluster)

    [{_id, pool_pid, _type, _mods} | _] =
      DynamicSupervisor.which_children(Process.whereis(sup_name))

    pool_pid
  end

  defp available_workers(pool_pid) do
    state = :sys.get_state(pool_pid)
    :queue.len(state.resources)
  end

  defp wait_for_available_at_least(pool_pid, target, deadline) do
    cond do
      available_workers(pool_pid) >= target ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(
          "pool resource queue never refilled to #{target} " <>
            "(last: #{available_workers(pool_pid)})"
        )

      true ->
        Process.sleep(20)
        wait_for_available_at_least(pool_pid, target, deadline)
    end
  end

  defp wait_for_socket_closed(socket, deadline) do
    case :inet.port(socket) do
      {:error, _} ->
        :ok

      {:ok, _} ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("evicted socket #{inspect(socket)} is still open")
        else
          Process.sleep(20)
          wait_for_socket_closed(socket, deadline)
        end
    end
  end
end
