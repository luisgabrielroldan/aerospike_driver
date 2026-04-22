defmodule Aerospike.TenderPoolTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.NodeSupervisor
  alias Aerospike.PartitionMapWriter
  alias Aerospike.TableOwner
  alias Aerospike.Tender
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  setup context do
    name = :"tender_pool_#{:erlang.phash2(context.test)}"

    {:ok, fake} =
      Fake.start_link(
        nodes: [
          {"A1", "10.0.0.1", 3000},
          {"B1", "10.0.0.2", 3000}
        ]
      )

    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)

    {:ok, writer} = PartitionMapWriter.start_link(name: name, tables: tables)
    {:ok, node_sup} = NodeSupervisor.start_link(name: name)

    on_exit(fn ->
      stop_if_alive(node_sup)
      stop_if_alive(writer)
      stop_if_alive(owner)
      stop_if_alive(fake)
    end)

    %{
      name: name,
      fake: fake,
      tables: tables,
      node_sup_name: NodeSupervisor.sup_name(name),
      node_sup_pid: node_sup
    }
  end

  describe "bootstrap + pool lifecycle" do
    test "node added during bootstrap publishes a live pool pid", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx)

      :ok = Tender.tend_now(pid)

      assert {:ok, pool_pid} = Tender.pool_pid(pid, "A1")
      assert Process.alive?(pool_pid)

      # The pool is a child of the NodeSupervisor, not of the Tender.
      children = DynamicSupervisor.which_children(ctx.node_sup_pid)
      assert Enum.any?(children, fn {_id, p, _type, _mods} -> p == pool_pid end)
    end

    test "unknown node reports {:error, :unknown_node}", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx)
      :ok = Tender.tend_now(pid)

      assert {:error, :unknown_node} = Tender.pool_pid(pid, "ghost_node")
    end
  end

  describe "failure threshold + pool lifecycle" do
    test "flipping a node to :inactive terminates its pool and forgets its pid", ctx do
      # Threshold = 2. Each failing refresh cycle contributes one failure
      # event: peers-clear-std is skipped when peers-generation cannot
      # advance (the info call failed), and replicas is skipped because
      # the generation-guard sees no generation advance. Cycle 2 takes
      # the counter to 1, cycle 3 hits threshold=2 and flips the node to
      # :inactive, stopping the pool along the way.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}

      Enum.each(1..2, fn _ ->
        Fake.script_info_error(
          ctx.fake,
          "A1",
          ["partition-generation", "cluster-stable", "peers-generation"],
          err
        )
      end)

      {:ok, pid} = start_tender(ctx, failure_threshold: 2)

      :ok = Tender.tend_now(pid)
      {:ok, pool_pid} = Tender.pool_pid(pid, "A1")
      assert Process.alive?(pool_pid)

      # Cycles 2 and 3 drive the failure counter past the threshold.
      :ok = Tender.tend_now(pid)
      :ok = Tender.tend_now(pid)

      assert {:error, :unknown_node} = Tender.pool_pid(pid, "A1")
      refute Process.alive?(pool_pid)
    end
  end

  describe "pool start failure" do
    test "leaves the node out of state.nodes and retries next cycle", ctx do
      # Point the Tender at a supervisor name that is not registered. The
      # first tend call sees `{:error, :not_found}` from start_pool and
      # skips the node; the second cycle succeeds once the real supervisor
      # is registered under the same name.
      bogus_sup = :"bogus_#{:erlang.phash2(ctx.name)}"

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, node_supervisor: bogus_sup)

      :ok = Tender.tend_now(pid)

      assert {:error, :unknown_node} = Tender.pool_pid(pid, "A1")

      # Bring the real supervisor up under the bogus name and re-script the
      # bootstrap — the Tender repeats the seed handshake because it never
      # registered A1 on the failing cycle.
      {:ok, real_sup} = DynamicSupervisor.start_link(strategy: :one_for_one, name: bogus_sup)
      on_exit(fn -> stop_if_alive(real_sup) end)

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      :ok = Tender.tend_now(pid)

      assert {:ok, pool_pid} = Tender.pool_pid(pid, "A1")
      assert Process.alive?(pool_pid)
    end
  end

  describe "pool opts plumbing" do
    # `:idle_timeout_ms` and `:max_idle_pings` live at the Tender level
    # and flow through `ensure_pool` into `NodeSupervisor.start_pool/2`.
    # The observable signal: with a tiny idle timeout, the fake sees the
    # pool close its single warm-up connection shortly after warm-up.
    test ":idle_timeout_ms from Tender reaches the NimblePool child", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, idle_timeout_ms: 50, max_idle_pings: 2)

      :ok = Tender.tend_now(pid)
      {:ok, pool_pid} = Tender.pool_pid(pid, "A1")
      assert Process.alive?(pool_pid)

      :ok = wait_for_close_count(ctx.fake, "A1", 1, 500)
      assert Fake.close_count(ctx.fake, "A1") >= 1
    end
  end

  describe "orphan pool cleanup on Tender start" do
    test "orphan pools from the previous incarnation are terminated", ctx do
      # Simulate: a previous Tender started a pool that survived the
      # Tender's death (NodeSupervisor lives longer under rest_for_one).
      # Start a dummy pool directly against the fake so the worker's
      # connect succeeds before Tender ever runs.
      {:ok, orphan} =
        NodeSupervisor.start_pool(ctx.node_sup_name,
          node_name: "orphan",
          transport: Fake,
          host: "10.0.0.2",
          port: 3000,
          connect_opts: [fake: ctx.fake],
          pool_size: 1
        )

      assert Process.alive?(orphan)

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx)

      # Orphan cleanup runs inside the Tender's init before any tend cycle.
      refute Process.alive?(orphan)

      :ok = Tender.tend_now(pid)

      # The only child now is the freshly-started A1 pool.
      {:ok, a1_pool} = Tender.pool_pid(pid, "A1")
      children = DynamicSupervisor.which_children(ctx.node_sup_pid)
      assert children == [{:undefined, a1_pool, :worker, [NimblePool]}]
    end
  end

  ## Helpers

  defp start_tender(ctx, opts \\ []) do
    base_opts = [
      name: ctx.name,
      transport: Fake,
      connect_opts: [fake: ctx.fake],
      seeds: [{"10.0.0.1", 3000}],
      namespaces: ["test"],
      tables: ctx.tables,
      tend_trigger: :manual,
      node_supervisor: Keyword.get(opts, :node_supervisor, ctx.node_sup_name),
      pool_size: Keyword.get(opts, :pool_size, 1)
    ]

    tender_opts =
      base_opts
      |> maybe_put(:failure_threshold, Keyword.get(opts, :failure_threshold))
      |> maybe_put(:idle_timeout_ms, Keyword.get(opts, :idle_timeout_ms))
      |> maybe_put(:max_idle_pings, Keyword.get(opts, :max_idle_pings))

    {:ok, pid} = Tender.start_link(tender_opts)

    on_exit(fn -> stop_tender(pid) end)
    {:ok, pid}
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp script_bootstrap_node(fake, node_name, partition_gen, replicas_value) do
    Fake.script_info(fake, node_name, ["node", "features"], %{
      "node" => node_name,
      "features" => ""
    })

    Fake.script_info(
      fake,
      node_name,
      ["partition-generation", "cluster-stable", "peers-generation"],
      %{
        "partition-generation" => Integer.to_string(partition_gen),
        "cluster-stable" => "deadbeef",
        "peers-generation" => "1"
      }
    )

    Fake.script_info(fake, node_name, ["peers-clear-std"], %{
      "peers-clear-std" => "0,3000,[]"
    })

    Fake.script_info(fake, node_name, ["replicas"], %{"replicas" => replicas_value})
  end

  defp stop_tender(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)

      # `GenServer.stop/3` can still race a concurrent exit after the
      # `Process.alive?/1` check; other Tender tests already absorb that.
      try do
        GenServer.stop(pid, :normal, 2_000)
      catch
        :exit, _ -> :ok
      end

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        2_000 -> :ok
      end
    end
  end

  defp wait_for_close_count(fake, node_id, target, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_close_count(fake, node_id, target, deadline)
  end

  defp do_wait_for_close_count(fake, node_id, target, deadline) do
    cond do
      Fake.close_count(fake, node_id) >= target ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("timed out waiting for #{target} close(s) on #{inspect(node_id)}")

      true ->
        Process.sleep(10)
        do_wait_for_close_count(fake, node_id, target, deadline)
    end
  end

  defp stop_if_alive(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :shutdown)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        2_000 -> :ok
      end
    end
  end
end
