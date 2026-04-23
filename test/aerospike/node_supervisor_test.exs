defmodule Aerospike.Cluster.NodeSupervisorTest do
  use ExUnit.Case, async: true

  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Transport.Fake

  setup context do
    name = :"node_sup_#{:erlang.phash2(context.test)}"
    %{name: name}
  end

  describe "start_link/1" do
    test "starts an alive DynamicSupervisor with zero children", ctx do
      {:ok, pid} = NodeSupervisor.start_link(name: ctx.name)

      assert Process.alive?(pid)
      assert Process.whereis(NodeSupervisor.sup_name(ctx.name)) == pid
      assert DynamicSupervisor.which_children(pid) == []

      assert DynamicSupervisor.count_children(pid) == %{
               active: 0,
               specs: 0,
               supervisors: 0,
               workers: 0
             }
    end

    test "requires :name to be an atom" do
      assert_raise ArgumentError, ~r/:name must be an atom/, fn ->
        NodeSupervisor.start_link(name: "not_an_atom")
      end
    end

    test "requires :name option" do
      assert_raise KeyError, fn ->
        NodeSupervisor.start_link([])
      end
    end
  end

  describe "sup_name/1" do
    test "derives a registered name atom from the cluster name" do
      assert NodeSupervisor.sup_name(:my_cluster) == :my_cluster_node_sup
    end
  end

  describe "start_pool/2 pool-level opts" do
    # `:idle_timeout_ms` and `:max_idle_pings` must land on the
    # NimblePool child started by `start_pool/2`. The observable
    # contract is: with a tiny idle timeout and a single worker, the
    # fake transport sees a close well before the default 55_000 ms
    # deadline would fire.
    test ":idle_timeout_ms drives NimblePool's worker_idle_timeout", ctx do
      {:ok, sup} = NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_quietly(sup) end)

      {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])
      on_exit(fn -> stop_quietly(fake) end)

      {:ok, pool} =
        NodeSupervisor.start_pool(NodeSupervisor.sup_name(ctx.name),
          node_name: "A1",
          transport: Fake,
          host: "10.0.0.1",
          port: 3000,
          connect_opts: [fake: fake],
          pool_size: 1,
          idle_timeout_ms: 50,
          max_idle_pings: 2
        )

      assert Process.alive?(pool)

      # NimblePool's check-idle cycle fires at `worker_idle_timeout`.
      # The worker is idle from the moment it warms up, so the first
      # cycle past 50 ms must evict it.
      :ok = wait_for_close_count(fake, "A1", 1, 500)
      assert Fake.close_count(fake, "A1") >= 1
    end
  end

  describe "stop_pool/2" do
    test "returns {:error, :not_found} when the supervisor name is unregistered" do
      # Never started — whereis returns nil.
      fake_pid = spawn(fn -> :ok end)
      assert {:error, :not_found} = NodeSupervisor.stop_pool(:nonexistent_sup_name, fake_pid)
    end

    test "returns {:error, :not_found} when pid is not a child", ctx do
      {:ok, _pid} = NodeSupervisor.start_link(name: ctx.name)
      stranger = spawn(fn -> Process.sleep(:infinity) end)

      assert {:error, :not_found} =
               NodeSupervisor.stop_pool(NodeSupervisor.sup_name(ctx.name), stranger)
    end
  end

  defp wait_for_close_count(fake, node_id, target, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_close_count(fake, node_id, target, deadline)
  end

  defp do_wait_for_close_count(fake, node_id, target, deadline) do
    if Fake.close_count(fake, node_id) >= target do
      :ok
    else
      if System.monotonic_time(:millisecond) > deadline do
        flunk("timed out waiting for #{target} close(s) on #{inspect(node_id)}")
      else
        Process.sleep(10)
        do_wait_for_close_count(fake, node_id, target, deadline)
      end
    end
  end

  defp stop_quietly(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :shutdown)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        1_000 -> :ok
      end
    end
  end
end
