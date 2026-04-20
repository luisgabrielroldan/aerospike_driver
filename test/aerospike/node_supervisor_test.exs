defmodule Aerospike.NodeSupervisorTest do
  use ExUnit.Case, async: true

  alias Aerospike.NodeSupervisor

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
end
