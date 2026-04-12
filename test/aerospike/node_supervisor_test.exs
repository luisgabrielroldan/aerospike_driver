defmodule Aerospike.NodeSupervisorTest do
  use ExUnit.Case, async: false

  alias Aerospike.NodeSupervisor

  describe "child_spec/1" do
    test "returns correct supervisor child spec" do
      spec = NodeSupervisor.child_spec(name: :test_ns)
      assert spec.id == {NodeSupervisor, :test_ns}
      assert spec.type == :supervisor
      assert spec.restart == :permanent
      assert spec.shutdown == :infinity
      assert {NodeSupervisor, :start_link, [[name: :test_ns]]} = spec.start
    end
  end

  describe "sup_name/1" do
    test "derives supervisor name from connection name" do
      assert NodeSupervisor.sup_name(:my_aero) == :my_aero_node_sup
    end
  end

  describe "stop_pool/2" do
    test "returns not_found for non-existent supervisor atom" do
      pool_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pool_pid, :kill) end)

      assert {:error, :not_found} =
               NodeSupervisor.stop_pool(:nonexistent_node_sup, pool_pid)
    end

    test "returns not_found for non-existent pool in running supervisor" do
      {:ok, sup} = DynamicSupervisor.start_link(strategy: :one_for_one)
      on_exit(fn -> safe_stop(sup) end)
      pool_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pool_pid, :kill) end)

      assert {:error, :not_found} = NodeSupervisor.stop_pool(sup, pool_pid)
    end

    test "accepts PID as supervisor reference" do
      {:ok, sup} = DynamicSupervisor.start_link(strategy: :one_for_one)
      on_exit(fn -> safe_stop(sup) end)
      pool_pid = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(pool_pid, :kill) end)

      assert {:error, :not_found} = NodeSupervisor.stop_pool(sup, pool_pid)
    end
  end

  describe "start_link/1" do
    test "starts a DynamicSupervisor with the derived name" do
      name = :"ns_test_#{:erlang.unique_integer([:positive])}"
      {:ok, pid} = NodeSupervisor.start_link(name: name)
      on_exit(fn -> safe_stop(pid) end)

      assert Process.whereis(NodeSupervisor.sup_name(name)) == pid
    end
  end

  defp safe_stop(pid) do
    DynamicSupervisor.stop(pid, :normal)
  catch
    :exit, _ -> :ok
  end
end
