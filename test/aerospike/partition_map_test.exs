defmodule Aerospike.Cluster.PartitionMapTest do
  use ExUnit.Case, async: true

  alias Aerospike.Cluster.PartitionMap
  alias Aerospike.Cluster.PartitionMap.PartitionOwners

  setup context do
    # Unique table prefix per test so tables do not collide across async cases.
    prefix = :"pm_#{context.test |> to_string() |> :erlang.phash2()}"
    {owners, node_gens} = PartitionMap.create_tables(prefix)

    on_exit(fn ->
      safe_delete(owners)
      safe_delete(node_gens)
    end)

    %{owners: owners, node_gens: node_gens}
  end

  describe "update/5 regime guard" do
    test "accepts an update when no entry exists", %{owners: tab} do
      assert :ok = PartitionMap.update(tab, "test", 0, 1, ["A1"])

      assert {:ok, %PartitionOwners{regime: 1, replicas: ["A1"]}} =
               PartitionMap.owners(tab, "test", 0)
    end

    test "accepts an update when regime rises", %{owners: tab} do
      :ok = PartitionMap.update(tab, "test", 0, 1, ["A1"])
      :ok = PartitionMap.update(tab, "test", 0, 2, ["A2", "A1"])

      assert {:ok, %PartitionOwners{regime: 2, replicas: ["A2", "A1"]}} =
               PartitionMap.owners(tab, "test", 0)
    end

    test "rejects an update when regime falls", %{owners: tab} do
      :ok = PartitionMap.update(tab, "test", 0, 5, ["A1"])

      assert {:error, :stale_regime} = PartitionMap.update(tab, "test", 0, 4, ["A2"])

      assert {:ok, %PartitionOwners{regime: 5, replicas: ["A1"]}} =
               PartitionMap.owners(tab, "test", 0)
    end

    test "equal regime overwrites idempotently", %{owners: tab} do
      :ok = PartitionMap.update(tab, "test", 0, 3, ["A1"])
      :ok = PartitionMap.update(tab, "test", 0, 3, ["A2", "A1"])

      assert {:ok, %PartitionOwners{regime: 3, replicas: ["A2", "A1"]}} =
               PartitionMap.owners(tab, "test", 0)
    end
  end

  describe "owners/3" do
    test "returns :unknown_partition when nothing is stored", %{owners: tab} do
      assert {:error, :unknown_partition} = PartitionMap.owners(tab, "test", 0)
    end
  end

  describe "complete?/2" do
    test "false when any partition is missing", %{owners: tab} do
      for id <- 0..4094 do
        :ok = PartitionMap.update(tab, "test", id, 1, ["A1"])
      end

      refute PartitionMap.complete?(tab, "test")
    end

    test "true only when every partition id has at least one replica", %{owners: tab} do
      for id <- 0..4095 do
        :ok = PartitionMap.update(tab, "test", id, 1, ["A1"])
      end

      assert PartitionMap.complete?(tab, "test")
    end

    test "unrelated namespaces do not make a namespace complete", %{owners: tab} do
      for id <- 0..4095 do
        :ok = PartitionMap.update(tab, "other", id, 1, ["A1"])
      end

      refute PartitionMap.complete?(tab, "test")
      assert PartitionMap.complete?(tab, "other")
    end
  end

  describe "clear/1" do
    test "removes every owners row", %{owners: tab} do
      :ok = PartitionMap.update(tab, "test", 0, 1, ["A1"])
      :ok = PartitionMap.update(tab, "other", 17, 2, ["B1"])

      assert :ok = PartitionMap.clear(tab)
      assert {:error, :unknown_partition} = PartitionMap.owners(tab, "test", 0)
      assert {:error, :unknown_partition} = PartitionMap.owners(tab, "other", 17)
    end
  end

  describe "drop_node/2" do
    test "removes the node from replica lists and keeps remaining replicas", %{owners: tab} do
      :ok = PartitionMap.update(tab, "test", 0, 1, ["A1", "A2"])

      assert :ok = PartitionMap.drop_node(tab, "A1")

      assert {:ok, %PartitionOwners{regime: 1, replicas: ["A2"]}} =
               PartitionMap.owners(tab, "test", 0)
    end

    test "deletes the entry when the dropped node was the last replica", %{owners: tab} do
      :ok = PartitionMap.update(tab, "test", 0, 1, ["A1"])

      assert :ok = PartitionMap.drop_node(tab, "A1")
      assert {:error, :unknown_partition} = PartitionMap.owners(tab, "test", 0)
    end

    test "is a no-op for an unknown node", %{owners: tab} do
      :ok = PartitionMap.update(tab, "test", 0, 1, ["A1", "A2"])

      assert :ok = PartitionMap.drop_node(tab, "X9")

      assert {:ok, %PartitionOwners{regime: 1, replicas: ["A1", "A2"]}} =
               PartitionMap.owners(tab, "test", 0)
    end
  end

  describe "node generation table" do
    test "get returns :unknown_node before put", %{node_gens: tab} do
      assert {:error, :unknown_node} = PartitionMap.get_node_gen(tab, "A1")
    end

    test "put then get round-trips the value", %{node_gens: tab} do
      :ok = PartitionMap.put_node_gen(tab, "A1", 42)
      assert {:ok, 42} = PartitionMap.get_node_gen(tab, "A1")
    end

    test "put overwrites the previous value", %{node_gens: tab} do
      :ok = PartitionMap.put_node_gen(tab, "A1", 1)
      :ok = PartitionMap.put_node_gen(tab, "A1", 2)
      assert {:ok, 2} = PartitionMap.get_node_gen(tab, "A1")
    end

    test "delete removes the entry", %{node_gens: tab} do
      :ok = PartitionMap.put_node_gen(tab, "A1", 7)
      :ok = PartitionMap.delete_node_gen(tab, "A1")
      assert {:error, :unknown_node} = PartitionMap.get_node_gen(tab, "A1")
    end
  end

  describe "partition_count/0" do
    test "returns 4096" do
      assert PartitionMap.partition_count() == 4096
    end
  end

  defp safe_delete(tab) do
    if :ets.info(tab) != :undefined do
      :ets.delete(tab)
    end
  end
end
