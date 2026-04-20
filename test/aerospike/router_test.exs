defmodule Aerospike.RouterTest do
  use ExUnit.Case, async: true

  alias Aerospike.Key
  alias Aerospike.PartitionMap
  alias Aerospike.Router

  setup context do
    prefix = :"router_#{:erlang.phash2(context.test)}"
    {owners, node_gens} = PartitionMap.create_tables(prefix)
    meta = :"#{prefix}_meta"
    :ets.new(meta, [:set, :public, :named_table, read_concurrency: true])
    :ets.insert(meta, {:ready, false})

    tables = %{owners: owners, node_gens: node_gens, meta: meta}

    on_exit(fn ->
      for tab <- [owners, node_gens, meta], :ets.info(tab) != :undefined do
        :ets.delete(tab)
      end
    end)

    %{tables: tables}
  end

  defp set_ready(%{meta: meta}, value), do: :ets.insert(meta, {:ready, value})

  defp seed_partition(%{owners: owners}, key, replicas, regime \\ 1) do
    :ok = PartitionMap.update(owners, key.namespace, Key.partition_id(key), regime, replicas)
  end

  describe "pick_for_write/2" do
    test "returns master for a ready cluster with a known partition", %{tables: tables} do
      set_ready(tables, true)
      key = Key.new("test", "s", 1)
      seed_partition(tables, key, ["A1", "B1"])

      assert {:ok, "A1"} = Router.pick_for_write(tables, key)
    end

    test "returns :cluster_not_ready while the ready flag is false", %{tables: tables} do
      key = Key.new("test", "s", 1)
      seed_partition(tables, key, ["A1"])

      assert {:error, :cluster_not_ready} = Router.pick_for_write(tables, key)
    end

    test "returns :no_master when the partition is unknown", %{tables: tables} do
      set_ready(tables, true)
      key = Key.new("test", "s", 1)

      assert {:error, :no_master} = Router.pick_for_write(tables, key)
    end

    test "returns :no_master when the master slot is nil", %{tables: tables} do
      set_ready(tables, true)
      key = Key.new("test", "s", 1)
      seed_partition(tables, key, [nil, "B1"])

      assert {:error, :no_master} = Router.pick_for_write(tables, key)
    end
  end

  describe "pick_for_read/4 with :master policy" do
    test "returns master regardless of attempt", %{tables: tables} do
      set_ready(tables, true)
      key = Key.new("test", "s", 1)
      seed_partition(tables, key, ["A1", "B1", "C1"])

      for attempt <- 0..5 do
        assert {:ok, "A1"} = Router.pick_for_read(tables, key, :master, attempt)
      end
    end

    test "returns :cluster_not_ready when not ready", %{tables: tables} do
      key = Key.new("test", "s", 1)
      seed_partition(tables, key, ["A1"])

      assert {:error, :cluster_not_ready} = Router.pick_for_read(tables, key, :master, 0)
    end

    test "returns :no_master when the master slot is nil", %{tables: tables} do
      set_ready(tables, true)
      key = Key.new("test", "s", 1)
      seed_partition(tables, key, [nil, "B1"])

      assert {:error, :no_master} = Router.pick_for_read(tables, key, :master, 0)
    end
  end

  describe "pick_for_read/4 with :sequence policy" do
    test "rotates through replicas across attempts", %{tables: tables} do
      set_ready(tables, true)
      key = Key.new("test", "s", 1)
      seed_partition(tables, key, ["A1", "B1", "C1"])

      assert {:ok, "A1"} = Router.pick_for_read(tables, key, :sequence, 0)
      assert {:ok, "B1"} = Router.pick_for_read(tables, key, :sequence, 1)
      assert {:ok, "C1"} = Router.pick_for_read(tables, key, :sequence, 2)
      assert {:ok, "A1"} = Router.pick_for_read(tables, key, :sequence, 3)
    end

    test "skips nil slots so attempt index walks live replicas only", %{tables: tables} do
      set_ready(tables, true)
      key = Key.new("test", "s", 1)
      seed_partition(tables, key, [nil, "B1", nil, "D1"])

      assert {:ok, "B1"} = Router.pick_for_read(tables, key, :sequence, 0)
      assert {:ok, "D1"} = Router.pick_for_read(tables, key, :sequence, 1)
      assert {:ok, "B1"} = Router.pick_for_read(tables, key, :sequence, 2)
    end

    test "returns :cluster_not_ready when not ready", %{tables: tables} do
      key = Key.new("test", "s", 1)
      seed_partition(tables, key, ["A1", "B1"])

      assert {:error, :cluster_not_ready} = Router.pick_for_read(tables, key, :sequence, 0)
    end

    test "returns :no_master when every replica slot is nil", %{tables: tables} do
      set_ready(tables, true)
      key = Key.new("test", "s", 1)
      seed_partition(tables, key, [nil, nil])

      assert {:error, :no_master} = Router.pick_for_read(tables, key, :sequence, 0)
    end

    test "returns :no_master when the partition is unknown", %{tables: tables} do
      set_ready(tables, true)
      key = Key.new("test", "s", 1)

      assert {:error, :no_master} = Router.pick_for_read(tables, key, :sequence, 0)
    end
  end
end
