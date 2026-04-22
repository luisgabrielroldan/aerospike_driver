defmodule Aerospike.BatchRouterTest do
  use ExUnit.Case, async: true

  alias Aerospike.BatchRouter
  alias Aerospike.Key
  alias Aerospike.PartitionMap

  setup context do
    prefix = :"batch_router_#{:erlang.phash2(context.test)}"
    {owners, node_gens} = PartitionMap.create_tables(prefix)
    meta = :"#{prefix}_meta"
    :ets.new(meta, [:set, :public, :named_table, read_concurrency: true])
    :ets.insert(meta, {:ready, false})

    tables = %{owners: owners, node_gens: node_gens, meta: meta}

    on_exit(fn ->
      for tab <- [owners, node_gens, meta] do
        delete_table_if_present(tab)
      end
    end)

    %{tables: tables}
  end

  describe "group_keys/3" do
    test "groups keys by node and preserves original indices for merge order", %{tables: tables} do
      set_ready(tables, true)

      key_a = key_for_partition("test", "batch", 100, "a")
      key_b = key_for_partition("test", "batch", 101, "b")
      key_c = key_for_partition("test", "batch", 100, "c")

      seed_partition(tables, key_a, ["A1", "B1"])
      seed_partition(tables, key_b, ["B1", "A1"])

      assert {:ok, grouping} =
               BatchRouter.group_keys(tables, [key_a, key_b, key_c],
                 dispatch: {:read, :master, 0},
                 payload_fun: fn key, index -> {key.user_key, index} end
               )

      assert Enum.map(grouping.node_requests, & &1.node_name) == ["A1", "B1"]

      assert Enum.map(grouping.node_requests, fn request ->
               {request.node_name, Enum.map(request.entries, &{&1.index, &1.payload})}
             end) == [
               {"A1", [{0, {key_a.user_key, 0}}, {2, {key_c.user_key, 2}}]},
               {"B1", [{1, {key_b.user_key, 1}}]}
             ]

      assert grouping.routing_failures == []
    end

    test "coalesces same-node writes into one node request", %{tables: tables} do
      set_ready(tables, true)

      key_a = key_for_partition("test", "batch", 200, "a")
      key_b = key_for_partition("test", "batch", 200, "b")

      seed_partition(tables, key_a, ["A1", "B1"])

      assert {:ok, grouping} =
               BatchRouter.group_keys(tables, [key_a, key_b], dispatch: :write)

      assert [
               %{node_name: "A1", entries: [%{index: 0}, %{index: 1}]}
             ] = grouping.node_requests

      assert grouping.routing_failures == []
    end

    test "returns indexed routing failures for partitions with no master", %{tables: tables} do
      set_ready(tables, true)

      key_a = key_for_partition("test", "batch", 300, "a")
      key_b = key_for_partition("test", "batch", 301, "b")
      key_c = key_for_partition("test", "batch", 302, "c")

      seed_partition(tables, key_a, ["A1", "B1"])
      seed_partition(tables, key_b, [nil, "B1"])
      seed_partition(tables, key_c, ["C1", "D1"])

      assert {:ok, grouping} =
               BatchRouter.group_keys(tables, [key_a, key_b, key_c], dispatch: :write)

      assert Enum.map(grouping.node_requests, fn request ->
               {request.node_name, Enum.map(request.entries, fn entry -> entry.index end)}
             end) == [
               {"A1", [0]},
               {"C1", [2]}
             ]

      assert [%{entry: %{index: 1, key: ^key_b}, reason: :no_master}] = grouping.routing_failures
    end

    test "aborts when the published cluster view is not ready", %{tables: tables} do
      key = key_for_partition("test", "batch", 400, "a")
      seed_partition(tables, key, ["A1"])

      assert {:error, :cluster_not_ready} =
               BatchRouter.group_keys(tables, [key], dispatch: {:read, :master, 0})
    end
  end

  defp set_ready(%{meta: meta}, value), do: :ets.insert(meta, {:ready, value})

  defp seed_partition(%{owners: owners}, key, replicas, regime \\ 1) do
    :ok = PartitionMap.update(owners, key.namespace, Key.partition_id(key), regime, replicas)
  end

  defp key_for_partition(namespace, set, partition_id, seed) do
    Stream.iterate(0, &(&1 + 1))
    |> Stream.map(&Key.new(namespace, set, "#{seed}-#{&1}"))
    |> Enum.find(&(Key.partition_id(&1) == partition_id))
  end

  defp delete_table_if_present(tab) do
    try do
      :ets.delete(tab)
    catch
      :error, :badarg -> :ok
    end
  end
end
