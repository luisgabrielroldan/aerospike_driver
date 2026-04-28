defmodule Aerospike.Integration.OperateCdtTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Ctx
  alias Aerospike.Op.Bit, as: BitOp
  alias Aerospike.Op.HLL, as: HLLOp
  alias Aerospike.Op.List, as: ListOp
  alias Aerospike.Op.Map, as: MapOp
  alias Aerospike.Record
  alias Aerospike.Test.IntegrationSupport

  @host "localhost"
  @port 3000
  @namespace "test"
  @set "spike"

  setup do
    IntegrationSupport.probe_aerospike!(@host, @port)
    IntegrationSupport.wait_for_seed_ready!(@host, @port, @namespace, 5_000)
    name = IntegrationSupport.unique_atom("spike_operate_cdt")

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["#{@host}:#{@port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 2
      )

    IntegrationSupport.wait_for_tender_ready!(name, 5_000)

    on_exit(fn ->
      IntegrationSupport.stop_supervisor_quietly(sup)
    end)

    %{cluster: name}
  end

  test "list and map CDT helpers read and modify records on the shared unary path", %{
    cluster: cluster
  } do
    key = IntegrationSupport.unique_key(@namespace, @set, "operate-cdt")

    assert {:ok, _} = Aerospike.put(cluster, key, %{"seed" => 0})

    assert {:ok, %Record{}} =
             Aerospike.operate(cluster, key, [ListOp.append("tags", "trial")])

    assert {:ok, %Record{bins: %{"tags" => 1}}} =
             Aerospike.operate(cluster, key, [ListOp.size("tags")])

    assert {:ok, %Record{}} =
             Aerospike.operate(cluster, key, [MapOp.put_items("prefs", %{"theme" => "dark"})])

    assert {:ok, %Record{bins: %{"prefs" => "dark"}}} =
             Aerospike.operate(cluster, key, [MapOp.get_by_key("prefs", "theme")])

    assert {:ok, %Record{}} =
             Aerospike.operate(cluster, key, [
               MapOp.put_items("scores", %{"ada" => 10, "grace" => 20},
                 policy: [order: :key_ordered]
               )
             ])

    assert {:ok, %Record{bins: %{"scores" => 20}}} =
             Aerospike.operate(cluster, key, [
               MapOp.get_by_rank("scores", -1, return_type: MapOp.return_value())
             ])
  end

  test "bit helpers modify and read byte-array bins on the shared unary path", %{
    cluster: cluster
  } do
    key = IntegrationSupport.unique_key(@namespace, @set, "operate-bit")

    assert {:ok, _} = Aerospike.put(cluster, key, %{"bits" => {:blob, <<0>>}})

    try do
      assert {:ok, %Record{}} =
               Aerospike.operate(cluster, key, [
                 BitOp.set("bits", 0, 8, <<0b1010_0000>>)
               ])

      assert {:ok, %Record{bins: %{"bits" => 2}}} =
               Aerospike.operate(cluster, key, [
                 BitOp.count("bits", 0, 8)
               ])

      assert {:ok, %Record{bins: %{"bits" => 0b1010_0000}}} =
               Aerospike.operate(cluster, key, [
                 BitOp.get_int("bits", 0, 8)
               ])
    after
      _ = Aerospike.delete(cluster, key)
    end
  end

  test "HLL helpers modify and read cardinality on the shared unary path", %{
    cluster: cluster
  } do
    key = IntegrationSupport.unique_key(@namespace, @set, "operate-hll")

    assert {:ok, _} = Aerospike.put(cluster, key, %{"seed" => 0})

    try do
      assert {:ok, %Record{}} =
               Aerospike.operate(cluster, key, [
                 HLLOp.init("visitors", 14, 0)
               ])

      assert {:ok, %Record{bins: %{"visitors" => added}}} =
               Aerospike.operate(cluster, key, [
                 HLLOp.add("visitors", ["ada", "grace", "katherine"], 14, 0)
               ])

      assert is_integer(added)
      assert added >= 0

      assert {:ok, %Record{bins: %{"visitors" => count}}} =
               Aerospike.operate(cluster, key, [
                 HLLOp.get_count("visitors")
               ])

      assert count == 3

      assert {:ok, %Record{bins: %{"visitors" => description}}} =
               Aerospike.operate(cluster, key, [
                 HLLOp.describe("visitors")
               ])

      assert is_list(description)
      assert [index_bit_count, min_hash_bit_count] = description
      assert is_integer(index_bit_count)
      assert is_integer(min_hash_bit_count)
    after
      _ = Aerospike.delete(cluster, key)
    end
  end

  test "create-on-navigation context creates nested maps", %{cluster: cluster} do
    key = IntegrationSupport.unique_key(@namespace, @set, "operate-cdt-create-ctx")

    assert {:ok, _} = Aerospike.put(cluster, key, %{"profile" => %{}})

    try do
      assert {:ok, %Record{}} =
               Aerospike.operate(cluster, key, [
                 MapOp.put("profile", "theme", "dark",
                   ctx: [Ctx.map_key_create("settings", :key_ordered)]
                 )
               ])

      assert {:ok, %Record{bins: %{"profile" => %{"settings" => %{"theme" => "dark"}}}}} =
               Aerospike.get(cluster, key)
    after
      _ = Aerospike.delete(cluster, key)
    end
  end

  test "additional list and map CDT variants execute against the server", %{cluster: cluster} do
    key = IntegrationSupport.unique_key(@namespace, @set, "operate-cdt-variants")

    assert {:ok, _} =
             Aerospike.put(cluster, key, %{
               "nums" => [1, 2, 3, 4],
               "scores" => %{"a" => 10, "b" => 20, "c" => 30}
             })

    try do
      assert {:ok, %Record{bins: %{"nums" => [3, 4]}}} =
               Aerospike.operate(cluster, key, [
                 ListOp.pop_range_from("nums", 2)
               ])

      assert {:ok, %Record{bins: %{"scores" => ["b", "c"]}}} =
               Aerospike.operate(cluster, key, [
                 MapOp.get_by_key_rel_index_range_count("scores", "b", 0, 2,
                   return_type: MapOp.return_key()
                 )
               ])
    after
      _ = Aerospike.delete(cluster, key)
    end
  end
end
