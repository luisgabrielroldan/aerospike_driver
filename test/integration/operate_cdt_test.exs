defmodule Aerospike.Integration.OperateCdtTest do
  use ExUnit.Case, async: false

  @moduletag :integration

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
  end
end
