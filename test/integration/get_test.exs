defmodule Aerospike.Integration.GetTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Test.IntegrationSupport

  @host "localhost"
  @port 3000
  @namespace "test"

  setup do
    IntegrationSupport.probe_aerospike!(@host, @port)
    IntegrationSupport.wait_for_seed_ready!(@host, @port, @namespace, 5_000)
    name = IntegrationSupport.unique_atom("spike_get_cluster")

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
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    %{cluster: name}
  end

  test "GET for a known-missing key returns :key_not_found through the full stack", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    key = IntegrationSupport.unique_key(@namespace, "spike", "spike_missing")

    assert {:error, %Error{code: :key_not_found}} = Aerospike.get(cluster, key)
  end

  test "GET_HEADER returns metadata with empty bins for an existing record", %{cluster: cluster} do
    key = IntegrationSupport.unique_key(@namespace, "spike", "spike_header")

    assert {:ok, %{generation: 1}} = Aerospike.put(cluster, key, %{"name" => "header-only"})

    assert {:ok, %Aerospike.Record{key: ^key, generation: generation, ttl: ttl, bins: %{}}} =
             Aerospike.get_header(cluster, key)

    assert generation >= 1
    assert ttl >= 0
  end

  test "GET projects a subset of named bins for an existing record", %{cluster: cluster} do
    key = IntegrationSupport.unique_key(@namespace, "spike", "spike_named_bins")

    assert {:ok, %{generation: 1}} =
             Aerospike.put(cluster, key, %{
               "name" => "Ada",
               "score" => 42,
               "ignored" => "not returned"
             })

    assert {:ok, %Aerospike.Record{key: ^key, bins: bins}} =
             Aerospike.get(cluster, key, ["name", :score])

    assert bins == %{"name" => "Ada", "score" => 42}
  end
end
