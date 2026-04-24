defmodule Aerospike.Integration.WriteFamilyTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Record
  alias Aerospike.Test.IntegrationSupport

  @host "localhost"
  @port 3000
  @namespace "test"
  @set "spike"

  setup do
    IntegrationSupport.probe_aerospike!(@host, @port)
    IntegrationSupport.wait_for_seed_ready!(@host, @port, @namespace, 5_000)
    name = IntegrationSupport.unique_atom("spike_write_family_cluster")

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

  test "put, exists, get, operate, and delete succeed through the full stack", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    user_key = IntegrationSupport.unique_name("spike_write_family")
    key = Key.new(@namespace, @set, user_key)

    assert {:ok, %{generation: generation}} =
             Aerospike.put(cluster, key, %{"count" => 1})

    assert generation >= 1

    assert {:ok, true} = Aerospike.exists(cluster, key)

    assert {:ok, %Record{bins: %{"count" => 1}}} = Aerospike.get(cluster, key)

    assert {:ok, %Record{bins: %{"count" => 2}}} =
             Aerospike.operate(cluster, key, [{:write, "count", 2}, {:read, "count"}])

    assert {:ok, %Record{bins: %{"count" => 2}}} = Aerospike.get(cluster, key)

    mutation_key = Key.new(@namespace, @set, "#{user_key}-mutation")

    assert {:ok, %{generation: mutation_generation}} =
             Aerospike.put(cluster, mutation_key, %{"count" => 1})

    assert {:ok, %{generation: add_generation}} =
             Aerospike.add(cluster, mutation_key, %{count: 3})

    assert add_generation > mutation_generation

    assert {:ok, %Record{bins: %{"count" => 4}}} = Aerospike.get(cluster, mutation_key)

    string_key = Key.new(@namespace, @set, "#{user_key}-string")

    assert {:ok, %{generation: string_generation}} =
             Aerospike.put(cluster, string_key, %{"greeting" => "world"})

    assert {:ok, %{generation: append_generation}} =
             Aerospike.append(cluster, string_key, %{greeting: "!"})

    assert append_generation > string_generation

    assert {:ok, %{generation: prepend_generation}} =
             Aerospike.prepend(cluster, string_key, %{greeting: "hello "})

    assert prepend_generation > append_generation

    assert {:ok, %Record{bins: %{"greeting" => "hello world!"}}} =
             Aerospike.get(cluster, string_key)

    assert {:ok, %Record{bins: %{"count" => 7}, generation: operate_generation, ttl: ttl}} =
             Aerospike.operate(
               cluster,
               mutation_key,
               [
                 {:add, "count", 1},
                 {:add, "count", 2},
                 {:read, "count"}
               ],
               ttl: 120
             )

    assert operate_generation > mutation_generation
    assert ttl > 0

    assert operate_generation > add_generation
    assert {:ok, %Record{bins: %{"count" => 7}}} = Aerospike.get(cluster, mutation_key)

    assert {:ok, true} = Aerospike.delete(cluster, key)
    assert {:ok, false} = Aerospike.exists(cluster, key)
    assert {:error, %Error{code: :key_not_found}} = Aerospike.get(cluster, key)
  end
end
