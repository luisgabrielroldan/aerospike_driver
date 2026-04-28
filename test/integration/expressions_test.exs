defmodule Aerospike.Integration.ExpressionsTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :ce

  alias Aerospike
  alias Aerospike.Error
  alias Aerospike.Exp
  alias Aerospike.Exp.List, as: ListExp
  alias Aerospike.Key
  alias Aerospike.Op
  alias Aerospike.Record
  alias Aerospike.Scan
  alias Aerospike.Test.IntegrationSupport

  @host "localhost"
  @port 3_000
  @namespace "test"

  setup do
    IntegrationSupport.probe_aerospike!(@host, @port)
    IntegrationSupport.wait_for_seed_ready!(@host, @port, @namespace, 5_000)

    name = IntegrationSupport.unique_atom("spike_expr_integration")

    {:ok, _sup} =
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
        Aerospike.close(name)
      catch
        :exit, _ -> :ok
      end
    end)

    %{cluster: name}
  end

  test "get with matching expression filter returns the record", %{cluster: cluster} do
    set = IntegrationSupport.unique_name("expr_get_match")
    key = IntegrationSupport.unique_key(@namespace, set, "record")

    assert {:ok, _} = Aerospike.put(cluster, key, %{"age" => 30})
    filter = Exp.gt(Exp.int_bin("age"), Exp.int(20))

    try do
      assert {:ok, record} = Aerospike.get(cluster, key, :all, filter: filter)
      assert record.bins["age"] == 30
    after
      _ = Aerospike.delete(cluster, key)
    end
  end

  test "get with non-matching expression filter returns :filtered_out", %{cluster: cluster} do
    set = IntegrationSupport.unique_name("expr_get_nomatch")
    key = IntegrationSupport.unique_key(@namespace, set, "record")

    assert {:ok, _} = Aerospike.put(cluster, key, %{"age" => 15})
    filter = Exp.gt(Exp.int_bin("age"), Exp.int(20))

    try do
      assert {:error, %Error{code: :filtered_out}} =
               Aerospike.get(cluster, key, :all, filter: filter)
    after
      _ = Aerospike.delete(cluster, key)
    end
  end

  test "scan with expression filter returns only matching records", %{cluster: cluster} do
    set = IntegrationSupport.unique_name("expr_scan")
    keys = seed_scan_records(cluster, set)

    scan =
      Scan.new(@namespace, set)
      |> Scan.filter(Exp.gt(Exp.int_bin("score"), Exp.int(30)))
      |> Scan.max_records(20)

    try do
      assert {:ok, records} = Aerospike.scan_all(cluster, scan)

      scores = Enum.map(records, & &1.bins["score"]) |> Enum.sort()
      assert scores == [40, 50, 60]
    after
      Enum.each(keys, &Aerospike.delete(cluster, &1))
    end
  end

  test "get with CDT expression filter evaluates list helpers", %{cluster: cluster} do
    set = IntegrationSupport.unique_name("expr_cdt_filter")
    key = IntegrationSupport.unique_key(@namespace, set, "record")

    assert {:ok, _} = Aerospike.put(cluster, key, %{"items" => [1, 2, 3]})
    filter = Exp.eq(ListExp.size(Exp.list_bin("items")), Exp.int(3))

    try do
      assert {:ok, record} = Aerospike.get(cluster, key, :all, filter: filter)
      assert record.bins["items"] == [1, 2, 3]
    after
      _ = Aerospike.delete(cluster, key)
    end
  end

  test "operate reads and writes expression values", %{cluster: cluster} do
    set = IntegrationSupport.unique_name("expr_operate")
    key = IntegrationSupport.unique_key(@namespace, set, "record")

    assert {:ok, _} = Aerospike.put(cluster, key, %{"count" => 42})

    try do
      assert {:ok, %Record{bins: %{"result" => 42}}} =
               Aerospike.operate(cluster, key, [
                 Op.Exp.read("result", Exp.int_bin("count"))
               ])

      assert {:ok, %Record{}} =
               Aerospike.operate(cluster, key, [
                 Op.Exp.write("computed", Exp.int(99))
               ])

      assert {:ok, %Record{bins: %{"computed" => 99, "count" => 42}}} =
               Aerospike.get(cluster, key)
    after
      _ = Aerospike.delete(cluster, key)
    end
  end

  defp seed_scan_records(cluster, set) when is_atom(cluster) do
    for value <- [10, 20, 30, 40, 50, 60] do
      key = Key.new(@namespace, set, "record-#{value}")
      assert {:ok, _} = Aerospike.put(cluster, key, %{"score" => value})
      key
    end
  end
end
