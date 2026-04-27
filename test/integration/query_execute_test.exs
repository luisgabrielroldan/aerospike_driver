defmodule Aerospike.Integration.QueryExecuteTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.ExecuteTask
  alias Aerospike.Filter
  alias Aerospike.IndexTask
  alias Aerospike.Query
  alias Aerospike.Test.IntegrationSupport

  @host "localhost"
  @port 3_000
  @namespace "test"

  setup do
    IntegrationSupport.probe_aerospike!(@host, @port)
    IntegrationSupport.wait_for_seed_ready!(@host, @port, @namespace, 15_000)

    name = IntegrationSupport.unique_atom("spike_query_execute")

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

    on_exit(fn -> IntegrationSupport.stop_supervisor_quietly(sup) end)

    %{cluster: name}
  end

  test "query_execute returns a task handle and applies simple write operations", %{
    cluster: cluster
  } do
    set = IntegrationSupport.unique_name("query_execute")
    index_name = IntegrationSupport.unique_name("query_execute_score_idx")
    keys = for i <- 1..3, do: Aerospike.key(@namespace, set, "query_execute_#{i}")

    try do
      assert {:ok, index_task} =
               Aerospike.create_index(cluster, @namespace, set,
                 bin: "score",
                 name: index_name,
                 type: :numeric
               )

      assert :ok = IndexTask.wait(index_task, timeout: 30_000, poll_interval: 200)

      for {key, score} <- Enum.zip(keys, 1..3) do
        assert {:ok, _metadata} =
                 Aerospike.put(cluster, key, %{"score" => score, "marker" => "pending"})
      end

      query =
        Query.new(@namespace, set)
        |> Query.where(Filter.range("score", 1, 3))

      assert {:ok, %ExecuteTask{} = task} =
               Aerospike.query_execute(cluster, query, [{:write, :marker, "done"}],
                 timeout: 5_000,
                 task_timeout: 10_000
               )

      assert :ok = ExecuteTask.wait(task, timeout: 15_000, poll_interval: 200)

      IntegrationSupport.assert_eventually("background query marks all records", fn ->
        keys
        |> Enum.map(fn key ->
          assert {:ok, record} = Aerospike.get(cluster, key)
          record.bins["marker"]
        end)
        |> Enum.all?(&(&1 == "done"))
      end)
    after
      _ = Aerospike.drop_index(cluster, @namespace, index_name)
      Enum.each(keys, fn key -> _ = Aerospike.delete(cluster, key) end)
    end
  end
end
