defmodule Aerospike.Integration.QueryAggregateResultTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Error
  alias Aerospike.Filter
  alias Aerospike.IndexTask
  alias Aerospike.Query
  alias Aerospike.RegisterTask
  alias Aerospike.Test.IntegrationSupport

  @host "localhost"
  @port 3000
  @namespace "test"
  @fixture Path.expand("../support/fixtures/aggregate_udf.lua", __DIR__)

  setup do
    IntegrationSupport.probe_aerospike!(
      @host,
      @port,
      "Run `docker compose up -d` in `aerospike_driver/` first."
    )

    name = IntegrationSupport.unique_atom("spike_query_aggregate_result")
    set = IntegrationSupport.unique_name("agg_result")
    index_name = IntegrationSupport.unique_name("agg_result_age_idx")
    package = IntegrationSupport.unique_name("agg_result_udf")
    server_name = "#{package}.lua"

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

    assert {:ok, register_task} = Aerospike.register_udf(name, @fixture, server_name)
    assert :ok = RegisterTask.wait(register_task, timeout: 10_000, poll_interval: 200)

    assert {:ok, index_task} =
             Aerospike.create_index(name, @namespace, set,
               bin: "age",
               name: index_name,
               type: :numeric
             )

    assert :ok = IndexTask.wait(index_task, timeout: 30_000, poll_interval: 200)

    on_exit(fn ->
      if Process.whereis(name) do
        cleanup_cluster(name, set, index_name, server_name)
      end

      IntegrationSupport.stop_supervisor_quietly(sup)
    end)

    %{cluster: name, set: set, package: package}
  end

  test "returns one finalized value while keeping aggregate partials available", %{
    cluster: cluster,
    set: set,
    package: package
  } do
    seed_ages(cluster, set, 20..24)
    query = age_query(set, 20, 24)

    IntegrationSupport.assert_eventually("finalized sum reaches seeded records", fn ->
      match?(
        {:ok, 110},
        Aerospike.query_aggregate_result(cluster, query, package, "sum_age", ["age"],
          source_path: @fixture,
          timeout: 10_000
        )
      )
    end)

    assert {:ok, partial_stream} =
             Aerospike.query_aggregate(cluster, query, package, "sum_age", ["age"],
               timeout: 10_000
             )

    partials = Enum.to_list(partial_stream)
    assert partials != []
    assert Enum.all?(partials, &is_integer/1)
    assert Enum.sum(partials) == 110
  end

  test "returns finalized map and list values", %{cluster: cluster, set: set, package: package} do
    seed_ages(cluster, set, 31..33)
    query = age_query(set, 31, 33)

    result =
      IntegrationSupport.eventually!("finalized stats reaches seeded records", fn ->
        case Aerospike.query_aggregate_result(cluster, query, package, "sum_summary", [],
               source_path: @fixture,
               timeout: 10_000
             ) do
          {:ok, %{"labels" => labels, "sum" => 96} = result}
          when is_list(labels) ->
            {:ok, result}

          {:ok, _other} ->
            :retry

          {:error, _error} ->
            :retry
        end
      end)

    assert result["labels"] == ["total", "96"]
  end

  test "returns nil when the server aggregate stream is empty", %{
    cluster: cluster,
    set: set,
    package: package
  } do
    query = age_query(set, 9_000, 9_001)

    assert {:ok, nil} =
             Aerospike.query_aggregate_result(cluster, query, package, "ages", [],
               source_path: @fixture,
               timeout: 10_000
             )
  end

  test "rejects local source and helper failures explicitly", %{
    cluster: cluster,
    set: set,
    package: package
  } do
    query = age_query(set, 1, 2)

    assert {:error, %Error{code: :invalid_argument}} =
             Aerospike.query_aggregate_result(cluster, query, package, "sum_age", ["age"],
               timeout: 10_000
             )

    assert {:error, %Error{code: :query_generic}} =
             Aerospike.query_aggregate_result(cluster, query, package, "sum_age", ["age"],
               source: "function broken(",
               timeout: 10_000
             )

    assert {:error, %Error{code: :query_generic}} =
             Aerospike.query_aggregate_result(
               cluster,
               query,
               package,
               "unsupported_client_helper",
               [],
               source_path: @fixture,
               timeout: 10_000
             )
  end

  defp seed_ages(cluster, set, ages) do
    Enum.each(ages, fn age ->
      key = IntegrationSupport.unique_key(@namespace, set, "age_#{age}")

      assert {:ok, _metadata} =
               Aerospike.put(cluster, key, %{"age" => age, "label" => "age-#{age}"})
    end)
  end

  defp age_query(set, min, max) do
    Query.new(@namespace, set)
    |> Query.where(Filter.range("age", min, max))
  end

  defp cleanup_cluster(cluster, set, index_name, server_name) do
    safe_cleanup(fn -> Aerospike.truncate(cluster, @namespace, set) end)
    safe_cleanup(fn -> Aerospike.drop_index(cluster, @namespace, index_name) end)
    safe_cleanup(fn -> Aerospike.remove_udf(cluster, server_name) end)
  end

  defp safe_cleanup(fun) when is_function(fun, 0) do
    _ = fun.()
    :ok
  catch
    :exit, _reason -> :ok
  end
end
