defmodule Aerospike.Integration.UdfLifecycleTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.RegisterTask
  alias Aerospike.Test.IntegrationSupport
  alias Aerospike.UDF

  @host "localhost"
  @port 3000
  @namespace "test"
  @fixture Path.expand("../support/fixtures/test_udf.lua", __DIR__)

  setup_all do
    IntegrationSupport.probe_aerospike!(
      @host,
      @port,
      "Run `docker compose up -d` in `aerospike_driver_spike/` first."
    )

    :ok
  end

  setup do
    name = IntegrationSupport.unique_atom("spike_udf_lifecycle_cluster")

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

  test "register_udf, list_udfs, and remove_udf expose the package lifecycle", %{cluster: cluster} do
    server_name = "#{IntegrationSupport.unique_name("spike_udf_lifecycle")}.lua"

    on_exit(fn ->
      cleanup_udf(server_name)
    end)

    assert {:ok, %RegisterTask{package_name: ^server_name} = task} =
             Aerospike.register_udf(cluster, @fixture, server_name)

    assert :ok = RegisterTask.wait(task, timeout: 10_000, poll_interval: 200)
    assert {:ok, udfs} = Aerospike.list_udfs(cluster)

    assert %UDF{filename: ^server_name, language: "LUA", hash: hash} =
             Enum.find(udfs, &(&1.filename == server_name))

    assert is_binary(hash)
    assert hash != ""

    assert :ok = Aerospike.remove_udf(cluster, server_name)

    IntegrationSupport.assert_eventually(
      "removed udf disappears from list_udfs",
      fn ->
        case Aerospike.list_udfs(cluster) do
          {:ok, remaining} ->
            Enum.all?(remaining, &(&1.filename != server_name))

          {:error, _error} ->
            false
        end
      end
    )
  end

  defp cleanup_udf(server_name) do
    name = IntegrationSupport.unique_atom("spike_udf_lifecycle_cleanup")

    case Aerospike.start_link(
           name: name,
           transport: Aerospike.Transport.Tcp,
           hosts: ["#{@host}:#{@port}"],
           namespaces: [@namespace],
           tend_trigger: :manual,
           pool_size: 1
         ) do
      {:ok, sup} ->
        try do
          IntegrationSupport.wait_for_tender_ready!(name, 5_000)
          _ = Aerospike.remove_udf(name, server_name)
          :ok
        after
          IntegrationSupport.stop_supervisor_quietly(sup)
        end

      {:error, _reason} ->
        :ok
    end
  catch
    :exit, _reason -> :ok
  end
end
