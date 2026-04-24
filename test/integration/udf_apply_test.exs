defmodule Aerospike.Integration.UdfApplyTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Test.IntegrationSupport

  @host "localhost"
  @port 3000
  @namespace "test"
  @container "aerospike1"
  @fixture Path.expand("../support/fixtures/test_udf.lua", __DIR__)
  @server_name "spike_test_udf.lua"
  @package "spike_test_udf"

  setup_all do
    IntegrationSupport.probe_aerospike!(
      @host,
      @port,
      "Run `docker compose up -d` in `aerospike_driver_spike/` first."
    )

    register_fixture_udf!()

    on_exit(fn ->
      remove_fixture_udf()
    end)

    :ok
  end

  setup do
    name = IntegrationSupport.unique_atom("spike_udf_apply_cluster")

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

  test "apply_udf returns the UDF value and surfaces runtime failures as typed errors", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    key = IntegrationSupport.unique_key(@namespace, "spike", "spike_udf_apply")

    assert {:ok, %{generation: generation}} = Aerospike.put(cluster, key, %{"seed" => 1})
    assert generation >= 1

    assert {:ok, "hello"} = Aerospike.apply_udf(cluster, key, @package, "echo", ["hello"])

    assert {:error, %Error{code: :udf_bad_response, message: message}} =
             Aerospike.apply_udf(cluster, key, @package, "explode", [])

    assert message =~ "boom"
  end

  defp register_fixture_udf! do
    remove_fixture_udf()

    encoded = @fixture |> File.read!() |> Base.encode64()

    command =
      "udf-put:filename=#{@server_name};content=#{encoded};content-len=#{byte_size(encoded)};udf-type=LUA;"

    case docker(["exec", @container, "asinfo", "-v", command]) do
      {output, 0} ->
        if String.contains?(String.downcase(output), "error") do
          raise "Failed to register #{@server_name}: #{output}"
        end

      {output, status} ->
        raise "Failed to register #{@server_name} (status #{status}): #{output}"
    end

    IntegrationSupport.assert_eventually(
      "registered udf appears in udf-list",
      fn ->
        case docker(["exec", @container, "asinfo", "-v", "udf-list"]) do
          {output, 0} -> output =~ @server_name
          _ -> false
        end
      end,
      5_000,
      100
    )
  end

  defp remove_fixture_udf do
    _ = docker(["exec", @container, "asinfo", "-v", "udf-remove:filename=#{@server_name};"])
    :ok
  end

  defp docker(args) do
    System.cmd("docker", args, stderr_to_stdout: true)
  end
end
