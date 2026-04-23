defmodule Aerospike.Integration.UdfApplyTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Key

  @host "localhost"
  @port 3000
  @namespace "test"
  @container "aerospike1"
  @fixture Path.expand("../support/fixtures/test_udf.lua", __DIR__)
  @server_name "spike_test_udf.lua"
  @package "spike_test_udf"

  setup_all do
    probe_aerospike!(@host, @port)
    register_fixture_udf!()

    on_exit(fn ->
      remove_fixture_udf()
    end)

    :ok
  end

  setup do
    name = :"spike_udf_apply_cluster_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["#{@host}:#{@port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 2
      )

    :ok = Tender.tend_now(name)

    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    %{cluster: name}
  end

  test "apply_udf returns the UDF value and surfaces runtime failures as typed errors", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    user_key = "spike_udf_apply_#{System.unique_integer([:positive])}"
    key = Key.new(@namespace, "spike", user_key)

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

    wait_for_udf!()
  end

  defp remove_fixture_udf do
    _ = docker(["exec", @container, "asinfo", "-v", "udf-remove:filename=#{@server_name};"])
    :ok
  end

  defp wait_for_udf!(timeout_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_udf(deadline)
  end

  defp do_wait_for_udf(deadline) do
    case docker(["exec", @container, "asinfo", "-v", "udf-list"]) do
      {output, 0} ->
        if output =~ @server_name do
          :ok
        else
          retry_wait_for_udf(deadline)
        end

      {_output, _status} ->
        retry_wait_for_udf(deadline)
    end
  end

  defp retry_wait_for_udf(deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      raise "Timed out waiting for #{@server_name} to appear in udf-list"
    end

    Process.sleep(100)
    do_wait_for_udf(deadline)
  end

  defp docker(args) do
    System.cmd("docker", args, stderr_to_stdout: true)
  end

  defp probe_aerospike!(host, port) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 1_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, reason} ->
        raise "Aerospike not reachable at #{host}:#{port} (#{inspect(reason)}). " <>
                "Run `docker compose up -d` in `aerospike_driver_spike/` first."
    end
  end
end
