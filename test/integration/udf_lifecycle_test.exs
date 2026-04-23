defmodule Aerospike.Integration.UdfLifecycleTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Cluster.Tender
  alias Aerospike.RegisterTask
  alias Aerospike.UDF

  @host "localhost"
  @port 3000
  @namespace "test"
  @fixture Path.expand("../support/fixtures/test_udf.lua", __DIR__)

  setup_all do
    probe_aerospike!(@host, @port)
    :ok
  end

  setup do
    name = :"spike_udf_lifecycle_cluster_#{System.unique_integer([:positive])}"

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

  test "register_udf, list_udfs, and remove_udf expose the package lifecycle", %{cluster: cluster} do
    server_name = "spike_udf_lifecycle_#{System.unique_integer([:positive])}.lua"

    on_exit(fn ->
      if Process.whereis(cluster) do
        _ = Aerospike.remove_udf(cluster, server_name)
      end
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

    assert_eventually(
      fn ->
        case Aerospike.list_udfs(cluster) do
          {:ok, remaining} ->
            removed? = Enum.all?(remaining, &(&1.filename != server_name))
            {removed?, "UDF #{inspect(server_name)} still present: #{inspect(remaining)}"}

          {:error, error} ->
            {false, "list_udfs failed after remove: #{inspect(error)}"}
        end
      end,
      timeout: 10_000,
      interval: 200
    )
  end

  defp assert_eventually(fun, opts) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.fetch!(opts, :timeout)
    interval = Keyword.get(opts, :interval, 200)
    deadline = System.monotonic_time(:millisecond) + timeout
    assert_eventually_loop(fun, deadline, interval, nil)
  end

  defp assert_eventually_loop(fun, deadline, interval, last_message) do
    case fun.() do
      true ->
        :ok

      {true, _message} ->
        :ok

      false ->
        await_or_flunk(fun, deadline, interval, last_message || "condition returned false")

      {false, message} when is_binary(message) ->
        await_or_flunk(fun, deadline, interval, message)
    end
  end

  defp await_or_flunk(fun, deadline, interval, message) do
    if System.monotonic_time(:millisecond) > deadline do
      flunk("condition did not become true within timeout: #{message}")
    else
      Process.sleep(interval)
      assert_eventually_loop(fun, deadline, interval, message)
    end
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
