defmodule Aerospike.Integration.GetTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Tender

  @host "localhost"
  @port 3000
  @namespace "test"

  setup do
    probe_aerospike!(@host, @port)
    name = :"spike_get_cluster_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        seeds: [{@host, @port}],
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

  test "GET for a known-missing key returns :key_not_found through the full stack", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    missing_user_key = "spike_missing_#{System.unique_integer([:positive])}"
    key = Key.new(@namespace, "spike", missing_user_key)

    assert {:error, %Error{code: :key_not_found}} = Aerospike.get(cluster, key)
  end

  defp probe_aerospike!(host, port) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 1_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, reason} ->
        raise "Aerospike not reachable at #{host}:#{port} (#{inspect(reason)}). " <>
                "Run `docker compose up -d` first."
    end
  end
end
