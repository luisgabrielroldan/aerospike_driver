defmodule Aerospike.Integration.OperateCdtTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Key
  alias Aerospike.Op.List, as: ListOp
  alias Aerospike.Op.Map, as: MapOp
  alias Aerospike.Record
  alias Aerospike.Tender

  @host "localhost"
  @port 3000
  @namespace "test"
  @set "spike"

  setup do
    probe_aerospike!(@host, @port)
    name = :"spike_operate_cdt_#{System.unique_integer([:positive])}"

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

  test "list and map CDT helpers read and modify records on the shared unary path", %{
    cluster: cluster
  } do
    key = Key.new(@namespace, @set, "operate-cdt-#{System.unique_integer([:positive])}")

    assert {:ok, _} = Aerospike.put(cluster, key, %{"seed" => 0})

    assert {:ok, %Record{}} =
             Aerospike.operate(cluster, key, [ListOp.append("tags", "trial")])

    assert {:ok, %Record{bins: %{"tags" => 1}}} =
             Aerospike.operate(cluster, key, [ListOp.size("tags")])

    assert {:ok, %Record{}} =
             Aerospike.operate(cluster, key, [MapOp.put_items("prefs", %{"theme" => "dark"})])

    assert {:ok, %Record{bins: %{"prefs" => "dark"}}} =
             Aerospike.operate(cluster, key, [MapOp.get_by_key("prefs", "theme")])
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
