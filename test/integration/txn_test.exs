defmodule Aerospike.Integration.TxnTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :enterprise

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Tender
  alias Aerospike.Txn
  alias Aerospike.TxnOps

  @host System.get_env("AEROSPIKE_EE_HOST", "127.0.0.1")
  @port System.get_env("AEROSPIKE_EE_PORT", "3100") |> String.to_integer()
  @namespace "test"
  @set "spike"

  setup do
    probe_aerospike!(@host, @port)
    name = :"spike_txn_cluster_#{System.unique_integer([:positive])}"

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
    assert Tender.ready?(name), "Tender must be ready after one manual tend cycle"

    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    %{cluster: name}
  end

  test "transaction/3 commits explicit transactions and cleans up tracking", %{cluster: cluster} do
    key1 = unique_key("txn")
    key2 = unique_key("txn")
    txn = Txn.new(timeout: 5_000)

    assert {:ok, :committed} =
             Aerospike.transaction(cluster, txn, fn tx ->
               assert {:ok, %{generation: generation1}} =
                        Aerospike.put(cluster, key1, %{"x" => 1}, txn: tx)

               assert generation1 >= 1

               assert {:ok, %{generation: generation2}} =
                        Aerospike.put(cluster, key2, %{"y" => 2}, txn: tx)

               assert generation2 >= 1
               :committed
             end)

    assert {:error, :not_found} = TxnOps.get_tracking(cluster, txn)
    assert {:ok, _} = Aerospike.get(cluster, key1)
    assert {:ok, _} = Aerospike.get(cluster, key2)
  end

  test "transaction/2 aborts on Aerospike.Error and does not persist writes", %{cluster: cluster} do
    key = unique_key("txn")

    result =
      Aerospike.transaction(cluster, fn tx ->
        assert {:ok, %{generation: generation}} =
                 Aerospike.put(cluster, key, %{"x" => 1}, txn: tx)

        assert generation >= 1
        raise Error.from_result_code(:parameter_error, message: "intentional abort")
      end)

    assert {:error, %Error{code: :parameter_error}} = result
    assert {:error, %Error{code: :key_not_found}} = Aerospike.get(cluster, key)
  end

  test "manual abort cleans up an empty transaction", %{cluster: cluster} do
    txn = Txn.new()
    TxnOps.init_tracking(cluster, txn)

    assert {:ok, :aborted} = Aerospike.abort(cluster, txn)
    assert {:error, :not_found} = TxnOps.get_tracking(cluster, txn)
  end

  defp unique_key(prefix) do
    Key.new(@namespace, @set, "#{prefix}_#{System.unique_integer([:positive])}")
  end

  defp probe_aerospike!(host, port) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 1_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, reason} ->
        raise "Aerospike not reachable at #{host}:#{port} (#{inspect(reason)}). Run `docker compose up -d` first."
    end
  end
end
