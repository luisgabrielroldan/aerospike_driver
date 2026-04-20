defmodule Aerospike.Integration.GetPoolTest do
  @moduledoc """
  Pool-path integration tests for `Aerospike.get/3`.

  These tests exercise the `NodePool` checkout path end-to-end against a
  real Aerospike server: a serial batch that reuses the same pool, and a
  concurrent batch that forces checkout queueing with a small pool size.

  The deep unit-level assertion that "only one connection is opened for N
  serial GETs" is covered by `Aerospike.NodePoolTest` against
  `Transport.Fake`, which counts `connect/3` invocations directly. These
  integration tests assert only the user-visible outcome (every call
  returns `{:error, :key_not_found}` for a missing key) so that adding
  library-level instrumentation is not required.
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Tender

  @host "localhost"
  @port 3000
  @namespace "test"
  @set "spike_pool"
  @concurrent_tasks 20
  @concurrent_pool_size 4
  @serial_gets 25

  setup context do
    probe_aerospike!(@host, @port)
    pool_size = Map.get(context, :pool_size, @concurrent_pool_size)
    name = :"spike_get_pool_cluster_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        seeds: [{@host, @port}],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: pool_size
      )

    :ok = Tender.tend_now(name)

    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    %{cluster: name, pool_size: pool_size}
  end

  @tag pool_size: 1
  test "serial GETs for missing keys all return :key_not_found through one pool", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster)

    for i <- 1..@serial_gets do
      key = Key.new(@namespace, @set, "missing_serial_#{i}_#{System.unique_integer([:positive])}")
      assert {:error, %Error{code: :key_not_found}} = Aerospike.get(cluster, key)
    end
  end

  test "concurrent GETs with a small pool size all return :key_not_found", %{cluster: cluster} do
    assert Tender.ready?(cluster)

    tasks =
      for i <- 1..@concurrent_tasks do
        Task.async(fn ->
          key =
            Key.new(
              @namespace,
              @set,
              "missing_concurrent_#{i}_#{System.unique_integer([:positive])}"
            )

          Aerospike.get(cluster, key)
        end)
      end

    results = Task.await_many(tasks, 10_000)

    assert length(results) == @concurrent_tasks

    Enum.each(results, fn result ->
      assert {:error, %Error{code: :key_not_found}} = result
    end)
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
