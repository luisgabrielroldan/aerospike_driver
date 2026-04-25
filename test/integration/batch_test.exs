defmodule Aerospike.Integration.BatchTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :cluster
  @moduletag capture_log: true

  alias Aerospike.Cluster.Router
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.Batch
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.OperateFlags
  alias Aerospike.Record
  alias Aerospike.Test.IntegrationSupport
  alias Aerospike.Transport.Tcp

  @seeds [{"localhost", 3000}, {"localhost", 3010}, {"localhost", 3020}]
  @namespace "test"
  @seed_containers %{
    3000 => "aerospike1",
    3010 => "aerospike2",
    3020 => "aerospike3"
  }

  setup do
    IntegrationSupport.probe_aerospike!(
      @seeds,
      "Run `docker compose --profile cluster up -d aerospike aerospike2 aerospike3` in `aerospike_driver_spike/` first."
    )

    IntegrationSupport.wait_for_cluster_ready!(@seeds, @namespace, 15_000)
    name = IntegrationSupport.unique_atom("spike_batch_cluster")

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Tcp,
        hosts: Enum.map(@seeds, fn {host, port} -> "#{host}:#{port}" end),
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 2
      )

    IntegrationSupport.wait_for_tender_ready!(name, 5_000)
    seed_nodes = seed_nodes!(@seeds)

    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end

      Enum.each(Map.values(@seed_containers), &docker_start/1)
      IntegrationSupport.wait_for_cluster_ready!(@seeds, @namespace, 15_000)
    end)

    %{cluster: name, seed_nodes: seed_nodes}
  end

  test "mixed batch preserves caller order and scopes a dead node to its own entry", %{
    cluster: cluster,
    seed_nodes: seed_nodes
  } do
    assert Tender.ready?(cluster), "Tender must be ready after one manual tend cycle"

    set = IntegrationSupport.unique_name("spike_batch_mixed")

    [{node_a, key_put}, {_node_b, key_operate}, {node_c, key_read}] =
      keys_for_distinct_nodes(cluster, set, 3)

    key_delete = key_for_node(cluster, set, node_a, "delete")
    stopped = stopped_seed!(seed_nodes, node_c)

    assert {:ok, %{generation: generation_operate}} =
             Aerospike.put(cluster, key_operate, %{"count" => 1})

    assert generation_operate >= 1
    assert {:ok, _} = Aerospike.put(cluster, key_delete, %{"gone" => true})
    assert {:ok, _} = Aerospike.put(cluster, key_read, %{"node" => node_c, "value" => 99})

    {:ok, add_op} = Operation.add("count", 2)
    read_op = Operation.read("count")
    operate_ops = [add_op, read_op]

    docker_stop(stopped.container)
    wait_for_tcp_down!(stopped.host, stopped.port, 5_000)

    entries = [
      %Entry{
        index: 0,
        key: key_operate,
        kind: :operate,
        dispatch: :write,
        payload: %{operations: operate_ops, flags: OperateFlags.scan_ops(operate_ops)}
      },
      %Entry{
        index: 1,
        key: key_read,
        kind: :read,
        dispatch: {:read, :master, 0},
        payload: nil
      },
      %Entry{
        index: 2,
        key: key_delete,
        kind: :delete,
        dispatch: :write,
        payload: nil
      },
      %Entry{
        index: 3,
        key: key_put,
        kind: :put,
        dispatch: :write,
        payload: %{bins: %{"created" => true}}
      }
    ]

    assert {:ok, [operate_result, read_result, delete_result, put_result]} =
             Batch.execute(cluster, entries, timeout: 10_000)

    assert %{
             index: 0,
             key: ^key_operate,
             kind: :operate,
             status: :ok,
             record: %Record{bins: %{"count" => 3}},
             error: nil
           } = operate_result

    assert %{index: 1, key: ^key_read, kind: :read, status: :error, record: nil} = read_result
    assert_transport_failure!(read_result.error)

    assert %{index: 2, key: ^key_delete, kind: :delete, status: :ok, record: nil, error: nil} =
             delete_result

    assert %{index: 3, key: ^key_put, kind: :put, status: :ok, record: nil, error: nil} =
             put_result

    assert {:ok, %Record{bins: %{"count" => 3}}} = Aerospike.get(cluster, key_operate)
    assert {:ok, false} = Aerospike.exists(cluster, key_delete)
    assert {:ok, true} = Aerospike.exists(cluster, key_put)
  end

  defp keys_for_distinct_nodes(cluster, set, count) when is_integer(count) and count > 0 do
    tables = Tender.tables(cluster)

    0..50_000
    |> Enum.reduce_while(%{}, fn suffix, acc ->
      key = Key.new(@namespace, set, "live-#{suffix}")

      case Router.pick_for_read(tables, key, :master, 0) do
        {:ok, node_name} ->
          node_keys = Map.put_new(acc, node_name, key)
          maybe_halt_distinct_node_search(node_keys, count)

        {:error, reason} ->
          flunk("expected a routed key while building mixed batch proof, got #{inspect(reason)}")
      end
    end)
    |> case do
      node_keys when map_size(node_keys) == count ->
        Enum.sort_by(node_keys, fn {node_name, _key} -> node_name end)

      node_keys ->
        flunk(
          "expected #{count} routed nodes from the live cluster, found #{map_size(node_keys)}"
        )
    end
  end

  defp maybe_halt_distinct_node_search(node_keys, count) do
    if map_size(node_keys) == count do
      {:halt, node_keys}
    else
      {:cont, node_keys}
    end
  end

  defp key_for_node(cluster, set, node_name, prefix) do
    tables = Tender.tables(cluster)

    0..50_000
    |> Enum.find_value(fn suffix ->
      key = Key.new(@namespace, set, "#{prefix}-#{suffix}")

      case Router.pick_for_read(tables, key, :master, 0) do
        {:ok, ^node_name} ->
          key

        {:ok, _other} ->
          nil

        {:error, reason} ->
          flunk("expected a routed key for #{node_name}, got #{inspect(reason)}")
      end
    end)
    |> case do
      %Key{} = key -> key
      nil -> flunk("expected to find a key routed to #{node_name}")
    end
  end

  defp seed_nodes!(seeds) do
    Map.new(seeds, fn {host, port} ->
      {:ok, conn} = Tcp.connect(host, port)

      try do
        {:ok, %{"node" => node_name}} = Tcp.info(conn, ["node"])

        {node_name, %{host: host, port: port, container: Map.fetch!(@seed_containers, port)}}
      after
        :ok = Tcp.close(conn)
      end
    end)
  end

  defp stopped_seed!(seed_nodes, node_name) do
    case Map.fetch(seed_nodes, node_name) do
      {:ok, seed} -> seed
      :error -> flunk("expected to map #{node_name} back to a local cluster seed")
    end
  end

  defp assert_transport_failure!(%Error{code: code})
       when code in [
              :connection_error,
              :network_error,
              :timeout,
              :pool_timeout,
              :invalid_node,
              :circuit_open
            ],
       do: :ok

  defp assert_transport_failure!(other) do
    flunk("expected a transport-class batch error for the dead node, got #{inspect(other)}")
  end

  defp wait_for_tcp_down!(host, port, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    wait_until!(
      deadline,
      fn ->
        case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 250) do
          {:ok, sock} ->
            :gen_tcp.close(sock)
            false

          {:error, _reason} ->
            true
        end
      end,
      "expected #{host}:#{port} to stop accepting TCP connections"
    )
  end

  defp wait_until!(deadline, predicate, error_message) do
    cond do
      predicate.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(error_message)

      true ->
        Process.sleep(100)
        wait_until!(deadline, predicate, error_message)
    end
  end

  defp docker_stop(container) do
    {_, 0} = System.cmd("docker", ["stop", container], stderr_to_stdout: true)
  end

  defp docker_start(container) do
    {_, 0} = System.cmd("docker", ["start", container], stderr_to_stdout: true)
  end
end
