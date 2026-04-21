defmodule Aerospike.AdminTest do
  use ExUnit.Case, async: true

  alias Aerospike.IndexTask
  alias Aerospike.NodeSupervisor
  alias Aerospike.TableOwner
  alias Aerospike.Tender
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  @namespace "test"

  setup do
    host = "10.0.0.1"
    port = 3_000
    name = :"admin_test_#{System.unique_integer([:positive, :monotonic])}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", host, port}])
    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)
    {:ok, node_sup} = NodeSupervisor.start_link(name: name)

    {:ok, tender} =
      Tender.start_link(
        name: name,
        transport: Fake,
        connect_opts: [fake: fake],
        seeds: [{host, port}],
        namespaces: [@namespace],
        tables: tables,
        tend_trigger: :manual,
        node_supervisor: NodeSupervisor.sup_name(name),
        pool_size: 1
      )

    script_single_node_cluster(fake)
    :ok = Tender.tend_now(tender)

    on_exit(fn ->
      stop_quietly(tender)
      stop_quietly(node_sup)
      stop_quietly(owner)
      stop_quietly(fake)
    end)

    {:ok, conn: tender, fake: fake}
  end

  test "create_index/4 returns a task and the task polls to completion", %{
    conn: conn,
    fake: fake
  } do
    set = "admin_idx_#{System.unique_integer([:positive, :monotonic])}"
    index_name = "age_idx_#{System.unique_integer([:positive, :monotonic])}"

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(
      fake,
      "A1",
      ["sindex-create:namespace=test;set=#{set};indexname=#{index_name};bin=age;type=NUMERIC"],
      %{
        "sindex-create:namespace=test;set=#{set};indexname=#{index_name};bin=age;type=NUMERIC" =>
          "OK"
      }
    )

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(fake, "A1", ["sindex-stat:namespace=test;indexname=#{index_name}"], %{
      "sindex-stat:namespace=test;indexname=#{index_name}" => "load_pct=47;state=RW"
    })

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(fake, "A1", ["sindex-stat:namespace=test;indexname=#{index_name}"], %{
      "sindex-stat:namespace=test;indexname=#{index_name}" => "load_pct=100;state=RW"
    })

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(fake, "A1", ["sindex-delete:namespace=test;indexname=#{index_name}"], %{
      "sindex-delete:namespace=test;indexname=#{index_name}" => "OK"
    })

    assert {:ok, %IndexTask{} = task} =
             Aerospike.create_index(conn, @namespace, set,
               bin: "age",
               name: index_name,
               type: :numeric
             )

    assert task.conn == conn
    assert task.namespace == @namespace
    assert task.index_name == index_name
    assert :ok = IndexTask.wait(task, timeout: 2_000, poll_interval: 10)
    assert :ok = Aerospike.drop_index(conn, @namespace, index_name)
  end

  defp script_single_node_cluster(fake) do
    Fake.script_info(fake, "A1", ["node", "features"], %{"node" => "A1", "features" => ""})

    Fake.script_info(fake, "A1", ["partition-generation", "cluster-stable"], %{
      "partition-generation" => "1",
      "cluster-stable" => "deadbeef"
    })

    Fake.script_info(fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

    Fake.script_info(fake, "A1", ["replicas"], %{
      "replicas" => ReplicasFixture.all_master(@namespace, 1)
    })
  end

  defp stop_quietly(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :shutdown)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        1_000 -> :ok
      end
    end
  end
end
