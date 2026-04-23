defmodule Aerospike.RegisterTaskTest do
  use ExUnit.Case, async: false

  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.RegisterTask
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  @namespace "test"

  setup do
    name = :"register_task_test_#{System.unique_integer([:positive, :monotonic])}"

    {:ok, fake} =
      Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}, {"B1", "10.0.0.2", 3000}])

    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)
    {:ok, writer} = PartitionMapWriter.start_link(name: name, tables: tables)
    {:ok, node_sup} = NodeSupervisor.start_link(name: name)

    {:ok, tender} =
      Tender.start_link(
        name: name,
        transport: Fake,
        connect_opts: [fake: fake],
        seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}],
        namespaces: [@namespace],
        tables: tables,
        tend_trigger: :manual,
        node_supervisor: NodeSupervisor.sup_name(name),
        pool_size: 1
      )

    script_two_node_cluster(fake)
    :ok = Tender.tend_now(tender)

    on_exit(fn ->
      stop_quietly(tender)
      stop_quietly(node_sup)
      stop_quietly(writer)
      stop_quietly(owner)
      stop_quietly(fake)
    end)

    {:ok, conn: tender, fake: fake}
  end

  test "status stays in progress until every active node reports the package", %{
    conn: conn,
    fake: fake
  } do
    Fake.script_info(fake, "A1", ["udf-list"], %{
      "udf-list" => "filename=demo.lua,hash=abc,type=LUA;"
    })

    Fake.script_info(fake, "B1", ["udf-list"], %{"udf-list" => ""})

    assert {:ok, :in_progress} =
             RegisterTask.status(%RegisterTask{conn: conn, package_name: "demo.lua"})

    Fake.script_info(fake, "A1", ["udf-list"], %{
      "udf-list" => "filename=demo.lua,hash=abc,type=LUA;"
    })

    Fake.script_info(fake, "B1", ["udf-list"], %{
      "udf-list" => "filename=demo.lua,hash=def,type=LUA;"
    })

    assert :ok =
             RegisterTask.wait(
               %RegisterTask{conn: conn, package_name: "demo.lua"},
               poll_interval: 0,
               timeout: 100
             )
  end

  test "status returns server errors for malformed udf inventory", %{conn: conn, fake: fake} do
    Fake.script_info(fake, "A1", ["udf-list"], %{
      "udf-list" => "filename=demo.lua,hash=abc,type=LUA;"
    })

    Fake.script_info(fake, "B1", ["udf-list"], %{"udf-list" => "filename=broken.lua,hash=abc;"})

    assert {:error, %Error{code: :server_error, message: message}} =
             RegisterTask.status(%RegisterTask{conn: conn, package_name: "demo.lua"})

    assert message ==
             "invalid udf-list entry (missing type): \"filename=broken.lua,hash=abc\""
  end

  defp script_two_node_cluster(fake) do
    for node <- ["A1", "B1"] do
      Fake.script_info(fake, node, ["node", "features"], %{"node" => node, "features" => ""})

      Fake.script_info(
        fake,
        node,
        ["partition-generation", "cluster-stable", "peers-generation"],
        %{
          "partition-generation" => "1",
          "cluster-stable" => "deadbeef",
          "peers-generation" => "1"
        }
      )
    end

    Fake.script_info(fake, "A1", ["peers-clear-std"], %{
      "peers-clear-std" => "0,3000,[[::1]:3000,[::2]:3000]"
    })

    Fake.script_info(fake, "B1", ["peers-clear-std"], %{
      "peers-clear-std" => "0,3000,[[::1]:3000,[::2]:3000]"
    })

    for node <- ["A1", "B1"] do
      Fake.script_info(fake, node, ["replicas"], %{
        "replicas" => ReplicasFixture.all_master(@namespace, 2)
      })
    end
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
