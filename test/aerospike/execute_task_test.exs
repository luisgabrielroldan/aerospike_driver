defmodule Aerospike.ExecuteTaskTest do
  use ExUnit.Case, async: false

  alias Aerospike.Error
  alias Aerospike.ExecuteTask
  alias Aerospike.NodeSupervisor
  alias Aerospike.PartitionMapWriter
  alias Aerospike.TableOwner
  alias Aerospike.Tender
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  @namespace "test"

  setup do
    host = "10.0.0.1"
    port = 3_000
    name = :"execute_task_test_#{System.unique_integer([:positive, :monotonic])}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", host, port}])
    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)
    {:ok, writer} = PartitionMapWriter.start_link(name: name, tables: tables)
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
      stop_quietly(writer)
      stop_quietly(owner)
      stop_quietly(fake)
    end)

    {:ok, conn: tender, fake: fake}
  end

  describe "parse_status_response/1" do
    test "treats empty and not-found responses as not found" do
      assert ExecuteTask.parse_status_response("") == :not_found
      assert ExecuteTask.parse_status_response("ERROR:2:not found") == :not_found
    end

    test "extracts query status values" do
      assert ExecuteTask.parse_status_response("trid=99:status=DONE:job-type=query") == :complete

      assert ExecuteTask.parse_status_response("trid=99:status=IN_PROGRESS:job-type=query") ==
               :in_progress
    end

    test "returns an error for server failures" do
      assert {:error, %Error{code: :server_error}} =
               ExecuteTask.parse_status_response("ERROR:201:bad command")
    end
  end

  describe "status/1 and wait/2" do
    test "returns complete when a node reports DONE", %{conn: conn, fake: fake} do
      commands = candidate_commands(42)

      Fake.script_info(fake, "A1", commands, %{
        Enum.at(commands, 0) => "trid=42:status=DONE:job-type=query",
        Enum.at(commands, 1) => "ERROR:2:not found",
        Enum.at(commands, 2) => "ERROR:2:not found",
        Enum.at(commands, 3) => "ERROR:2:not found"
      })

      assert {:ok, :complete} =
               ExecuteTask.status(%ExecuteTask{
                 conn: conn,
                 namespace: @namespace,
                 set: "demo",
                 task_id: 42,
                 kind: :query_execute
               })
    end

    test "treats an all-not-found first poll as still in progress", %{conn: conn, fake: fake} do
      commands = candidate_commands(99)
      Fake.script_info(fake, "A1", commands, Map.new(commands, &{&1, "ERROR:2:not found"}))

      assert {:ok, :in_progress} =
               ExecuteTask.status(%ExecuteTask{
                 conn: conn,
                 namespace: @namespace,
                 set: "demo",
                 task_id: 99,
                 kind: :query_execute
               })
    end

    test "wait keeps polling until a later attempt reaches complete", %{conn: conn, fake: fake} do
      commands = candidate_commands(123)
      not_found = Map.new(commands, &{&1, "ERROR:2:not found"})

      Fake.script_info(fake, "A1", commands, not_found)

      Fake.script_info(fake, "A1", commands, %{
        Enum.at(commands, 0) => "trid=123:status=DONE:job-type=query",
        Enum.at(commands, 1) => "ERROR:2:not found",
        Enum.at(commands, 2) => "ERROR:2:not found",
        Enum.at(commands, 3) => "ERROR:2:not found"
      })

      assert :ok =
               ExecuteTask.wait(
                 %ExecuteTask{
                   conn: conn,
                   namespace: @namespace,
                   set: "demo",
                   task_id: 123,
                   kind: :query_execute
                 },
                 poll_interval: 0,
                 timeout: 100
               )
    end

    test "returns invalid_node when a specific node name is unknown", %{conn: conn} do
      assert {:error, %Error{code: :invalid_node, node: "missing"}} =
               ExecuteTask.status(%ExecuteTask{
                 conn: conn,
                 namespace: @namespace,
                 set: "demo",
                 task_id: 7,
                 kind: :query_execute,
                 node_name: "missing"
               })
    end
  end

  defp candidate_commands(task_id) do
    task_id = Integer.to_string(task_id)

    [
      "query-show:id=#{task_id}",
      "query-show:trid=#{task_id}",
      "jobs:module=query;cmd=get-job;id=#{task_id}",
      "jobs:module=query;cmd=get-job;trid=#{task_id}"
    ]
  end

  defp script_single_node_cluster(fake) do
    Fake.script_info(fake, "A1", ["node", "features"], %{"node" => "A1", "features" => ""})

    Fake.script_info(
      fake,
      "A1",
      ["partition-generation", "cluster-stable", "peers-generation"],
      %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef",
        "peers-generation" => "1"
      }
    )

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
