defmodule Aerospike.ExecuteTaskTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.ExecuteTask
  alias Aerospike.Tables

  describe "struct" do
    test "enforces required keys" do
      task = %ExecuteTask{
        conn: :aero,
        namespace: "test",
        set: "users",
        task_id: 42,
        kind: :query_execute
      }

      assert task.conn == :aero
      assert task.namespace == "test"
      assert task.set == "users"
      assert task.task_id == 42
      assert task.kind == :query_execute
      assert task.node_name == nil
    end

    test "raises on missing required keys" do
      assert_raise ArgumentError, fn ->
        struct!(ExecuteTask, conn: :aero)
      end
    end
  end

  describe "parse_status_response/1" do
    test "treats done status as complete" do
      assert :complete = ExecuteTask.parse_status_response("trid=99:status=DONE:job-type=query")
    end

    test "treats active status as in_progress" do
      assert :in_progress =
               ExecuteTask.parse_status_response("trid=99:status=IN_PROGRESS:job-type=query")
    end

    test "treats task-not-found as not_found" do
      assert :not_found = ExecuteTask.parse_status_response("ERROR:2:not found")
      assert :not_found = ExecuteTask.parse_status_response("")
    end

    test "translates other server errors" do
      assert {:error, %Error{code: :server_error}} =
               ExecuteTask.parse_status_response("ERROR:201:bad command")
    end
  end

  describe "status/wait with unavailable cluster" do
    test "returns cluster_not_ready when ETS tables do not exist" do
      task = %ExecuteTask{
        conn: :missing_conn,
        namespace: "test",
        set: "users",
        task_id: 1,
        kind: :query_execute
      }

      assert {:error, %Error{code: :cluster_not_ready}} = ExecuteTask.status(task)
      assert {:error, %Error{code: :cluster_not_ready}} = ExecuteTask.wait(task, timeout: 10)
    end

    test "returns invalid_node when cluster is ready but no nodes are registered" do
      conn = :"execute_task_#{System.unique_integer([:positive, :monotonic])}"
      meta = Tables.meta(conn)
      nodes = Tables.nodes(conn)

      :ets.new(meta, [:set, :public, :named_table])
      :ets.new(nodes, [:set, :public, :named_table, read_concurrency: true])
      :ets.insert(meta, {Tables.ready_key(), true})

      on_exit(fn ->
        for table <- [meta, nodes] do
          try do
            :ets.delete(table)
          catch
            :error, :badarg -> :ok
          end
        end
      end)

      task = %ExecuteTask{
        conn: conn,
        namespace: "test",
        set: "users",
        task_id: 1,
        kind: :query_udf
      }

      assert {:error, %Error{code: :invalid_node}} = ExecuteTask.status(task)
    end
  end
end
