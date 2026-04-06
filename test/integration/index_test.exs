defmodule Aerospike.Integration.IndexTest do
  use ExUnit.Case, async: false

  alias Aerospike.IndexTask
  alias Aerospike.Tables

  @moduletag :integration

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"index_itest_#{System.unique_integer([:positive])}"

    {:ok, _sup} =
      start_supervised(
        {Aerospike,
         name: name,
         hosts: ["#{host}:#{port}"],
         pool_size: 2,
         connect_timeout: 5_000,
         tend_interval: 60_000}
      )

    await_cluster_ready(name)
    {:ok, conn: name}
  end

  describe "create_index/4 and drop_index/3" do
    test "creates a numeric index and returns an IndexTask", %{conn: conn} do
      set = "idx_test_#{System.unique_integer([:positive])}"
      index_name = "age_idx_#{System.unique_integer([:positive])}"

      assert {:ok, %IndexTask{} = task} =
               Aerospike.create_index(conn, "test", set,
                 bin: "age",
                 name: index_name,
                 type: :numeric
               )

      assert task.conn == conn
      assert task.namespace == "test"
      assert task.index_name == index_name

      :ok = Aerospike.drop_index(conn, "test", index_name)
    end

    test "IndexTask.wait/2 blocks until index is complete", %{conn: conn} do
      set = "idx_wait_#{System.unique_integer([:positive])}"
      index_name = "wait_idx_#{System.unique_integer([:positive])}"

      for i <- 1..10 do
        key = Aerospike.key("test", set, "r#{i}")
        :ok = Aerospike.put(conn, key, %{"age" => i * 5})
      end

      {:ok, task} =
        Aerospike.create_index(conn, "test", set,
          bin: "age",
          name: index_name,
          type: :numeric
        )

      assert :ok = IndexTask.wait(task, timeout: 30_000, poll_interval: 200)

      assert {:ok, :complete} = IndexTask.status(task)

      :ok = Aerospike.drop_index(conn, "test", index_name)
    end

    test "drop_index/3 returns :ok for non-existent index", %{conn: conn} do
      assert :ok =
               Aerospike.drop_index(
                 conn,
                 "test",
                 "no_such_index_#{System.unique_integer([:positive])}"
               )
    end

    test "creates a string index", %{conn: conn} do
      set = "idx_str_#{System.unique_integer([:positive])}"
      index_name = "name_idx_#{System.unique_integer([:positive])}"

      assert {:ok, %IndexTask{}} =
               Aerospike.create_index(conn, "test", set,
                 bin: "name",
                 name: index_name,
                 type: :string
               )

      :ok = Aerospike.drop_index(conn, "test", index_name)
    end

    test "creates a list collection index", %{conn: conn} do
      set = "idx_list_#{System.unique_integer([:positive])}"
      index_name = "tags_idx_#{System.unique_integer([:positive])}"

      assert {:ok, %IndexTask{}} =
               Aerospike.create_index(conn, "test", set,
                 bin: "tags",
                 name: index_name,
                 type: :string,
                 collection: :list
               )

      :ok = Aerospike.drop_index(conn, "test", index_name)
    end

    test "returns error for invalid opts", %{conn: conn} do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.create_index(conn, "test", "demo", bin: "age", name: "x", type: :bad)
    end

    test "returns error for missing required opts", %{conn: conn} do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.create_index(conn, "test", "demo", bin: "age")
    end
  end

  defp await_cluster_ready(name, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_cluster_ready_loop(name, deadline)
  end

  defp await_cluster_ready_loop(name, deadline) do
    cond do
      match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key())) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("cluster not ready within timeout")

      true ->
        Process.sleep(50)
        await_cluster_ready_loop(name, deadline)
    end
  end
end
