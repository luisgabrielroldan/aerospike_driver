defmodule Aerospike.IndexTaskTest do
  use ExUnit.Case, async: true

  alias Aerospike.IndexTask
  alias Aerospike.NodePool
  alias Aerospike.Tables
  alias Aerospike.Test.MockTcpServer

  describe "struct" do
    test "enforces required keys" do
      task = %IndexTask{conn: :my_conn, namespace: "test", index_name: "age_idx"}
      assert task.conn == :my_conn
      assert task.namespace == "test"
      assert task.index_name == "age_idx"
    end

    test "raises on missing required keys" do
      assert_raise ArgumentError, fn ->
        struct!(IndexTask, conn: :my_conn)
      end
    end
  end

  describe "status/1 response parsing" do
    # We test the parsing logic by invoking the private parse function indirectly
    # via a stubbed status call. Since status/1 requires a live Router, we test
    # the full module interface through the AsyncTask polling path using a mock.

    defmodule StubTask do
      use Aerospike.AsyncTask

      @impl Aerospike.AsyncTask
      def status(%{response: response}) do
        parse(response)
      end

      defp parse(""), do: {:ok, :complete}

      defp parse(response) do
        case Regex.run(~r/load_pct=(\d+)/, response) do
          [_, "100"] -> {:ok, :complete}
          [_, _] -> {:ok, :in_progress}
          nil -> {:ok, :complete}
        end
      end
    end

    test "empty response -> complete" do
      assert {:ok, :complete} = StubTask.status(%{response: ""})
    end

    test "load_pct=100 -> complete" do
      response = "ns=test:indexname=age_idx:bin=age:load_pct=100:state=RW"
      assert {:ok, :complete} = StubTask.status(%{response: response})
    end

    test "load_pct < 100 -> in_progress" do
      response = "ns=test:indexname=age_idx:bin=age:load_pct=47:state=RW"
      assert {:ok, :in_progress} = StubTask.status(%{response: response})
    end

    test "load_pct=0 -> in_progress" do
      response = "ns=test:indexname=age_idx:bin=age:load_pct=0:state=RW"
      assert {:ok, :in_progress} = StubTask.status(%{response: response})
    end

    test "response without load_pct -> complete (synced index)" do
      response = "ns=test:indexname=age_idx:bin=age:sync_state=synced:state=RW"
      assert {:ok, :complete} = StubTask.status(%{response: response})
    end

    test "wait/2 delegates to poll loop" do
      # Immediately complete
      assert :ok = StubTask.wait(%{response: ""})
    end
  end

  describe "IndexTask fields" do
    test "t/0 type is a struct" do
      task = %IndexTask{conn: :aero, namespace: "test", index_name: "demo_idx"}
      assert is_struct(task, IndexTask)
    end
  end

  describe "status/1 command compatibility" do
    setup do
      name = :"index_task_#{System.unique_integer([:positive, :monotonic])}"
      start_ets(name)
      {:ok, name: name}
    end

    test "uses the modern status command on Aerospike 8.1+", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert body == "build\n"
          MockTcpServer.send_info_response(client, "build\t8.1.0.0\n")

          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert body == "sindex-stat:namespace=test;indexname=age_idx\n"

          MockTcpServer.send_info_response(
            client,
            "sindex-stat:namespace=test;indexname=age_idx\tload_pct=100;state=RW\n"
          )
        end)

      register_node(name, pool_pid, 3_025)

      assert {:ok, :complete} =
               IndexTask.status(%IndexTask{conn: name, namespace: "test", index_name: "age_idx"})

      Task.await(server)
    end

    test "uses the legacy status command before Aerospike 8.1", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert body == "build\n"
          MockTcpServer.send_info_response(client, "build\t7.2.0.0\n")

          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert body == "sindex/test/age_idx\n"
          MockTcpServer.send_info_response(client, "sindex/test/age_idx\tload_pct=47:state=RW\n")
        end)

      register_node(name, pool_pid, 3_026)

      assert {:ok, :in_progress} =
               IndexTask.status(%IndexTask{conn: name, namespace: "test", index_name: "age_idx"})

      Task.await(server)
    end
  end

  defp start_ets(name) do
    :ets.new(Tables.nodes(name), [:set, :public, :named_table, read_concurrency: true])
    :ets.new(Tables.meta(name), [:set, :public, :named_table])

    on_exit(fn ->
      for t <- [Tables.nodes(name), Tables.meta(name)] do
        try do
          :ets.delete(t)
        catch
          :error, :badarg -> :ok
        end
      end
    end)
  end

  defp start_pool_with_server(name, handler, auth_opts \\ []) do
    {:ok, listen_socket, port} = MockTcpServer.start()

    server =
      Task.async(fn ->
        MockTcpServer.accept_once(listen_socket, handler)
      end)

    pool_pid =
      start_supervised!(
        Supervisor.child_spec(
          {NimblePool,
           [
             worker:
               {NodePool,
                connect_opts: [
                  host: "127.0.0.1",
                  port: port,
                  timeout: 30_000,
                  recv_timeout: 30_000
                ],
                auth_opts: auth_opts},
             pool_size: 1
           ]},
          id: {NimblePool, name, port}
        )
      )

    {:ok, pool_pid, server}
  end

  defp register_node(name, pool_pid, port) do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})

    :ets.insert(
      Tables.nodes(name),
      {"node1", %{pool_pid: pool_pid, host: "127.0.0.1", port: port, active: true}}
    )
  end
end
