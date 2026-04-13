defmodule Aerospike.Integration.IndexTest do
  use ExUnit.Case, async: true

  alias Aerospike.Ctx
  alias Aerospike.Exp
  alias Aerospike.Filter
  alias Aerospike.IndexTask
  alias Aerospike.Query
  alias Aerospike.Test.Helpers

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

    Helpers.await_cluster_ready(name)
    {:ok, conn: name, host: host, port: port}
  end

  describe "create_index/4 and drop_index/3" do
    test "creates a numeric index and returns an IndexTask", %{
      conn: conn,
      host: host,
      port: port
    } do
      set = "idx_test_#{System.unique_integer([:positive])}"
      index_name = "age_idx_#{System.unique_integer([:positive])}"
      on_exit(fn -> Helpers.cleanup_index("test", index_name, host: host, port: port) end)

      assert {:ok, %IndexTask{} = task} =
               Aerospike.create_index(conn, "test", set,
                 bin: "age",
                 name: index_name,
                 type: :numeric
               )

      assert task.conn == conn
      assert task.namespace == "test"
      assert task.index_name == index_name
    end

    test "IndexTask.wait/2 blocks until index is complete", %{
      conn: conn,
      host: host,
      port: port
    } do
      set = "idx_wait_#{System.unique_integer([:positive])}"
      index_name = "wait_idx_#{System.unique_integer([:positive])}"
      on_exit(fn -> Helpers.cleanup_index("test", index_name, host: host, port: port) end)

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

      assert_eventually(
        fn ->
          match?({:ok, :complete}, IndexTask.status(task))
        end,
        timeout: 5_000,
        interval: 200
      )
    end

    test "drop_index/3 returns :ok for non-existent index", %{conn: conn} do
      assert :ok =
               Aerospike.drop_index(
                 conn,
                 "test",
                 "no_such_index_#{System.unique_integer([:positive])}"
               )
    end

    test "creates a string index", %{conn: conn, host: host, port: port} do
      set = "idx_str_#{System.unique_integer([:positive])}"
      index_name = "name_idx_#{System.unique_integer([:positive])}"
      on_exit(fn -> Helpers.cleanup_index("test", index_name, host: host, port: port) end)

      assert {:ok, %IndexTask{}} =
               Aerospike.create_index(conn, "test", set,
                 bin: "name",
                 name: index_name,
                 type: :string
               )
    end

    test "creates a list collection index", %{conn: conn, host: host, port: port} do
      set = "idx_list_#{System.unique_integer([:positive])}"
      index_name = "tags_idx_#{System.unique_integer([:positive])}"
      on_exit(fn -> Helpers.cleanup_index("test", index_name, host: host, port: port) end)

      assert {:ok, %IndexTask{}} =
               Aerospike.create_index(conn, "test", set,
                 bin: "tags",
                 name: index_name,
                 type: :string,
                 collection: :list
               )
    end

    test "returns error for invalid opts", %{conn: conn} do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.create_index(conn, "test", "demo", bin: "age", name: "x", type: :bad)
    end

    test "returns error for missing required opts", %{conn: conn} do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.create_index(conn, "test", "demo", bin: "age")
    end

    test "creates a nested CDT index and queries it through Filter.with_ctx/2", %{
      conn: conn,
      host: host,
      port: port
    } do
      set = "idx_ctx_#{System.unique_integer([:positive])}"
      index_name = "roles_idx_#{System.unique_integer([:positive])}"
      ctx = [Ctx.map_key("roles")]

      records = [
        {"admin_1", %{"profile" => %{"roles" => ["admin", "owner"]}}, true},
        {"admin_2", %{"profile" => %{"roles" => ["admin", "editor"]}}, true},
        {"user_1", %{"profile" => %{"roles" => ["viewer"]}}, false}
      ]

      keys =
        Enum.map(records, fn {user_key, bins, _matches?} ->
          key = Aerospike.key("test", set, user_key)
          :ok = Aerospike.put(conn, key, bins)
          key
        end)

      on_exit(fn ->
        Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port))
        Helpers.cleanup_index("test", index_name, host: host, port: port)
      end)

      assert {:ok, %IndexTask{} = task} =
               Aerospike.create_index(conn, "test", set,
                 bin: "profile",
                 name: index_name,
                 type: :string,
                 collection: :list,
                 ctx: ctx
               )

      assert :ok = IndexTask.wait(task, timeout: 30_000, poll_interval: 200)
      Process.sleep(500)

      query =
        Query.new("test", set)
        |> Query.where(Filter.contains("profile", :list, "admin") |> Filter.with_ctx(ctx))
        |> Query.max_records(20)

      assert {:ok, result_records} = Aerospike.all(conn, query)

      result_roles =
        result_records
        |> Enum.map(&get_in(&1.bins, ["profile", "roles"]))
        |> Enum.sort()

      assert result_roles == [["admin", "editor"], ["admin", "owner"]]
    end

    test "expression-backed index creation is version-gated and queryable", %{
      conn: conn,
      host: host,
      port: port
    } do
      set = "idx_expr_#{System.unique_integer([:positive])}"
      index_name = "adult_expr_idx_#{System.unique_integer([:positive])}"
      version = fetch_server_version!(conn)

      records = [
        {"teen", %{"age" => 17}},
        {"adult_1", %{"age" => 24}},
        {"adult_2", %{"age" => 31}}
      ]

      keys =
        Enum.map(records, fn {user_key, bins} ->
          key = Aerospike.key("test", set, user_key)
          :ok = Aerospike.put(conn, key, bins)
          key
        end)

      on_exit(fn ->
        Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port))
        Helpers.cleanup_index("test", index_name, host: host, port: port)
      end)

      expression = Exp.int_bin("age")

      if supports_expression_indexes?(version) do
        assert {:ok, %IndexTask{} = task} =
                 Aerospike.create_expression_index(conn, "test", set, expression,
                   name: index_name,
                   type: :numeric
                 )

        assert :ok = IndexTask.wait(task, timeout: 30_000, poll_interval: 200)
        Process.sleep(500)

        query =
          Query.new("test", set)
          |> Query.where(Filter.range("age", 18, 40) |> Filter.using_index(index_name))
          |> Query.max_records(20)

        assert {:ok, result_records} = Aerospike.all(conn, query)

        result_ages =
          result_records
          |> Enum.map(& &1.bins["age"])
          |> Enum.sort()

        assert result_ages == [24, 31]
      else
        assert {:error, %Aerospike.Error{code: :parameter_error, message: message}} =
                 Aerospike.create_expression_index(conn, "test", set, expression,
                   name: index_name,
                   type: :numeric
                 )

        assert message ==
                 "expression-backed secondary indexes require Aerospike server 8.1.0 or newer"
      end
    end
  end

  defp fetch_server_version!(conn) do
    {:ok, build} = Aerospike.info(conn, "build")

    case Regex.run(~r/^v?\d+(?:\.\d+){0,3}/, build) do
      [matched] ->
        matched
        |> String.trim_leading("v")
        |> String.split(".")
        |> Enum.map(&String.to_integer/1)
        |> Kernel.++([0, 0, 0, 0])
        |> Enum.take(4)
        |> List.to_tuple()

      _ ->
        flunk("unable to parse Aerospike build string: #{inspect(build)}")
    end
  end

  defp supports_expression_indexes?(version) when is_tuple(version), do: version >= {8, 1, 0, 0}

  defp assert_eventually(fun, opts) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.fetch!(opts, :timeout)
    interval = Keyword.get(opts, :interval, 200)
    deadline = System.monotonic_time(:millisecond) + timeout
    assert_eventually_loop(fun, deadline, interval)
  end

  defp assert_eventually_loop(fun, deadline, interval) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) > deadline do
        flunk("condition did not become true within timeout")
      else
        Process.sleep(interval)
        assert_eventually_loop(fun, deadline, interval)
      end
    end
  end
end
