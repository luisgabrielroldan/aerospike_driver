defmodule Aerospike.FacadeUnitTest do
  use ExUnit.Case, async: false

  import Aerospike.Op

  alias Aerospike.Batch
  alias Aerospike.Key
  alias Aerospike.PartitionFilter
  alias Aerospike.Query
  alias Aerospike.Scan
  alias Aerospike.TableOwner
  alias Aerospike.Tables
  alias Aerospike.Txn

  setup do
    key = Key.new("test", "users", "facade-unit-key")
    {:ok, key: key}
  end

  describe "CRUD with no policy_defaults and cluster not ready" do
    setup %{key: key} do
      name = :"crud_nodefaults_#{:erlang.unique_integer([:positive])}"
      meta = Tables.meta(name)
      nodes = Tables.nodes(name)
      parts = Tables.partitions(name)

      :ets.new(meta, [:set, :public, :named_table])
      :ets.new(nodes, [:set, :public, :named_table, read_concurrency: true])
      :ets.new(parts, [:set, :public, :named_table, read_concurrency: true])

      on_exit(fn ->
        for t <- [meta, nodes, parts] do
          try do
            :ets.delete(t)
          catch
            :error, :badarg -> :ok
          end
        end
      end)

      {:ok, conn: name, key: key}
    end

    test "get returns cluster_not_ready when no defaults and cluster not initialized",
         %{conn: conn, key: key} do
      assert {:error, %Aerospike.Error{code: :cluster_not_ready}} = Aerospike.get(conn, key)
    end

    test "delete returns cluster_not_ready", %{conn: conn, key: key} do
      assert {:error, %Aerospike.Error{code: :cluster_not_ready}} = Aerospike.delete(conn, key)
    end

    test "exists returns cluster_not_ready", %{conn: conn, key: key} do
      assert {:error, %Aerospike.Error{code: :cluster_not_ready}} = Aerospike.exists(conn, key)
    end

    test "touch returns cluster_not_ready", %{conn: conn, key: key} do
      assert {:error, %Aerospike.Error{code: :cluster_not_ready}} = Aerospike.touch(conn, key)
    end

    test "put returns cluster_not_ready", %{conn: conn, key: key} do
      assert {:error, %Aerospike.Error{code: :cluster_not_ready}} =
               Aerospike.put(conn, key, %{"x" => 1})
    end

    test "add/append/prepend return cluster_not_ready", %{conn: conn, key: key} do
      assert {:error, %{code: :cluster_not_ready}} = Aerospike.add(conn, key, %{"c" => 1})
      assert {:error, %{code: :cluster_not_ready}} = Aerospike.append(conn, key, %{"s" => "x"})
      assert {:error, %{code: :cluster_not_ready}} = Aerospike.prepend(conn, key, %{"s" => "x"})
    end

    test "operate returns cluster_not_ready with non-empty ops", %{conn: conn, key: key} do
      assert {:error, %{code: :cluster_not_ready}} =
               Aerospike.operate(conn, key, [get("n")])
    end

    test "batch_get/batch_exists/batch_operate return cluster_not_ready", %{conn: conn, key: key} do
      assert {:error, %{code: :cluster_not_ready}} = Aerospike.batch_get(conn, [key])
      assert {:error, %{code: :cluster_not_ready}} = Aerospike.batch_get_header(conn, [key])
      assert {:error, %{code: :cluster_not_ready}} = Aerospike.batch_exists(conn, [key])

      assert {:error, %{code: :cluster_not_ready}} =
               Aerospike.batch_get_operate(conn, [key], [get("n")])

      assert {:error, %{code: :cluster_not_ready}} = Aerospike.batch_delete(conn, [key])

      assert {:error, %{code: :cluster_not_ready}} =
               Aerospike.batch_udf(conn, [key], "pkg", "fn", [])

      assert {:error, %{code: :cluster_not_ready}} =
               Aerospike.batch_operate(conn, [Batch.read(key)])
    end

    test "batch_operate with empty ops list returns ok empty", %{conn: conn} do
      assert {:ok, []} = Aerospike.batch_operate(conn, [])
    end
  end

  describe "validation error paths" do
    test "get with invalid option returns parameter_error", %{key: key} do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.get(:nonexistent, key, bad_opt: true)
    end

    test "delete with invalid option returns parameter_error", %{key: key} do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.delete(:nonexistent, key, bad_opt: true)
    end

    test "exists with invalid option returns parameter_error", %{key: key} do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.exists(:nonexistent, key, bad_opt: true)
    end

    test "touch with invalid option returns parameter_error", %{key: key} do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.touch(:nonexistent, key, bad_opt: true)
    end

    test "add/append/prepend with invalid option returns parameter_error", %{key: key} do
      assert {:error, %{code: :parameter_error}} =
               Aerospike.add(:nonexistent, key, %{"c" => 1}, bad_opt: true)

      assert {:error, %{code: :parameter_error}} =
               Aerospike.append(:nonexistent, key, %{"s" => "x"}, bad_opt: true)

      assert {:error, %{code: :parameter_error}} =
               Aerospike.prepend(:nonexistent, key, %{"s" => "x"}, bad_opt: true)
    end

    test "operate with invalid option returns parameter_error", %{key: key} do
      assert {:error, %{code: :parameter_error}} =
               Aerospike.operate(:nonexistent, key, [get("n")], bad_opt: true)
    end

    test "batch_* with invalid option returns parameter_error", %{key: key} do
      assert {:error, %{code: :parameter_error}} =
               Aerospike.batch_get(:nonexistent, [key], bad_opt: true)

      assert {:error, %{code: :parameter_error}} =
               Aerospike.batch_get_header(:nonexistent, [key], header_only: true)

      assert {:error, %{code: :parameter_error}} =
               Aerospike.batch_exists(:nonexistent, [key], bad_opt: true)

      assert {:error, %{code: :parameter_error}} =
               Aerospike.batch_get_operate(:nonexistent, [key], [get("n")], bad_opt: true)

      assert {:error, %{code: :parameter_error}} =
               Aerospike.batch_delete(:nonexistent, [key], bad_opt: true)

      assert {:error, %{code: :parameter_error}} =
               Aerospike.batch_udf(:nonexistent, [key], "pkg", "fn", [], bad_opt: true)

      assert {:error, %{code: :parameter_error}} =
               Aerospike.batch_operate(:nonexistent, [Batch.read(key)], bad_opt: true)
    end

    test "batch_get_header rejects contradictory read options", %{key: key} do
      assert {:error, %{code: :parameter_error, message: message}} =
               Aerospike.batch_get_header(:nonexistent, [key], bins: ["n"])

      assert message =~ "does not accept :bins"
    end

    test "batch_get_operate rejects empty and write op lists", %{key: key} do
      assert {:error, %{code: :parameter_error, message: empty_message}} =
               Aerospike.batch_get_operate(:nonexistent, [key], [])

      assert empty_message =~ "cannot be empty"

      assert {:error, %{code: :parameter_error, message: write_message}} =
               Aerospike.batch_get_operate(:nonexistent, [key], [put("n", 1)])

      assert write_message =~ "read-only"
    end

    test "single-record APIs return parameter_error on invalid key shape" do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.delete(:nonexistent, {:invalid})

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.exists(:nonexistent, {:invalid})

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.touch(:nonexistent, {:invalid})

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.operate(:nonexistent, {:invalid}, [get("n")])

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.add(:nonexistent, {:invalid}, %{"n" => 1})

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.append(:nonexistent, {:invalid}, %{"s" => "x"})

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.prepend(:nonexistent, {:invalid}, %{"s" => "x"})
    end
  end

  describe "bang variants raise on validation error" do
    test "put! raises Aerospike.Error", %{key: key} do
      assert_raise Aerospike.Error, fn ->
        Aerospike.put!(:nonexistent, key, %{}, bad_opt: true)
      end
    end

    test "get! raises Aerospike.Error", %{key: key} do
      assert_raise Aerospike.Error, fn ->
        Aerospike.get!(:nonexistent, key, bad_opt: true)
      end
    end

    test "delete! raises Aerospike.Error", %{key: key} do
      assert_raise Aerospike.Error, fn ->
        Aerospike.delete!(:nonexistent, key, bad_opt: true)
      end
    end

    test "exists! raises Aerospike.Error", %{key: key} do
      assert_raise Aerospike.Error, fn ->
        Aerospike.exists!(:nonexistent, key, bad_opt: true)
      end
    end

    test "touch! raises Aerospike.Error", %{key: key} do
      assert_raise Aerospike.Error, fn ->
        Aerospike.touch!(:nonexistent, key, bad_opt: true)
      end
    end

    test "add!/append!/prepend! raise on validation error", %{key: key} do
      assert_raise Aerospike.Error, fn ->
        Aerospike.add!(:nonexistent, key, %{"c" => 1}, bad_opt: true)
      end

      assert_raise Aerospike.Error, fn ->
        Aerospike.append!(:nonexistent, key, %{"s" => "x"}, bad_opt: true)
      end

      assert_raise Aerospike.Error, fn ->
        Aerospike.prepend!(:nonexistent, key, %{"s" => "x"}, bad_opt: true)
      end
    end

    test "operate! raises on validation error", %{key: key} do
      assert_raise Aerospike.Error, fn ->
        Aerospike.operate!(:nonexistent, key, [get("n")], bad_opt: true)
      end
    end

    test "batch_*! raise on validation error", %{key: key} do
      assert_raise Aerospike.Error, fn ->
        Aerospike.batch_get!(:nonexistent, [key], bad_opt: true)
      end

      assert_raise Aerospike.Error, fn ->
        Aerospike.batch_get_header!(:nonexistent, [key], header_only: true)
      end

      assert_raise Aerospike.Error, fn ->
        Aerospike.batch_exists!(:nonexistent, [key], bad_opt: true)
      end

      assert_raise Aerospike.Error, fn ->
        Aerospike.batch_get_operate!(:nonexistent, [key], [put("n", 1)])
      end

      assert_raise Aerospike.Error, fn ->
        Aerospike.batch_delete!(:nonexistent, [key], bad_opt: true)
      end

      assert_raise Aerospike.Error, fn ->
        Aerospike.batch_udf!(:nonexistent, [key], "pkg", "fn", [], bad_opt: true)
      end

      assert_raise Aerospike.Error, fn ->
        Aerospike.batch_operate!(:nonexistent, [Batch.read(key)], bad_opt: true)
      end
    end
  end

  describe "facade edge cases" do
    setup %{key: key} do
      name = :"facade_edge_#{:erlang.unique_integer([:positive])}"
      meta = Tables.meta(name)
      nodes = Tables.nodes(name)
      parts = Tables.partitions(name)

      :ets.new(meta, [:set, :public, :named_table])
      :ets.new(nodes, [:set, :public, :named_table, read_concurrency: true])
      :ets.new(parts, [:set, :public, :named_table, read_concurrency: true])

      on_exit(fn ->
        for t <- [meta, nodes, parts] do
          try do
            :ets.delete(t)
          catch
            :error, :badarg -> :ok
          end
        end
      end)

      {:ok, conn: name, key: key}
    end

    test "operate with empty op list returns parameter_error", %{conn: conn, key: key} do
      assert {:error, %{code: :parameter_error}} = Aerospike.operate(conn, key, [])
    end

    test "child_spec returns a supervisor child spec" do
      spec =
        Aerospike.child_spec(
          name: :"facade_child_spec_#{:erlang.unique_integer([:positive])}",
          hosts: ["127.0.0.1:3000"]
        )

      assert spec.type == :supervisor
      assert match?({Aerospike.Supervisor, :start_link, [_]}, spec.start)
    end
  end

  describe "scan/admin/txn wrapper coverage branches" do
    setup do
      scan = Scan.new("test", "users")
      {:ok, scan: scan}
    end

    test "scan APIs map option validation errors and bang wrappers raise", %{scan: scan} do
      assert_raise Aerospike.Error, fn ->
        Aerospike.stream!(:nonexistent, scan, bad_opt: true)
      end

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.all(:nonexistent, scan, bad_opt: true)

      assert_raise Aerospike.Error, fn ->
        Aerospike.all!(:nonexistent, scan, bad_opt: true)
      end

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.count(:nonexistent, scan, bad_opt: true)

      assert_raise Aerospike.Error, fn ->
        Aerospike.count!(:nonexistent, scan, bad_opt: true)
      end

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.page(:nonexistent, scan, bad_opt: true)

      assert_raise Aerospike.Error, fn ->
        Aerospike.page!(:nonexistent, scan, bad_opt: true)
      end
    end

    test "query-specific read APIs map option validation errors and bang wrappers raise" do
      query = Query.new("test", "users")

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.query_stream(:nonexistent, query, bad_opt: true)

      assert_raise Aerospike.Error, fn ->
        Aerospike.query_stream!(:nonexistent, query, bad_opt: true)
      end

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.query_all(:nonexistent, query, bad_opt: true)

      assert_raise Aerospike.Error, fn ->
        Aerospike.query_all!(:nonexistent, query, bad_opt: true)
      end

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.query_count(:nonexistent, query, bad_opt: true)

      assert_raise Aerospike.Error, fn ->
        Aerospike.query_count!(:nonexistent, query, bad_opt: true)
      end

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.query_page(:nonexistent, query, bad_opt: true)

      assert_raise Aerospike.Error, fn ->
        Aerospike.query_page!(:nonexistent, query, bad_opt: true)
      end
    end

    test "admin wrappers map validation errors", %{} do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.info(:nonexistent, "namespaces", bad_opt: true)

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.info_node(:nonexistent, "node", "statistics", bad_opt: true)

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.truncate(:nonexistent, "test", bad_opt: true)

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.truncate(:nonexistent, "test", "users", bad_opt: true)

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.create_index(:nonexistent, "test", "users", bad_opt: true)
    end

    test "udf wrappers cover opts variants and key coercion error", %{key: key} do
      conn = :"facade_udf_#{System.unique_integer([:positive, :monotonic])}"
      meta = Tables.meta(conn)
      nodes = Tables.nodes(conn)
      parts = Tables.partitions(conn)

      :ets.new(meta, [:set, :public, :named_table])
      :ets.new(nodes, [:set, :public, :named_table, read_concurrency: true])
      :ets.new(parts, [:set, :public, :named_table, read_concurrency: true])

      on_exit(fn ->
        for t <- [meta, nodes, parts] do
          try do
            :ets.delete(t)
          catch
            :error, :badarg -> :ok
          end
        end
      end)

      assert {:error, %Aerospike.Error{}} =
               Aerospike.register_udf(conn, "function x() return 1 end", "x.lua", [])

      assert {:error, %Aerospike.Error{}} =
               Aerospike.remove_udf(conn, "x.lua", [])

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.apply_udf(:nonexistent, {:invalid}, "pkg", "f", [], timeout: 1_000)

      assert {:error, %Aerospike.Error{}} =
               Aerospike.apply_udf(conn, key, "pkg", "f", [], timeout: 1_000)
    end

    test "phase 2 query/UDF APIs expose explicit contracts" do
      conn = :"facade_phase2_#{System.unique_integer([:positive, :monotonic])}"
      meta = Tables.meta(conn)
      nodes = Tables.nodes(conn)
      parts = Tables.partitions(conn)

      :ets.new(meta, [:set, :public, :named_table])
      :ets.new(nodes, [:set, :public, :named_table, read_concurrency: true])
      :ets.new(parts, [:set, :public, :named_table, read_concurrency: true])

      on_exit(fn ->
        for t <- [meta, nodes, parts] do
          try do
            :ets.delete(t)
          catch
            :error, :badarg -> :ok
          end
        end
      end)

      query = Query.new("test", "users")

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.list_udfs(:nonexistent, bad_opt: true)

      assert {:error, %Aerospike.Error{code: :cluster_not_ready}} =
               Aerospike.list_udfs(conn)

      assert_raise Aerospike.Error, fn ->
        Aerospike.list_udfs!(conn)
      end

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.query_execute(:nonexistent, query, [])

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.query_execute(conn, query, [get("name")])

      assert {:error, %Aerospike.Error{code: :cluster_not_ready}} =
               Aerospike.query_execute(conn, query, [put("visits", 1)])

      assert_raise Aerospike.Error, fn ->
        Aerospike.query_execute!(conn, query, [put("visits", 1)])
      end

      assert {:error, %Aerospike.Error{code: :cluster_not_ready}} =
               Aerospike.query_udf(conn, query, "pkg", "fn", [])

      assert_raise Aerospike.Error, fn ->
        Aerospike.query_udf!(conn, query, "pkg", "fn", [])
      end

      assert {:ok, aggregate_stream} =
               Aerospike.query_aggregate(conn, query, "pkg", "fn", [])

      assert_raise Aerospike.Error, fn ->
        Enum.to_list(aggregate_stream)
      end

      assert_raise Aerospike.Error, fn ->
        Aerospike.query_aggregate!(conn, query, "pkg", "fn", []) |> Enum.to_list()
      end
    end

    test "phase 4 node-targeted read APIs expose missing-node errors", %{scan: scan} do
      conn = :"facade_phase4_#{System.unique_integer([:positive, :monotonic])}"
      query = Query.new("test", "users")
      setup_ready_tables(conn)

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.query_all_node(:nonexistent, "node-a", query, bad_opt: true)

      assert_raise Aerospike.Error, fn ->
        Aerospike.scan_stream_node!(:nonexistent, "node-a", scan, bad_opt: true) |> Enum.to_list()
      end

      assert {:error, %Aerospike.Error{code: :invalid_node}} =
               Aerospike.query_all_node(conn, "missing-node", Query.max_records(query, 1))

      assert {:error, %Aerospike.Error{code: :invalid_node}} =
               Aerospike.query_count_node(conn, "missing-node", query)

      assert {:error, %Aerospike.Error{code: :invalid_node}} =
               Aerospike.query_page_node(conn, "missing-node", Query.max_records(query, 1))

      assert {:error, %Aerospike.Error{code: :invalid_node}} =
               Aerospike.scan_all_node(conn, "missing-node", Scan.max_records(scan, 1))

      assert {:error, %Aerospike.Error{code: :invalid_node}} =
               Aerospike.scan_count_node(conn, "missing-node", scan)

      assert {:error, %Aerospike.Error{code: :invalid_node}} =
               Aerospike.scan_page_node(conn, "missing-node", Scan.max_records(scan, 1))

      assert {:ok, query_stream} = Aerospike.query_stream_node(conn, "missing-node", query)

      err =
        assert_raise Aerospike.Error, fn ->
          Enum.to_list(query_stream)
        end

      assert err.code == :invalid_node

      err =
        assert_raise Aerospike.Error, fn ->
          Aerospike.scan_stream_node!(conn, "missing-node", scan) |> Enum.to_list()
        end

      assert err.code == :invalid_node
    end

    test "phase 4.5 node-targeted background query APIs expose missing-node errors" do
      conn = :"facade_phase45_#{System.unique_integer([:positive, :monotonic])}"
      query = Query.new("test", "users")
      setup_ready_tables(conn)

      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.query_execute_node(:nonexistent, "node-a", query, [put("visits", 1)],
                 bad_opt: true
               )

      assert_raise Aerospike.Error, fn ->
        Aerospike.query_udf_node!(:nonexistent, "node-a", query, "pkg", "fn", [], bad_opt: true)
      end

      assert {:error, %Aerospike.Error{code: :invalid_node}} =
               Aerospike.query_execute_node(conn, "missing-node", query, [put("visits", 1)])

      assert {:error, %Aerospike.Error{code: :invalid_node}} =
               Aerospike.query_udf_node(conn, "missing-node", query, "pkg", "fn", [])

      err =
        assert_raise Aerospike.Error, fn ->
          Aerospike.query_execute_node!(conn, "missing-node", query, [put("visits", 1)])
        end

      assert err.code == :invalid_node

      err =
        assert_raise Aerospike.Error, fn ->
          Aerospike.query_udf_node!(conn, "missing-node", query, "pkg", "fn", [])
        end

      assert err.code == :invalid_node
    end

    test "phase 4 node-targeted scan APIs do not widen to other nodes when the target has no partitions" do
      conn = :"facade_phase4_scan_scope_#{System.unique_integer([:positive, :monotonic])}"
      setup_ready_tables(conn)

      target_node = "node-a"
      other_node = "node-b"
      part_id = 42

      :ets.insert(Tables.nodes(conn), {target_node, %{pool_pid: self(), active: true}})
      :ets.insert(Tables.nodes(conn), {other_node, %{pool_pid: self(), active: true}})
      :ets.insert(Tables.partitions(conn), {{"test", part_id, 0}, other_node})

      scan =
        Scan.new("test", "users")
        |> Scan.partition_filter(PartitionFilter.by_id(part_id))
        |> Scan.max_records(5)

      assert {:ok, []} = Aerospike.scan_all_node(conn, target_node, scan)
      assert {:ok, 0} = Aerospike.scan_count_node(conn, target_node, scan)

      assert {:ok, %Aerospike.Page{records: [], cursor: nil, done?: true}} =
               Aerospike.scan_page_node(conn, target_node, scan)

      assert [] =
               Aerospike.scan_stream_node!(conn, target_node, scan)
               |> Enum.to_list()
    end

    test "phase 4 node-targeted query APIs do not widen to other nodes when the target has no partitions" do
      conn = :"facade_phase4_query_scope_#{System.unique_integer([:positive, :monotonic])}"
      setup_ready_tables(conn)

      target_node = "node-a"
      other_node = "node-b"
      part_id = 84

      :ets.insert(Tables.nodes(conn), {target_node, %{pool_pid: self(), active: true}})
      :ets.insert(Tables.nodes(conn), {other_node, %{pool_pid: self(), active: true}})
      :ets.insert(Tables.partitions(conn), {{"test", part_id, 0}, other_node})

      query =
        Query.new("test", "users")
        |> Query.partition_filter(PartitionFilter.by_id(part_id))
        |> Query.max_records(5)

      assert {:ok, []} = Aerospike.query_all_node(conn, target_node, query)
      assert {:ok, 0} = Aerospike.query_count_node(conn, target_node, query)

      assert {:ok, %Aerospike.Page{records: [], cursor: nil, done?: true}} =
               Aerospike.query_page_node(conn, target_node, query)

      assert {:ok, stream} = Aerospike.query_stream_node(conn, target_node, query)
      assert [] = Enum.to_list(stream)
    end

    test "phase 4.5 node-targeted background query APIs do not widen to other nodes when the target has no partitions" do
      conn = :"facade_phase45_query_scope_#{System.unique_integer([:positive, :monotonic])}"
      setup_ready_tables(conn)

      target_node = "node-a"
      other_node = "node-b"
      part_id = 126

      :ets.insert(Tables.nodes(conn), {target_node, %{pool_pid: self(), active: true}})
      :ets.insert(Tables.nodes(conn), {other_node, %{pool_pid: self(), active: true}})
      :ets.insert(Tables.partitions(conn), {{"test", part_id, 0}, other_node})

      query =
        Query.new("test", "users")
        |> Query.partition_filter(PartitionFilter.by_id(part_id))
        |> Query.max_records(5)

      assert {:ok, %Aerospike.ExecuteTask{} = execute_task} =
               Aerospike.query_execute_node(conn, target_node, query, [put("visits", 1)])

      assert execute_task.node_name == target_node
      assert execute_task.kind == :query_execute

      assert {:ok, %Aerospike.ExecuteTask{} = udf_task} =
               Aerospike.query_udf_node(conn, target_node, query, "pkg", "fn", [])

      assert udf_task.node_name == target_node
      assert udf_task.kind == :query_udf
    end

    test "transaction wrappers delegate to TxnRoll variants" do
      conn = :"facade_txn_#{System.unique_integer([:positive, :monotonic])}"
      _pid = start_supervised!({TableOwner, name: conn})

      txn = Txn.new()

      assert {:error, %Aerospike.Error{code: :parameter_error}} = Aerospike.commit(conn, txn)
      assert {:error, %Aerospike.Error{code: :parameter_error}} = Aerospike.abort(conn, txn)
      assert {:error, %Aerospike.Error{code: :parameter_error}} = Aerospike.txn_status(conn, txn)

      assert {:ok, :v1} = Aerospike.transaction(conn, fn _txn -> :v1 end)
      assert {:ok, :v2} = Aerospike.transaction(conn, [timeout: 5_000], fn _txn -> :v2 end)
    end
  end

  defp setup_ready_tables(conn) do
    meta = Tables.meta(conn)
    nodes = Tables.nodes(conn)
    parts = Tables.partitions(conn)

    :ets.new(meta, [:set, :public, :named_table])
    :ets.new(nodes, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(parts, [:set, :public, :named_table, read_concurrency: true])
    :ets.insert(meta, {Tables.ready_key(), true})

    on_exit(fn ->
      for t <- [meta, nodes, parts] do
        try do
          :ets.delete(t)
        catch
          :error, :badarg -> :ok
        end
      end
    end)
  end
end
