defmodule Aerospike.FacadeUnitTest do
  use ExUnit.Case, async: false

  import Aerospike.Op

  alias Aerospike.Batch
  alias Aerospike.Key
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
      assert {:error, %{code: :cluster_not_ready}} = Aerospike.batch_exists(conn, [key])

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
               Aerospike.batch_exists(:nonexistent, [key], bad_opt: true)

      assert {:error, %{code: :parameter_error}} =
               Aerospike.batch_operate(:nonexistent, [Batch.read(key)], bad_opt: true)
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
        Aerospike.batch_exists!(:nonexistent, [key], bad_opt: true)
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

      assert {:error, %Aerospike.Error{code: :unsupported_feature}} =
               Aerospike.query_execute(:nonexistent, query, [:write_op])

      assert_raise Aerospike.Error, fn ->
        Aerospike.query_execute!(:nonexistent, query, [:write_op])
      end

      assert {:error, %Aerospike.Error{code: :unsupported_feature}} =
               Aerospike.query_udf(:nonexistent, query, "pkg", "fn", [])

      assert_raise Aerospike.Error, fn ->
        Aerospike.query_udf!(:nonexistent, query, "pkg", "fn", [])
      end

      assert {:error, %Aerospike.Error{code: :unsupported_feature}} =
               Aerospike.query_aggregate(:nonexistent, query, "pkg", "fn", [])

      assert_raise Aerospike.Error, fn ->
        Aerospike.query_aggregate!(:nonexistent, query, "pkg", "fn", [])
      end
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
end
