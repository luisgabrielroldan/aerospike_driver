defmodule Aerospike.FacadeUnitTest do
  use ExUnit.Case, async: false

  import Aerospike.Op

  alias Aerospike.Batch
  alias Aerospike.Key
  alias Aerospike.Tables

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
end
