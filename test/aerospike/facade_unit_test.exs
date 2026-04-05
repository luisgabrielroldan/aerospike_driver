defmodule Aerospike.FacadeUnitTest do
  use ExUnit.Case, async: false

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
  end
end
