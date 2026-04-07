defmodule Aerospike.FacadeTupleKeyTest do
  use ExUnit.Case, async: false

  alias Aerospike.Batch
  alias Aerospike.Error
  alias Aerospike.Tables

  setup do
    conn = :"facade_tuple_#{:erlang.unique_integer([:positive])}"
    meta = Tables.meta(conn)
    nodes = Tables.nodes(conn)
    parts = Tables.partitions(conn)

    :ets.new(meta, [:set, :public, :named_table])
    :ets.new(nodes, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(parts, [:set, :public, :named_table, read_concurrency: true])

    on_exit(fn ->
      for table <- [meta, nodes, parts] do
        try do
          :ets.delete(table)
        catch
          :error, :badarg -> :ok
        end
      end
    end)

    {:ok, conn: conn}
  end

  describe "valid tuple keys pass facade coercion boundary" do
    test "single-key operations fail later with cluster_not_ready", %{conn: conn} do
      assert {:error, %Error{code: :cluster_not_ready}} =
               Aerospike.get(conn, {"test", "users", "tuple-key"})

      assert {:error, %Error{code: :cluster_not_ready}} =
               Aerospike.put(conn, {"test", "users", "tuple-key"}, %{"a" => 1})
    end

    test "batch operations with tuple keys fail later with cluster_not_ready", %{conn: conn} do
      assert {:error, %Error{code: :cluster_not_ready}} =
               Aerospike.batch_get(conn, [{"test", "users", "tuple-key"}])

      assert {:error, %Error{code: :cluster_not_ready}} =
               Aerospike.batch_exists(conn, [{"test", "users", "tuple-key"}])

      assert {:error, %Error{code: :cluster_not_ready}} =
               Aerospike.batch_operate(conn, [Batch.read({"test", "users", "tuple-key"})])
    end
  end

  describe "invalid tuple keys are normalized to parameter_error" do
    test "non-bang APIs return {:error, %Error{code: :parameter_error}}" do
      assert {:error, %Error{code: :parameter_error}} =
               Aerospike.get(:nonexistent, {123, "users", "tuple-key"})

      assert {:error, %Error{code: :parameter_error}} =
               Aerospike.put(:nonexistent, {"", "users", "tuple-key"}, %{"a" => 1})

      assert {:error, %Error{code: :parameter_error}} =
               Aerospike.batch_get(:nonexistent, [{"test", :users, "tuple-key"}])

      assert {:error, %Error{code: :parameter_error}} =
               Aerospike.batch_exists(:nonexistent, [{"test", "users", :bad_key}])
    end

    test "bang APIs raise Aerospike.Error" do
      assert_raise Error, fn ->
        Aerospike.put!(:nonexistent, {123, "users", "tuple-key"}, %{})
      end

      assert_raise Error, fn ->
        Aerospike.batch_get!(:nonexistent, [{123, "users", "tuple-key"}])
      end
    end
  end
end
