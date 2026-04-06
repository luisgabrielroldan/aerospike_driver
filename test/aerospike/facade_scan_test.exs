defmodule Aerospike.FacadeScanTest do
  use ExUnit.Case, async: false

  alias Aerospike.Error
  alias Aerospike.Scan
  alias Aerospike.Tables

  describe "all/3" do
    test "returns max_records_required when max_records is unset (no cluster I/O)" do
      scan = Scan.new("test", "users")

      assert {:error, %Error{code: :max_records_required}} = Aerospike.all(:no_such_conn, scan)
    end
  end

  describe "stream!/3" do
    setup do
      name = :"facade_stream_#{:erlang.unique_integer([:positive])}"
      meta = Tables.meta(name)
      _ = :ets.new(meta, [:set, :public, :named_table])

      on_exit(fn ->
        try do
          :ets.delete(meta)
        catch
          :error, :badarg -> :ok
        end
      end)

      {:ok, conn: name}
    end

    test "returns a Stream (Enumerable)", %{conn: conn} do
      scan = Scan.new("test", "users")
      stream = Aerospike.stream!(conn, scan)

      assert Enumerable.impl_for(stream) != nil
    end
  end
end
