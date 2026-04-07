defmodule Aerospike.BatchTest do
  use ExUnit.Case, async: true

  alias Aerospike.Batch
  alias Aerospike.BatchResult
  alias Aerospike.Key
  alias Aerospike.Op

  setup do
    key = Key.new("ns", "set", "batch-ut")
    {:ok, key: key}
  end

  describe "constructors" do
    test "read/2 stores key and opts", %{key: key} do
      r = Batch.read(key, bins: ["a"])
      assert %Batch.Read{key: ^key, opts: [bins: ["a"]]} = r
    end

    test "put/3 normalizes bin keys to strings", %{key: key} do
      p = Batch.put(key, %{"a" => 1, "b" => 2})
      assert p.bins == %{"a" => 1, "b" => 2}
    end

    test "delete/2", %{key: key} do
      assert %Batch.Delete{key: ^key, opts: []} = Batch.delete(key)
    end

    test "operate/3", %{key: key} do
      ops = [Op.get("x")]
      assert %Batch.Operate{key: ^key, ops: ^ops} = Batch.operate(key, ops)
    end

    test "udf/5 defaults args", %{key: key} do
      u = Batch.udf(key, "pkg", "fn")
      assert %Batch.UDF{package: "pkg", function: "fn", args: []} = u
    end

    test "key/1 returns the operation key", %{key: key} do
      assert Batch.key(Batch.read(key)) == key
      assert Batch.key(Batch.put(key, %{"a" => 1})) == key
      assert Batch.key(Batch.delete(key)) == key
      assert Batch.key(Batch.operate(key, [])) == key
      assert Batch.key(Batch.udf(key, "p", "f")) == key
    end
  end

  describe "tuple key support" do
    test "read/2 accepts tuple key" do
      assert %Batch.Read{key: %Key{}} = Batch.read({"ns", "set", "uk"})
    end

    test "put/3 accepts tuple key" do
      assert %Batch.Put{key: %Key{}, bins: %{"a" => 1}} =
               Batch.put({"ns", "set", "uk"}, %{"a" => 1})
    end

    test "delete/2 accepts tuple key with integer user key" do
      assert %Batch.Delete{key: %Key{}} = Batch.delete({"ns", "set", 42})
    end

    test "operate/3 accepts tuple key" do
      op = Op.get("x")
      assert %Batch.Operate{key: %Key{}, ops: [^op]} = Batch.operate({"ns", "set", "uk"}, [op])
    end

    test "udf/5 accepts tuple key" do
      assert %Batch.UDF{key: %Key{}, package: "pkg", function: "fn"} =
               Batch.udf({"ns", "set", "uk"}, "pkg", "fn")
    end

    test "invalid tuple key raises ArgumentError" do
      assert_raise ArgumentError, fn ->
        Batch.read({123, "set", "uk"})
      end
    end
  end

  describe "BatchResult" do
    test "ok/0" do
      r = BatchResult.ok()
      assert r.status == :ok
      assert r.record == nil
      assert r.error == nil
      assert r.in_doubt == false
    end

    test "ok/1 with record" do
      key = Key.new("n", "s", "x")
      rec = %Aerospike.Record{key: key, bins: %{}, generation: 1, ttl: 0}
      r = BatchResult.ok(rec)
      assert r.status == :ok
      assert r.record == rec
      assert r.error == nil
    end

    test "error/2 default in_doubt" do
      e = Aerospike.Error.from_result_code(:key_not_found)
      r = BatchResult.error(e)
      assert r.status == :error
      assert r.record == nil
      assert r.error == e
      assert r.in_doubt == false
    end

    test "error/2 with in_doubt" do
      e = Aerospike.Error.from_result_code(:timeout)
      r = BatchResult.error(e, true)
      assert r.in_doubt == true
    end
  end
end
