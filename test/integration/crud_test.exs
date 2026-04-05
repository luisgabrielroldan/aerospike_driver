defmodule Aerospike.Integration.CRUDTest do
  use Aerospike.Test.AerospikeCase

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Test.Helpers

  test "put then get returns bins, generation, ttl", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key()
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    bins = %{"name" => "Alice", "n" => 7}
    assert {:ok, conn} = Helpers.put(conn, key, bins)

    assert {:ok, _conn, record} = Helpers.get(conn, key)
    assert record.bins["name"] == "Alice"
    assert record.bins["n"] == 7
    assert record.generation >= 1
    assert record.ttl >= 0
    assert record.key == key
  end

  test "delete then get returns key_not_found", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key()
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert {:ok, conn} = Helpers.put(conn, key, %{"x" => 1})
    assert {:ok, conn, true} = Helpers.delete(conn, key)
    assert {:error, %Error{code: :key_not_found}} = Helpers.get(conn, key)
  end

  test "delete on missing key returns false", %{conn: conn} do
    suffix = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    key = Key.new("test", "crud_int", "missing-#{suffix}")
    assert {:ok, _conn, false} = Helpers.delete(conn, key)
  end

  test "exists true and false", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key()
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert {:ok, conn, false} = Helpers.exists(conn, key)
    assert {:ok, conn} = Helpers.put(conn, key, %{"a" => 1})
    assert {:ok, _conn, true} = Helpers.exists(conn, key)
  end

  test "touch updates record (generation or readable get)", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key()
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert {:ok, conn} = Helpers.put(conn, key, %{"a" => 1})
    assert {:ok, conn, before} = Helpers.get(conn, key)
    assert {:ok, conn} = Helpers.touch(conn, key)
    assert {:ok, _conn, after_rec} = Helpers.get(conn, key)
    assert after_rec.generation >= before.generation
  end

  test "multiple bin types round-trip", %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key()
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    bins = %{
      "s" => "text",
      "i" => 42,
      "f" => 2.5,
      "b" => true,
      "z" => nil
    }

    assert {:ok, conn} = Helpers.put(conn, key, bins)
    assert {:ok, _conn, record} = Helpers.get(conn, key)
    assert record.bins["s"] == "text"
    assert record.bins["i"] == 42
    assert record.bins["f"] == 2.5
    assert record.bins["b"] == true
    assert record.bins["z"] == nil
  end
end
