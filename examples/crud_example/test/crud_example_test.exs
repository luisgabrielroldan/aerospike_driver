defmodule CrudExampleTest do
  use ExUnit.Case

  @namespace "test"
  @set "people"

  test "put, get, exists, delete lifecycle" do
    key = Aerospike.key(@namespace, @set, "test:crud:1")
    bins = %{"name" => "Test", "value" => 123}

    :ok = Aerospike.put!(:aero, key, bins)

    {:ok, record} = Aerospike.get(:aero, key)
    assert record.bins["name"] == "Test"
    assert record.bins["value"] == 123

    {:ok, true} = Aerospike.exists(:aero, key)

    {:ok, true} = Aerospike.delete(:aero, key)
    {:ok, false} = Aerospike.exists(:aero, key)
  end

  test "put with TTL and touch" do
    key = Aerospike.key(@namespace, @set, "test:crud:2")

    :ok = Aerospike.put!(:aero, key, %{"x" => 1}, ttl: 300)

    {:ok, record} = Aerospike.get(:aero, key)
    assert record.ttl > 0

    :ok = Aerospike.touch!(:aero, key, ttl: 600)

    {:ok, touched} = Aerospike.get(:aero, key, header_only: true)
    assert touched.ttl > record.ttl

    Aerospike.delete!(:aero, key)
  end

  test "get with bin selection" do
    key = Aerospike.key(@namespace, @set, "test:crud:3")
    :ok = Aerospike.put!(:aero, key, %{"a" => 1, "b" => 2, "c" => 3})

    {:ok, record} = Aerospike.get(:aero, key, bins: ["a", "c"])
    assert Map.has_key?(record.bins, "a")
    assert Map.has_key?(record.bins, "c")
    refute Map.has_key?(record.bins, "b")

    Aerospike.delete!(:aero, key)
  end
end
