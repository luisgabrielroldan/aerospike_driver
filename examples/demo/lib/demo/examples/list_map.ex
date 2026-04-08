defmodule Demo.Examples.ListMap do
  @moduledoc """
  Demonstrates list and map CDT operations: string lists, complex lists, string maps,
  complex maps, nested list/map combinations, and list operate with policies.

  """

  require Logger

  alias Aerospike.Op

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo"

  def run do
    test_list_strings()
    test_list_complex()
    test_map_strings()
    test_map_complex()
    test_list_map_combined()
    test_list_operate()
  end

  # Write/Read string list
  defp test_list_strings do
    Logger.info("  Read/Write string list")
    key = Aerospike.key(@namespace, @set, "listkey1")
    @repo.delete(key)

    list = ["string1", "string2", "string3"]
    :ok = @repo.put!(key, %{"listbin1" => list})

    {:ok, record} = @repo.get(key)
    received = record.bins["listbin1"]

    validate_size(3, length(received))
    validate("string1", Enum.at(received, 0))
    validate("string2", Enum.at(received, 1))
    validate("string3", Enum.at(received, 2))

    Logger.info("  Read/Write string list successful.")
    @repo.delete(key)
  end

  # Write/Read complex list (mixed types)
  defp test_list_complex do
    Logger.info("  Read/Write complex list")
    key = Aerospike.key(@namespace, @set, "listkey2")
    @repo.delete(key)

    # Opaque bytes on write use `{:bytes, binary}`; reads surface as `{:blob, binary}`.
    list = ["string1", 2, {:bytes, <<3, 52, 125>>}]
    :ok = @repo.put!(key, %{"listbin2" => list})

    {:ok, record} = @repo.get(key)
    received = record.bins["listbin2"]

    validate_size(3, length(received))
    validate("string1", Enum.at(received, 0))
    validate(2, Enum.at(received, 1))
    # Blob comes back as {:blob, bytes}
    Logger.info("  Read/Write complex list successful.")
    @repo.delete(key)
  end

  # Write/Read string map
  defp test_map_strings do
    Logger.info("  Read/Write string map")
    key = Aerospike.key(@namespace, @set, "mapkey1")
    @repo.delete(key)

    map = %{"key1" => "string1", "key2" => "string2", "key3" => "string3"}
    :ok = @repo.put!(key, %{"mapbin1" => map})

    {:ok, record} = @repo.get(key)
    received = record.bins["mapbin1"]

    validate_size(3, map_size(received))
    validate("string1", received["key1"])
    validate("string2", received["key2"])
    validate("string3", received["key3"])

    Logger.info("  Read/Write string map successful.")
    @repo.delete(key)
  end

  # Write/Read complex map (mixed key/value types)
  defp test_map_complex do
    Logger.info("  Read/Write complex map")
    key = Aerospike.key(@namespace, @set, "mapkey2")
    @repo.delete(key)

    inner_list = [100_034, 12_384_955, 3, 512]

    map = %{
      "key1" => "string1",
      "key2" => 2,
      "key4" => inner_list
    }

    :ok = @repo.put!(key, %{"mapbin2" => map})

    {:ok, record} = @repo.get(key)
    received = record.bins["mapbin2"]

    validate("string1", received["key1"])
    validate(2, received["key2"])

    received_inner = received["key4"]
    validate_size(4, length(received_inner))
    validate(100_034, Enum.at(received_inner, 0))
    validate(12_384_955, Enum.at(received_inner, 1))
    validate(3, Enum.at(received_inner, 2))
    validate(512, Enum.at(received_inner, 3))

    Logger.info("  Read/Write complex map successful.")
    @repo.delete(key)
  end

  # Write/Read combined list/map structures
  defp test_list_map_combined do
    Logger.info("  Read/Write Array/Map combined")
    key = Aerospike.key(@namespace, @set, "listmapkey")
    @repo.delete(key)

    inner = ["string2", 5]

    inner_map = %{
      "a" => 1,
      "list" => inner
    }

    list = ["string1", 8, inner, inner_map]

    :ok = @repo.put!(key, %{"listmapbin" => list})

    {:ok, record} = @repo.get(key)
    received = record.bins["listmapbin"]

    validate_size(4, length(received))
    validate("string1", Enum.at(received, 0))
    validate(8, Enum.at(received, 1))

    received_inner = Enum.at(received, 2)
    validate_size(2, length(received_inner))
    validate("string2", Enum.at(received_inner, 0))
    validate(5, Enum.at(received_inner, 1))

    received_map = Enum.at(received, 3)
    validate(1, received_map["a"])

    received_inner2 = received_map["list"]
    validate_size(2, length(received_inner2))
    validate("string2", Enum.at(received_inner2, 0))
    validate(5, Enum.at(received_inner2, 1))

    Logger.info("  Read/Write Array/Map combined successful.")
    @repo.delete(key)
  end

  # Write/Read a single item into a list using the operate command
  defp test_list_operate do
    Logger.info("  Read/Write List operate")
    key = Aerospike.key(@namespace, @set, "listopkey1")
    @repo.delete(key)

    # Create a list as a bin
    list = ["string1", "string2", "string3"]
    :ok = @repo.put!(key, %{"listbin1" => list})

    # Append a unique item via operate
    {:ok, _} =
      @repo.operate(key, [
        Op.List.append("listbin1", "string4", policy: %{order: 0, flags: 0})
      ])

    # Append duplicate — should succeed with default flags (no unique constraint)
    {:ok, _} =
      @repo.operate(key, [
        Op.List.append("listbin1", "string5")
      ])

    {:ok, record} = @repo.get(key)
    received_list = record.bins["listbin1"]

    validate_size(5, length(received_list))
    validate("string1", Enum.at(received_list, 0))
    validate("string2", Enum.at(received_list, 1))
    validate("string3", Enum.at(received_list, 2))
    validate("string4", Enum.at(received_list, 3))
    validate("string5", Enum.at(received_list, 4))

    Logger.info("  Read/Write list operate successful.")
    @repo.delete(key)
  end

  defp validate_size(expected, received) do
    unless received == expected do
      raise "Size mismatch: expected=#{expected} received=#{received}"
    end
  end

  defp validate(expected, received) do
    unless received == expected do
      raise "Mismatch: expected=#{inspect(expected)} received=#{inspect(received)}"
    end
  end
end
