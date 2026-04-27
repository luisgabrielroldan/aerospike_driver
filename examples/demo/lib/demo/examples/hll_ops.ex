defmodule Demo.Examples.HllOps do
  @moduledoc """
  Demonstrates HyperLogLog operations via `Aerospike.Op.HLL`:
  init, add, get_count, describe, and union operations.

  HLL provides probabilistic cardinality estimation with low memory usage.
  """

  require Logger

  alias Aerospike.Op

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_hll"

  def run do
    init_and_add()
    describe_hll()
    union_count()
    cleanup()
  end

  defp init_and_add do
    key = key("basic")
    @repo.delete(key)

    rec =
      @repo.operate!(key, [
        Op.HLL.add("visitors", ["user_1", "user_2", "user_3", "user_4", "user_5"], 10)
      ])

    updates = rec.bins["visitors"]
    Logger.info("  HLL add 5 unique elements: #{updates} register updates")

    rec2 =
      @repo.operate!(key, [
        Op.HLL.add("visitors", ["user_3", "user_4", "user_6"], 10)
      ])

    updates2 = rec2.bins["visitors"]
    Logger.info("  HLL add 3 elements (2 duplicates): #{updates2} new register updates")

    rec3 =
      @repo.operate!(key, [
        Op.HLL.get_count("visitors")
      ])

    count = rec3.bins["visitors"]
    Logger.info("  Estimated cardinality: #{count} (actual: 6)")

    unless count >= 4 and count <= 8 do
      raise "HLL estimate #{count} too far from actual 6"
    end
  end

  defp describe_hll do
    key = key("basic")

    rec =
      @repo.operate!(key, [
        Op.HLL.describe("visitors")
      ])

    [index_bits, min_hash_bits] = rec.bins["visitors"]
    Logger.info("  HLL describe: index_bits=#{index_bits}, min_hash_bits=#{min_hash_bits}")
  end

  defp union_count do
    key_a = key("set_a")
    key_b = key("set_b")
    @repo.delete(key_a)
    @repo.delete(key_b)

    @repo.operate!(key_a, [
      Op.HLL.add("hll", ["apple", "banana", "cherry"], 8)
    ])

    @repo.operate!(key_b, [
      Op.HLL.add("hll", ["cherry", "date", "elderberry"], 8)
    ])

    # HLL bins come back as {:raw, 18, binary} — extract the binary and wrap as {:bytes, ...}
    {:ok, rec_b} = @repo.get(key_b)
    {:raw, _particle_type, hll_b_bytes} = rec_b.bins["hll"]

    rec =
      @repo.operate!(key_a, [
        Op.HLL.get_union_count("hll", [{:bytes, hll_b_bytes}])
      ])

    union_est = rec.bins["hll"]
    Logger.info("  Union cardinality: #{union_est} (actual: 5, sets share 'cherry')")

    rec2 =
      @repo.operate!(key_a, [
        Op.HLL.get_intersect_count("hll", [{:bytes, hll_b_bytes}])
      ])

    intersect_est = rec2.bins["hll"]
    Logger.info("  Intersection cardinality: #{intersect_est} (actual: 1)")
  end

  defp cleanup do
    for id <- ["basic", "set_a", "set_b"] do
      @repo.delete(key(id))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
