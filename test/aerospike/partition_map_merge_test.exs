defmodule Aerospike.PartitionMapMergeTest do
  @moduledoc """
  Seam tests for `Aerospike.PartitionMapMerge`. Exercise the pure
  partition-map accumulation (regime guard, idempotent equal-regime
  overwrite, namespace allow-list) and the cluster-stable agreement check
  directly against an ad-hoc owners table, without starting a Tender or
  Writer.
  """

  use ExUnit.Case, async: true

  alias Aerospike.PartitionMap
  alias Aerospike.PartitionMapMerge

  setup do
    owners = :ets.new(:merge_test_owners, [:set, :public])
    on_exit(fn -> if :ets.info(owners) != :undefined, do: :ets.delete(owners) end)
    %{owners: owners}
  end

  describe "apply_segments/4 — regime guard" do
    test "accepts a fresh segment at regime N", %{owners: owners} do
      segments = [{"test", 3, [{0, 0}, {1, 0}, {4095, 0}]}]

      assert PartitionMapMerge.apply_segments(owners, "A1", segments, ["test"])

      assert {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.regime == 3
      assert po.replicas == ["A1"]
    end

    test "equal regime overwrites idempotently", %{owners: owners} do
      segments = [{"test", 3, [{0, 0}]}]

      assert PartitionMapMerge.apply_segments(owners, "A1", segments, ["test"])
      # Same node, same regime — replicas stay `["A1"]`.
      assert PartitionMapMerge.apply_segments(owners, "A1", segments, ["test"])

      assert {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.regime == 3
      assert po.replicas == ["A1"]
    end

    test "strictly lower regime is rejected and leaves the entry untouched",
         %{owners: owners} do
      fresh = [{"test", 5, [{0, 0}]}]
      stale = [{"test", 4, [{0, 0}]}]

      assert PartitionMapMerge.apply_segments(owners, "A1", fresh, ["test"])
      # A rejected segment causes apply_segments/4 to return false. The
      # stored entry must still show the fresh regime.
      refute PartitionMapMerge.apply_segments(owners, "A1", stale, ["test"])

      assert {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.regime == 5
      assert po.replicas == ["A1"]
    end

    test "strictly higher regime replaces the prior entry", %{owners: owners} do
      older = [{"test", 2, [{0, 0}]}]
      newer = [{"test", 4, [{0, 0}]}]

      assert PartitionMapMerge.apply_segments(owners, "A1", older, ["test"])
      assert PartitionMapMerge.apply_segments(owners, "A1", newer, ["test"])

      assert {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.regime == 4
    end
  end

  describe "apply_segments/4 — multi-replica slot placement" do
    test "places node_name at the advertised replica index", %{owners: owners} do
      # Node reports itself as replica index 1 (prole) for partition 0.
      segments = [{"test", 1, [{0, 1}]}]

      assert PartitionMapMerge.apply_segments(owners, "A1", segments, ["test"])

      {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.replicas == [nil, "A1"]
    end

    test "equal-regime segments from different nodes accumulate into slots",
         %{owners: owners} do
      master = [{"test", 1, [{0, 0}]}]
      prole = [{"test", 1, [{0, 1}]}]

      assert PartitionMapMerge.apply_segments(owners, "A1", master, ["test"])
      assert PartitionMapMerge.apply_segments(owners, "B1", prole, ["test"])

      {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.replicas == ["A1", "B1"]
    end
  end

  describe "apply_segments/4 — namespace allow-list" do
    test "segments for unknown namespaces are silently dropped", %{owners: owners} do
      segments = [{"unknown_ns", 1, [{0, 0}]}]

      # Returns `true` — the unknown namespace is skipped as a no-op, not
      # treated as a regime-guard rejection.
      assert PartitionMapMerge.apply_segments(owners, "A1", segments, ["test"])

      assert {:error, :unknown_partition} =
               PartitionMap.owners(owners, "unknown_ns", 0)
    end

    test "mixed in-scope and out-of-scope segments apply only the in-scope half",
         %{owners: owners} do
      segments = [
        {"test", 1, [{0, 0}]},
        {"other", 1, [{0, 0}]}
      ]

      assert PartitionMapMerge.apply_segments(owners, "A1", segments, ["test"])

      assert {:ok, _po} = PartitionMap.owners(owners, "test", 0)
      assert {:error, :unknown_partition} = PartitionMap.owners(owners, "other", 0)
    end
  end

  describe "verify_cluster_stable/1" do
    test "returns {:ok, hash} when every contributor agrees" do
      contributors = %{"A1" => "deadbeef", "B1" => "deadbeef"}

      assert {:ok, "deadbeef"} = PartitionMapMerge.verify_cluster_stable(contributors)
    end

    test "a single contributor still returns the hash" do
      assert {:ok, "cafef00d"} = PartitionMapMerge.verify_cluster_stable(%{"A1" => "cafef00d"})
    end

    test "returns {:ok, :empty} when no node reported a hash" do
      assert {:ok, :empty} = PartitionMapMerge.verify_cluster_stable(%{})
    end

    test "returns a disagreement error carrying the conflicting map" do
      contributors = %{"A1" => "deadbeef", "B1" => "cafef00d"}

      assert {:error, :disagreement, ^contributors} =
               PartitionMapMerge.verify_cluster_stable(contributors)
    end
  end
end
