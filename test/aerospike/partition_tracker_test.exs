defmodule Aerospike.Command.PartitionTrackerTest do
  use ExUnit.Case, async: true

  alias Aerospike.Command.NodePartitions
  alias Aerospike.Command.PartitionStatus
  alias Aerospike.Command.PartitionTracker
  alias Aerospike.Cursor
  alias Aerospike.Error
  alias Aerospike.PartitionFilter

  describe "new/2" do
    test "builds tracker state from a partition filter and resets runtime fields" do
      filter = %PartitionFilter{
        begin: 3,
        count: 2,
        digest: <<1::160>>,
        partitions: [%{id: 9, digest: <<2::160>>, bval: 12}],
        done?: false,
        retry?: false
      }

      tracker = PartitionTracker.new(filter, nodes: ["A1", "B1"], max_records: 25)

      assert tracker.partition_begin == 3
      assert tracker.partitions_capacity == 2
      assert tracker.node_capacity == 2
      assert tracker.max_records == 25

      assert tracker.partitions == [
               %PartitionStatus{
                 id: 9,
                 digest: <<2::160>>,
                 bval: 12,
                 sequence: 0,
                 retry?: true,
                 node: nil
               }
             ]

      assert tracker.partition_filter.partitions == [%{id: 9, digest: <<2::160>>, bval: 12}]
      assert tracker.partition_filter.retry? == true
      assert tracker.partition_filter.done? == false
    end

    test "validates nodes, max_records, replica, and node_filter inputs" do
      filter = PartitionFilter.by_id(0)

      assert_raise ArgumentError, ~r/nodes list must not be empty/, fn ->
        PartitionTracker.new(filter, nodes: [])
      end

      assert_raise ArgumentError, ~r/max_records must be >= 0/, fn ->
        PartitionTracker.new(filter, nodes: ["A1"], max_records: -1)
      end

      assert_raise ArgumentError, ~r/replica must be :master, :sequence, or :any/, fn ->
        PartitionTracker.new(filter, nodes: ["A1"], replica: :bogus)
      end

      assert_raise ArgumentError, ~r/node_filter must be a binary string or nil/, fn ->
        PartitionTracker.new(filter, nodes: ["A1"], node_filter: 123)
      end
    end
  end

  describe "route_partition/3" do
    test "selects the master or sequence replica from a routing snapshot" do
      status = %PartitionStatus{id: 5}
      map = %{5 => [nil, "B1", "C1"]}

      assert {:ok, "B1"} = PartitionTracker.route_partition(status, :master, map)
      assert {:ok, "B1"} = PartitionTracker.route_partition(status, :sequence, map)
    end

    test "fails when the partition is missing or has no live replicas" do
      status = %PartitionStatus{id: 6}

      assert {:error, %Error{code: :partition_unavailable}} =
               PartitionTracker.route_partition(status, :sequence, %{})

      assert {:error, %Error{code: :partition_unavailable}} =
               PartitionTracker.route_partition(status, :sequence, %{6 => [nil, nil]})
    end
  end

  describe "assign_partitions_to_nodes/2" do
    test "groups partitions by node and keeps one tracker-owned node bucket per node" do
      tracker = PartitionTracker.new(PartitionFilter.by_range(0, 3), nodes: ["A1", "B1", "C1"])

      partition_map = %{
        0 => ["A1", "B1"],
        1 => [nil, "B1", "C1"],
        2 => ["C1"]
      }

      assert {:ok, updated, groups} =
               PartitionTracker.assign_partitions_to_nodes(tracker, partition_map)

      assert length(groups) == 3
      assert Enum.map(groups, & &1.node) |> Enum.sort() == ["A1", "B1", "C1"]
      assert updated.node_partitions_list == groups
    end

    test "fails when every routed partition is filtered out by a node constraint" do
      tracker =
        PartitionTracker.new(PartitionFilter.by_id(0), nodes: ["A1"], node_filter: "Z9")

      assert {:error, %Error{code: :invalid_node}} =
               PartitionTracker.assign_partitions_to_nodes(tracker, %{0 => ["A1"]})
    end
  end

  describe "retry and cursor ownership" do
    test "advances sequence on retryable errors and exports the current cursor from runtime state" do
      tracker = PartitionTracker.new(PartitionFilter.by_id(0), nodes: ["A1", "B1"])
      np = NodePartitions.add_partition(NodePartitions.new("A1"), %PartitionStatus{id: 0})

      {tracker, np} = PartitionTracker.partition_unavailable(tracker, np, 0)

      assert [%PartitionStatus{sequence: 1, retry?: true}] = tracker.partitions
      assert np.parts_unavailable == 1

      err = Error.from_result_code(:timeout, message: "node timed out")
      assert {true, tracker, np} = PartitionTracker.should_retry?(tracker, np, err)
      assert tracker.exceptions == [err]
      assert np.parts_unavailable == 1

      assert %Cursor{partitions: [%{id: 0, digest: nil, bval: nil}]} =
               PartitionTracker.cursor(tracker)
    end

    test "records digests and bvals in the cursor projection" do
      tracker = PartitionTracker.new(PartitionFilter.by_id(12), nodes: ["A1"])
      np = NodePartitions.new("A1")

      {tracker, _np} = PartitionTracker.set_last(tracker, np, 12, {<<3::160>>, 44})

      assert %Cursor{partitions: [%{id: 12, digest: <<3::160>>, bval: 44}]} =
               PartitionTracker.cursor(tracker)
    end

    test "cursor returns nil when the tracker has no filter and sleep reports the retry delay" do
      tracker =
        PartitionTracker.new(PartitionFilter.by_id(1), nodes: ["A1"], sleep_between_retries: 17)

      assert nil == PartitionTracker.cursor(%{tracker | partition_filter: nil})
      assert 17 == PartitionTracker.should_sleep_for(tracker)
    end
  end

  describe "completion" do
    test "marks a finished tracker as complete and clears the cursor" do
      tracker = PartitionTracker.new(PartitionFilter.by_id(0), nodes: ["A1"])

      assert {:complete, %PartitionFilter{done?: true, retry?: false}, updated} =
               PartitionTracker.is_complete?(tracker, true)

      assert PartitionTracker.cursor(updated) == nil
      assert updated.partition_filter.done? == true
    end

    test "keeps retry metadata when a later iteration completes without unavailable partitions" do
      tracker =
        PartitionTracker.new(PartitionFilter.by_id(0), nodes: ["A1"], max_records: 1)
        |> Map.put(:iteration, 2)

      assert {:complete, %PartitionFilter{done?: false, retry?: true}, _updated} =
               PartitionTracker.is_complete?(tracker, true)
    end

    test "completes without marking done when partition queries are unavailable" do
      tracker = PartitionTracker.new(PartitionFilter.by_id(0), nodes: ["A1"], max_records: 1)

      assert {:complete, %PartitionFilter{done?: false, retry?: false}, _updated} =
               PartitionTracker.is_complete?(tracker, false)
    end

    test "returns max_retries_exceeded with sub-errors once retries are exhausted" do
      err = Error.from_result_code(:timeout, message: "node timed out")

      tracker = %PartitionTracker{
        PartitionTracker.new(PartitionFilter.by_id(0), nodes: ["A1"])
        | iteration: 3,
          max_retries: 2,
          node_partitions_list: [%NodePartitions{NodePartitions.new("A1") | parts_unavailable: 1}],
          exceptions: [err]
      }

      assert {:error, %Error{code: :max_retries_exceeded, message: message}, _tracker} =
               PartitionTracker.is_complete?(tracker, true)

      assert message =~ "Max retries exceeded: 2"
      assert message =~ "timeout: node timed out"
    end

    test "returns timeout once the total timeout budget has expired" do
      tracker = %PartitionTracker{
        PartitionTracker.new(PartitionFilter.by_id(0), nodes: ["A1"], total_timeout: 1)
        | node_partitions_list: [%NodePartitions{NodePartitions.new("A1") | parts_unavailable: 1}],
          sleep_between_retries: 5,
          deadline_mono_ms: System.monotonic_time(:millisecond)
      }

      assert {:error, %Error{code: :timeout, message: "Total timeout exceeded"}, _tracker} =
               PartitionTracker.is_complete?(tracker, true)
    end
  end
end
