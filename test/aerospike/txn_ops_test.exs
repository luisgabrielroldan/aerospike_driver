defmodule Aerospike.TxnOpsTest do
  use ExUnit.Case, async: false

  alias Aerospike.Key
  alias Aerospike.TableOwner
  alias Aerospike.Txn
  alias Aerospike.TxnOps

  setup do
    name = :"txn_ops_test_#{:erlang.unique_integer([:positive])}"
    {:ok, pid} = TableOwner.start_link(name: name)

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5_000)
    end)

    txn = Txn.new()
    key1 = Key.new("test", "set1", "k1")
    key2 = Key.new("test", "set1", "k2")

    {:ok, conn: name, txn: txn, key1: key1, key2: key2}
  end

  describe "init_tracking/2" do
    test "inserts empty tracking entry", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      assert {:ok, tracking} = TxnOps.get_tracking(conn, txn)
      assert tracking.state == :open
      assert tracking.namespace == nil
      assert tracking.deadline == 0
      assert tracking.reads == %{}
      assert tracking.writes == MapSet.new()
      assert tracking.write_in_doubt == false
    end
  end

  describe "get_tracking/2" do
    test "returns :not_found for unknown txn", %{conn: conn} do
      txn = Txn.new()
      assert {:error, :not_found} = TxnOps.get_tracking(conn, txn)
    end
  end

  describe "track_read/4" do
    test "stores version for key when version is non-nil", %{conn: conn, txn: txn, key1: key1} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.track_read(conn, txn, key1, 42)
      assert {:ok, t} = TxnOps.get_tracking(conn, txn)
      assert t.reads[key1] == 42
    end

    test "does not store when version is nil", %{conn: conn, txn: txn, key1: key1} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.track_read(conn, txn, key1, nil)
      assert {:ok, t} = TxnOps.get_tracking(conn, txn)
      assert t.reads == %{}
    end
  end

  describe "track_write/5" do
    test "stores version in reads map when version is non-nil", %{
      conn: conn,
      txn: txn,
      key1: key1
    } do
      TxnOps.init_tracking(conn, txn)
      TxnOps.track_write(conn, txn, key1, 99, :ok)
      assert {:ok, t} = TxnOps.get_tracking(conn, txn)
      assert t.reads[key1] == 99
      assert MapSet.size(t.writes) == 0
    end

    test "moves key from reads to writes on :ok with nil version", %{
      conn: conn,
      txn: txn,
      key1: key1
    } do
      TxnOps.init_tracking(conn, txn)
      TxnOps.track_read(conn, txn, key1, 10)
      TxnOps.track_write(conn, txn, key1, nil, :ok)
      assert {:ok, t} = TxnOps.get_tracking(conn, txn)
      refute Map.has_key?(t.reads, key1)
      assert MapSet.member?(t.writes, key1)
    end

    test "does nothing on non-:ok result_code with nil version", %{
      conn: conn,
      txn: txn,
      key1: key1
    } do
      TxnOps.init_tracking(conn, txn)
      TxnOps.track_write(conn, txn, key1, nil, :key_not_found)
      assert {:ok, t} = TxnOps.get_tracking(conn, txn)
      assert t.reads == %{}
      assert MapSet.size(t.writes) == 0
    end
  end

  describe "track_write_in_doubt/3" do
    test "sets write_in_doubt, removes from reads, adds to writes", %{
      conn: conn,
      txn: txn,
      key1: key1
    } do
      TxnOps.init_tracking(conn, txn)
      TxnOps.track_read(conn, txn, key1, 5)
      TxnOps.track_write_in_doubt(conn, txn, key1)
      assert {:ok, t} = TxnOps.get_tracking(conn, txn)
      assert t.write_in_doubt == true
      refute Map.has_key?(t.reads, key1)
      assert MapSet.member?(t.writes, key1)
    end
  end

  describe "get_reads/2 and get_writes/2" do
    test "returns reads map", %{conn: conn, txn: txn, key1: key1, key2: key2} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.track_read(conn, txn, key1, 1)
      TxnOps.track_read(conn, txn, key2, 2)
      reads = TxnOps.get_reads(conn, txn)
      assert reads[key1] == 1
      assert reads[key2] == 2
    end

    test "returns writes list", %{conn: conn, txn: txn, key1: key1} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.track_write(conn, txn, key1, nil, :ok)
      writes = TxnOps.get_writes(conn, txn)
      assert key1 in writes
    end
  end

  describe "set_state/3" do
    test "transitions state", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_state(conn, txn, :verified)
      assert {:ok, %{state: :verified}} = TxnOps.get_tracking(conn, txn)
      TxnOps.set_state(conn, txn, :committed)
      assert {:ok, %{state: :committed}} = TxnOps.get_tracking(conn, txn)
    end
  end

  describe "set_namespace/3" do
    test "sets namespace on first call", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      assert :ok = TxnOps.set_namespace(conn, txn, "test")
      assert {:ok, %{namespace: "test"}} = TxnOps.get_tracking(conn, txn)
    end

    test "is idempotent for same namespace", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      assert :ok = TxnOps.set_namespace(conn, txn, "test")
      assert :ok = TxnOps.set_namespace(conn, txn, "test")
    end

    test "returns error on namespace mismatch", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_namespace(conn, txn, "test")
      assert {:error, _} = TxnOps.set_namespace(conn, txn, "other")
    end

    test "returns error when txn not initialized", %{conn: conn} do
      txn = Txn.new()
      assert {:error, _} = TxnOps.set_namespace(conn, txn, "test")
    end
  end

  describe "deadline functions" do
    test "get/set deadline", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      assert TxnOps.get_deadline(conn, txn) == 0
      TxnOps.set_deadline(conn, txn, 12_345)
      assert TxnOps.get_deadline(conn, txn) == 12_345
    end

    test "monitor_exists? is false when deadline is 0", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      refute TxnOps.monitor_exists?(conn, txn)
    end

    test "monitor_exists? is true when deadline is non-zero", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_deadline(conn, txn, 999)
      assert TxnOps.monitor_exists?(conn, txn)
    end

    test "close_monitor? is false when deadline is 0", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      refute TxnOps.close_monitor?(conn, txn)
    end

    test "close_monitor? is true when deadline non-zero and write_in_doubt false", %{
      conn: conn,
      txn: txn
    } do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_deadline(conn, txn, 100)
      assert TxnOps.close_monitor?(conn, txn)
    end

    test "close_monitor? is false when write_in_doubt is true", %{
      conn: conn,
      txn: txn,
      key1: key1
    } do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_deadline(conn, txn, 100)
      TxnOps.track_write_in_doubt(conn, txn, key1)
      refute TxnOps.close_monitor?(conn, txn)
    end
  end

  describe "write_exists?/3" do
    test "returns false when key not in writes", %{conn: conn, txn: txn, key1: key1} do
      TxnOps.init_tracking(conn, txn)
      refute TxnOps.write_exists?(conn, txn, key1)
    end

    test "returns true after track_write", %{conn: conn, txn: txn, key1: key1} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.track_write(conn, txn, key1, nil, :ok)
      assert TxnOps.write_exists?(conn, txn, key1)
    end
  end

  describe "get_read_version/3" do
    test "returns nil for unknown key", %{conn: conn, txn: txn, key1: key1} do
      TxnOps.init_tracking(conn, txn)
      assert TxnOps.get_read_version(conn, txn, key1) == nil
    end

    test "returns stored version", %{conn: conn, txn: txn, key1: key1} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.track_read(conn, txn, key1, 77)
      assert TxnOps.get_read_version(conn, txn, key1) == 77
    end
  end

  describe "cleanup/2" do
    test "removes the tracking entry", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.cleanup(conn, txn)
      assert {:error, :not_found} = TxnOps.get_tracking(conn, txn)
    end
  end

  describe "verify_command/2" do
    test "returns :ok when state is :open", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      assert :ok = TxnOps.verify_command(conn, txn)
    end

    test "returns error when state is :committed", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_state(conn, txn, :committed)
      assert {:error, _} = TxnOps.verify_command(conn, txn)
    end

    test "returns error when state is :aborted", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_state(conn, txn, :aborted)
      assert {:error, _} = TxnOps.verify_command(conn, txn)
    end

    test "returns error when txn not initialized", %{conn: conn} do
      txn = Txn.new()
      assert {:error, _} = TxnOps.verify_command(conn, txn)
    end
  end
end
