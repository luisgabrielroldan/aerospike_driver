defmodule Aerospike.Runtime.TxnOpsTest do
  use ExUnit.Case, async: false

  alias Aerospike.Cluster.Supervisor, as: ClusterSupervisor
  alias Aerospike.Key
  alias Aerospike.Runtime.TxnOps
  alias Aerospike.Transport.Fake
  alias Aerospike.Txn

  setup do
    name = :"txn_ops_test_#{System.unique_integer([:positive])}"
    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])

    {:ok, sup} =
      ClusterSupervisor.start_link(
        name: name,
        transport: Fake,
        hosts: ["10.0.0.1:3000"],
        namespaces: ["test"],
        tend_trigger: :manual,
        connect_opts: [fake: fake]
      )

    on_exit(fn ->
      stop_if_alive(sup)
      stop_if_alive(fake)
    end)

    {:ok, conn: name, txn: Txn.new(), key: Key.new("test", "spike", "txn")}
  end

  test "init_tracking/2 creates an open transaction row", %{conn: conn, txn: txn} do
    assert true = TxnOps.init_tracking(conn, txn)
    assert {:ok, tracking} = TxnOps.get_tracking(conn, txn)
    assert tracking.state == :open
    assert tracking.namespace == nil
    assert tracking.reads == %{}
    assert tracking.writes == MapSet.new()
  end

  test "track_read/4 and track_write/5 store versions", %{conn: conn, txn: txn, key: key} do
    assert true = TxnOps.init_tracking(conn, txn)
    assert true = TxnOps.track_read(conn, txn, key, 42)
    assert true = TxnOps.track_write(conn, txn, key, 42, :ok)

    assert TxnOps.get_read_version(conn, txn, key) == 42
    assert TxnOps.get_writes(conn, txn) == []
  end

  test "nil read versions and non-ok nil writes are treated as no-ops", %{
    conn: conn,
    txn: txn,
    key: key
  } do
    assert true = TxnOps.init_tracking(conn, txn)
    assert true = TxnOps.track_read(conn, txn, key, nil)
    assert true = TxnOps.track_write(conn, txn, key, nil, :network_error)

    assert TxnOps.get_reads(conn, txn) == %{}
    assert TxnOps.get_writes(conn, txn) == []
  end

  test "track_write/5 and track_write_in_doubt/3 move nil-version writes into the write set", %{
    conn: conn,
    txn: txn,
    key: key
  } do
    assert true = TxnOps.init_tracking(conn, txn)
    assert true = TxnOps.track_read(conn, txn, key, 42)
    assert true = TxnOps.track_write(conn, txn, key, nil, :ok)

    assert TxnOps.get_read_version(conn, txn, key) == nil
    assert TxnOps.write_exists?(conn, txn, key)
    assert TxnOps.get_writes(conn, txn) == [key]

    assert true = TxnOps.track_write_in_doubt(conn, txn, key)
    assert {:ok, tracking} = TxnOps.get_tracking(conn, txn)
    assert tracking.write_in_doubt
  end

  test "set_namespace/3 rejects mismatched namespaces", %{conn: conn, txn: txn} do
    assert true = TxnOps.init_tracking(conn, txn)
    assert :ok = TxnOps.set_namespace(conn, txn, "test")

    assert {:error, %Aerospike.Error{code: :parameter_error}} =
             TxnOps.set_namespace(conn, txn, "other")
  end

  test "set_namespace/3 accepts repeated namespaces and rejects missing transactions", %{
    conn: conn,
    txn: txn
  } do
    assert true = TxnOps.init_tracking(conn, txn)
    assert :ok = TxnOps.set_namespace(conn, txn, "test")
    assert :ok = TxnOps.set_namespace(conn, txn, "test")

    assert {:error, %Aerospike.Error{message: "transaction not initialized"}} =
             TxnOps.set_namespace(conn, Txn.new(), "test")
  end

  test "deadline and monitor helpers reflect tracked state", %{conn: conn, txn: txn} do
    other_txn = Txn.new()

    assert true = TxnOps.init_tracking(conn, txn)
    refute TxnOps.monitor_exists?(conn, txn)
    refute TxnOps.close_monitor?(conn, txn)
    assert TxnOps.get_deadline(conn, other_txn) == 0

    assert true = TxnOps.set_deadline(conn, txn, 1_234)
    assert TxnOps.get_deadline(conn, txn) == 1_234
    assert TxnOps.monitor_exists?(conn, txn)
    assert TxnOps.close_monitor?(conn, txn)

    assert true = TxnOps.set_write_in_doubt(conn, txn)
    refute TxnOps.close_monitor?(conn, txn)
  end

  test "verify_command/2 requires an open initialized transaction", %{conn: conn, txn: txn} do
    other_txn = Txn.new()

    assert true = TxnOps.init_tracking(conn, txn)
    assert :ok = TxnOps.verify_command(conn, txn)

    assert true = TxnOps.set_state(conn, txn, :verified)

    assert {:error, %Aerospike.Error{message: "transaction is not open (state: verified)"}} =
             TxnOps.verify_command(conn, txn)

    assert {:error, %Aerospike.Error{message: "transaction not initialized"}} =
             TxnOps.verify_command(conn, other_txn)
  end

  test "cleanup/2 removes tracking rows", %{conn: conn, txn: txn} do
    assert true = TxnOps.init_tracking(conn, txn)
    assert true = TxnOps.cleanup(conn, txn)
    assert {:error, :not_found} = TxnOps.get_tracking(conn, txn)
  end

  test "init_tracking/2 requires a cluster with published transaction tables" do
    conn = :"txn_ops_missing_#{System.unique_integer([:positive])}"

    assert_raise ArgumentError, ~r/transaction tracking table is unavailable/, fn ->
      TxnOps.init_tracking(conn, Txn.new())
    end
  end

  defp stop_if_alive(pid) do
    if Process.alive?(pid) do
      Process.exit(pid, :shutdown)
    end
  end
end
