defmodule Aerospike.TxnSupportTest do
  use ExUnit.Case, async: false

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Supervisor, as: ClusterSupervisor
  alias Aerospike.Transport.Fake
  alias Aerospike.Txn
  alias Aerospike.TxnOps
  alias Aerospike.TxnSupport

  setup do
    name = :"txn_support_test_#{System.unique_integer([:positive])}"
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

    txn = Txn.new(timeout: 5_000)
    key = Key.new("test", "spike", "txn")

    on_exit(fn ->
      stop_if_alive(sup)
      stop_if_alive(fake)
    end)

    {:ok, conn: name, txn: txn, key: key}
  end

  test "txn_from_opts validates the optional txn handle", %{txn: txn} do
    assert {:ok, nil} = TxnSupport.txn_from_opts([])
    assert {:ok, ^txn} = TxnSupport.txn_from_opts(txn: txn)

    assert {:error, %Error{code: :invalid_argument, message: message}} =
             TxnSupport.txn_from_opts(txn: :bad)

    assert message =~ "%Aerospike.Txn{}"
  end

  test "maybe_add_mrt_fields appends transaction fields for tracked writes", %{
    conn: conn,
    txn: txn,
    key: key
  } do
    TxnOps.init_tracking(conn, txn)
    TxnOps.set_namespace(conn, txn, key.namespace)
    TxnOps.set_deadline(conn, txn, 123)
    TxnOps.track_read(conn, txn, key, 99)

    msg = TxnSupport.maybe_add_mrt_fields(%AsmMsg{fields: []}, conn, key, [txn: txn], true)
    types = Enum.map(msg.fields, & &1.type)

    assert Field.type_mrt_id() in types
    assert Field.type_record_version() in types
    assert Field.type_mrt_deadline() in types
  end

  test "prepare_txn_read sets namespace for initialized transactions", %{
    conn: conn,
    txn: txn,
    key: key
  } do
    TxnOps.init_tracking(conn, txn)

    assert :ok = TxnSupport.prepare_txn_read(conn, txn, key)
    assert {:ok, %{namespace: "test"}} = TxnOps.get_tracking(conn, txn)
  end

  test "track_txn_response and track_txn_in_doubt update tracking tables", %{
    conn: conn,
    txn: txn,
    key: key
  } do
    TxnOps.init_tracking(conn, txn)

    read_msg = %AsmMsg{fields: [Field.record_version(77)]}
    write_msg = %AsmMsg{fields: []}

    assert :ok = TxnSupport.track_txn_response(conn, txn, key, :read, read_msg, {:ok, :ignored})
    assert TxnOps.get_read_version(conn, txn, key) == 77

    assert :ok = TxnSupport.track_txn_response(conn, txn, key, :write, write_msg, {:ok, :ok})
    assert key in TxnOps.get_writes(conn, txn)

    assert :ok =
             TxnSupport.track_txn_in_doubt(
               conn,
               txn,
               key,
               Error.from_result_code(:timeout, in_doubt: true)
             )

    assert {:ok, %{write_in_doubt: true}} = TxnOps.get_tracking(conn, txn)
  end

  test "prepare_txn_for_operate routes read-only requests through prepare_txn_read", %{
    conn: conn,
    txn: txn,
    key: key
  } do
    TxnOps.init_tracking(conn, txn)

    assert :ok = TxnSupport.prepare_txn_for_operate(conn, txn, key, false, [])
    assert {:ok, %{namespace: "test"}} = TxnOps.get_tracking(conn, txn)
  end

  defp stop_if_alive(pid) do
    if Process.alive?(pid) do
      Process.exit(pid, :shutdown)
    end
  end
end
