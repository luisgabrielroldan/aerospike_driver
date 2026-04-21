defmodule Aerospike.TxnRollTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Message
  alias Aerospike.Supervisor, as: ClusterSupervisor
  alias Aerospike.Transport.Fake
  alias Aerospike.Txn
  alias Aerospike.TxnOps
  alias Aerospike.TxnRoll

  setup do
    name = :"txn_roll_test_#{System.unique_integer([:positive])}"
    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])

    {:ok, sup} =
      ClusterSupervisor.start_link(
        name: name,
        transport: Fake,
        seeds: [{"10.0.0.1", 3000}],
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

  test "commit/2 and abort/2 reject unknown transactions", %{conn: conn} do
    txn = Txn.new()

    assert {:error, %Error{code: :parameter_error}} = TxnRoll.commit(conn, txn)
    assert {:error, %Error{code: :parameter_error}} = TxnRoll.abort(conn, txn)
  end

  test "encode_verify_msg/2 contains the record version field", %{key: key} do
    wire = TxnRoll.encode_verify_msg(key, 42)
    assert {:ok, {2, 3, body}} = wire |> IO.iodata_to_binary() |> Message.decode()
    assert {:ok, %AsmMsg{} = msg} = AsmMsg.decode(body)

    assert msg.info1 == (AsmMsg.info1_read() ||| AsmMsg.info1_nobindata())
    assert msg.info4 == AsmMsg.info4_mrt_verify_read()
    assert Enum.any?(msg.fields, &(&1.type == Field.type_record_version()))
  end

  test "encode_roll_msg/3 sets the forward and back flags", %{key: key, txn: txn} do
    fwd = TxnRoll.encode_roll_msg(key, txn.id, :forward)
    bck = TxnRoll.encode_roll_msg(key, txn.id, :back)

    assert {:ok, {2, 3, fwd_body}} = fwd |> IO.iodata_to_binary() |> Message.decode()
    assert {:ok, {2, 3, bck_body}} = bck |> IO.iodata_to_binary() |> Message.decode()
    assert {:ok, %AsmMsg{info4: fwd_info4}} = AsmMsg.decode(fwd_body)
    assert {:ok, %AsmMsg{info4: bck_info4}} = AsmMsg.decode(bck_body)

    assert fwd_info4 != bck_info4
  end

  test "transaction/3 cleans up empty transactions", %{conn: conn} do
    txn = Txn.new()

    assert {:ok, :done} =
             TxnRoll.transaction(conn, txn, fn tx ->
               assert {:ok, :open} = TxnRoll.txn_status(conn, tx)
               :done
             end)

    assert {:error, :not_found} = TxnOps.get_tracking(conn, txn)
  end

  defp stop_if_alive(pid) do
    if Process.alive?(pid) do
      Process.exit(pid, :shutdown)
    end
  end
end
