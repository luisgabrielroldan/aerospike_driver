defmodule Aerospike.Runtime.TxnRollTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Message
  alias Aerospike.Cluster.Supervisor, as: ClusterSupervisor
  alias Aerospike.Cluster.Tender
  alias Aerospike.Runtime.TxnOps
  alias Aerospike.Runtime.TxnRoll
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake
  alias Aerospike.Txn

  setup do
    name = :"txn_roll_test_#{System.unique_integer([:positive])}"
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

    script_single_node_cluster(fake)
    :ok = Tender.tend_now(name)

    {:ok, conn: name, fake: fake, txn: Txn.new(), key: Key.new("test", "spike", "txn")}
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

  test "commit, abort, and txn_status reflect tracked terminal state branches", %{
    conn: conn,
    txn: txn
  } do
    TxnOps.init_tracking(conn, txn)
    TxnOps.set_state(conn, txn, :committed)

    assert {:ok, :already_committed} = TxnRoll.commit(conn, txn)
    assert {:error, %Error{code: :txn_already_committed}} = TxnRoll.abort(conn, txn)
    assert {:ok, :committed} = TxnRoll.txn_status(conn, txn)

    txn2 = Txn.new()
    TxnOps.init_tracking(conn, txn2)
    TxnOps.set_state(conn, txn2, :aborted)

    assert {:ok, :already_aborted} = TxnRoll.abort(conn, txn2)
    assert {:error, %Error{code: :txn_already_aborted}} = TxnRoll.commit(conn, txn2)
  end

  test "commit/3 aborts and cleans up when read verification fails", %{
    conn: conn,
    txn: txn,
    key: key,
    fake: fake
  } do
    TxnOps.init_tracking(conn, txn)
    TxnOps.set_namespace(conn, txn, key.namespace)
    TxnOps.track_read(conn, txn, key, 7)

    Fake.script_command(fake, "A1", {:ok, verify_reply(2)})

    assert {:error, %Error{code: :key_not_found}} = TxnRoll.commit(conn, txn)
    assert {:error, :not_found} = TxnOps.get_tracking(conn, txn)
  end

  test "commit/3 closes the transaction after a verified monitor-backed commit", %{
    conn: conn,
    txn: txn,
    key: key,
    fake: fake
  } do
    TxnOps.init_tracking(conn, txn)
    TxnOps.set_namespace(conn, txn, key.namespace)
    TxnOps.set_deadline(conn, txn, 123)
    TxnOps.track_write(conn, txn, key, nil, :ok)
    TxnOps.set_state(conn, txn, :verified)

    Fake.script_command(fake, "A1", {:ok, metadata_reply(0)})
    Fake.script_command(fake, "A1", {:ok, metadata_reply(0)})

    assert {:ok, :committed} = TxnRoll.commit(conn, txn)
    assert {:error, :not_found} = TxnOps.get_tracking(conn, txn)
  end

  test "transaction/3 aborts on Aerospike.Error and reraises non-Aerospike failures", %{
    conn: conn
  } do
    txn = Txn.new()

    assert {:error, %Error{code: :timeout}} =
             TxnRoll.transaction(conn, txn, fn _tx ->
               raise Error.from_result_code(:timeout)
             end)

    assert {:error, :not_found} = TxnOps.get_tracking(conn, txn)

    assert_raise RuntimeError, "boom", fn ->
      TxnRoll.transaction(conn, [], fn _tx -> raise "boom" end)
    end

    assert catch_throw(
             TxnRoll.transaction(conn, [], fn _tx ->
               throw(:halted)
             end)
           ) == :halted
  end

  defp verify_reply(result_code) do
    %AsmMsg{result_code: result_code}
    |> AsmMsg.encode()
    |> IO.iodata_to_binary()
  end

  defp metadata_reply(result_code) do
    %AsmMsg{result_code: result_code}
    |> AsmMsg.encode()
    |> IO.iodata_to_binary()
  end

  defp script_single_node_cluster(fake) do
    Fake.script_info(fake, "A1", ["node", "features"], %{"node" => "A1", "features" => ""})

    Fake.script_info(
      fake,
      "A1",
      ["partition-generation", "cluster-stable", "peers-generation"],
      %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef",
        "peers-generation" => "1"
      }
    )

    Fake.script_info(fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

    Fake.script_info(fake, "A1", ["replicas"], %{
      "replicas" => ReplicasFixture.all_master("test", 1)
    })
  end

  defp stop_if_alive(pid) do
    if Process.alive?(pid) do
      Process.exit(pid, :shutdown)
    end
  end
end
