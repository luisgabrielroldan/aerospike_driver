defmodule Aerospike.Runtime.TxnMonitorTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias Aerospike.Cluster.Supervisor, as: ClusterSupervisor
  alias Aerospike.Cluster.Tender
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.MessagePack
  alias Aerospike.Runtime.TxnMonitor
  alias Aerospike.Runtime.TxnOps
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake
  alias Aerospike.Txn

  setup do
    name = :"txn_monitor_test_#{System.unique_integer([:positive])}"
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

  test "monitor_key/2 is stable per txn/namespace", %{txn: txn} do
    key1 = TxnMonitor.monitor_key(txn, "test")
    key2 = TxnMonitor.monitor_key(txn, "test")

    assert key1.digest == key2.digest
  end

  test "encode_monitor_msg/5 emits a valid AS_MSG body", %{txn: txn} do
    monitor_key = TxnMonitor.monitor_key(txn, "test")

    wire =
      TxnMonitor.encode_monitor_msg(
        "test",
        monitor_key.digest,
        AsmMsg.info2_write() ||| AsmMsg.info2_respond_all_ops(),
        5,
        []
      )

    assert {:ok, {2, 3, body}} = wire |> IO.iodata_to_binary() |> Message.decode()
    assert {:ok, %AsmMsg{info2: info2, fields: fields}} = AsmMsg.decode(body)
    assert info2 == (AsmMsg.info2_write() ||| AsmMsg.info2_respond_all_ops())
    assert Enum.any?(fields, &(&1.type == Field.type_digest()))
    assert Enum.any?(fields, &(&1.type == Field.type_namespace()))
  end

  test "register_key/4 initializes namespace and stores the monitor deadline", %{
    conn: conn,
    txn: txn,
    key: key,
    fake: fake
  } do
    TxnOps.init_tracking(conn, txn)

    Fake.script_command(fake, "A1", {:ok, monitor_reply(0, [Field.mrt_deadline(321)])})

    assert :ok = TxnMonitor.register_key(conn, txn, key)
    assert {:ok, %{namespace: "test"}} = TxnOps.get_tracking(conn, txn)
    assert TxnOps.get_deadline(conn, txn) == 321

    assert %AsmMsg{operations: operations} = last_monitor_msg(fake)
    assert %Operation{op_type: op_type, bin_name: "keyds", data: data} = List.last(operations)
    assert op_type == Operation.op_cdt_modify()
    assert MessagePack.unpack!(data) == [1, key.digest, 1, 13]
  end

  test "register_key/4 is a no-op for already tracked writes and roll helpers validate initialization",
       %{
         conn: conn,
         txn: txn,
         key: key
       } do
    TxnOps.init_tracking(conn, txn)
    TxnOps.set_namespace(conn, txn, key.namespace)
    TxnOps.track_write(conn, txn, key, nil, :ok)

    assert :ok = TxnMonitor.register_key(conn, txn, key)

    assert {:error,
            %Aerospike.Error{code: :parameter_error, message: "transaction not initialized"}} =
             TxnMonitor.mark_roll_forward(conn, Txn.new())

    txn2 = Txn.new()
    TxnOps.init_tracking(conn, txn2)

    assert {:error,
            %Aerospike.Error{code: :parameter_error, message: "transaction has no namespace set"}} =
             TxnMonitor.close(conn, txn2)
  end

  test "check_result_code/1 maps known and unknown server codes" do
    assert :ok = TxnMonitor.check_result_code(%AsmMsg{result_code: 0})

    assert {:error, %Aerospike.Error{code: :key_not_found}} =
             TxnMonitor.check_result_code(%AsmMsg{result_code: 2})

    assert {:error, %Aerospike.Error{code: :server_error, message: message}} =
             TxnMonitor.check_result_code(%AsmMsg{result_code: 255})

    assert message =~ "255"
  end

  defp monitor_reply(result_code, fields) do
    %AsmMsg{result_code: result_code, fields: fields}
    |> AsmMsg.encode()
    |> IO.iodata_to_binary()
  end

  defp last_monitor_msg(fake) do
    request = fake |> Fake.last_command_request("A1") |> IO.iodata_to_binary()

    assert {:ok, {2, 3, body}} = Message.decode(request)
    assert {:ok, %AsmMsg{} = msg} = AsmMsg.decode(body)

    msg
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
