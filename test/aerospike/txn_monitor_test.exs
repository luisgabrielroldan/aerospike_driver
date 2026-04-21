defmodule Aerospike.TxnMonitorTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Message
  alias Aerospike.Supervisor, as: ClusterSupervisor
  alias Aerospike.Transport.Fake
  alias Aerospike.Txn
  alias Aerospike.TxnMonitor

  setup do
    name = :"txn_monitor_test_#{System.unique_integer([:positive])}"
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

  defp stop_if_alive(pid) do
    if Process.alive?(pid) do
      Process.exit(pid, :shutdown)
    end
  end
end
