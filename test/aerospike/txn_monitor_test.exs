defmodule Aerospike.TxnMonitorTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Op
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Message
  alias Aerospike.TableOwner
  alias Aerospike.Txn
  alias Aerospike.TxnMonitor
  alias Aerospike.TxnOps

  describe "monitor_key/2" do
    test "returns a Key with set '<ERO~MRT' and integer user_key" do
      txn = Txn.new()
      key = TxnMonitor.monitor_key(txn, "test")

      assert %Key{} = key
      assert key.namespace == "test"
      assert key.set == "<ERO~MRT"
      assert key.user_key == txn.id
      assert byte_size(key.digest) == 20
    end

    test "same txn+namespace produces the same key" do
      txn = Txn.new()
      k1 = TxnMonitor.monitor_key(txn, "ns")
      k2 = TxnMonitor.monitor_key(txn, "ns")

      assert k1.digest == k2.digest
    end

    test "different txn IDs produce different digests" do
      t1 = Txn.new()
      t2 = Txn.new()
      k1 = TxnMonitor.monitor_key(t1, "test")
      k2 = TxnMonitor.monitor_key(t2, "test")

      refute k1.digest == k2.digest
    end
  end

  describe "encode_monitor_msg/5 — register_key shape" do
    setup do
      txn = Txn.new(timeout: 10_000)
      cmd_key = Key.new("test", "users", "user:1")

      ops = [
        Op.put("id", txn.id),
        Op.List.append("keyds", cmd_key.digest, policy: %{order: 1, flags: 13})
      ]

      mkey = TxnMonitor.monitor_key(txn, "test")

      wire =
        TxnMonitor.encode_monitor_msg(
          "test",
          mkey.digest,
          AsmMsg.info2_write() ||| AsmMsg.info2_respond_all_ops(),
          div(txn.timeout, 1000),
          ops
        )

      {:ok, wire: wire, txn: txn, mkey: mkey}
    end

    test "decodes as a valid AS_MSG", %{wire: wire} do
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, %AsmMsg{}} = AsmMsg.decode(body)
    end

    test "has INFO2_WRITE | INFO2_RESPOND_ALL_OPS flags", %{wire: wire} do
      msg = decode_msg!(wire)
      expected = AsmMsg.info2_write() ||| AsmMsg.info2_respond_all_ops()
      assert msg.info2 == expected
    end

    test "has no info4 MRT flags", %{wire: wire} do
      msg = decode_msg!(wire)
      assert msg.info4 == 0
    end

    test "has no info1 flags", %{wire: wire} do
      msg = decode_msg!(wire)
      assert msg.info1 == 0
    end

    test "has no info3 flags", %{wire: wire} do
      msg = decode_msg!(wire)
      assert msg.info3 == 0
    end

    test "does NOT contain MRT_ID or MRT_DEADLINE fields", %{wire: wire} do
      msg = decode_msg!(wire)
      mrt_id_type = Field.type_mrt_id()
      mrt_deadline_type = Field.type_mrt_deadline()

      field_types = Enum.map(msg.fields, & &1.type)
      refute mrt_id_type in field_types
      refute mrt_deadline_type in field_types
    end

    test "contains namespace, set, and digest fields", %{wire: wire, mkey: mkey} do
      msg = decode_msg!(wire)
      field_types = Enum.map(msg.fields, & &1.type)

      assert Field.type_namespace() in field_types
      assert Field.type_table() in field_types
      assert Field.type_digest() in field_types

      ns_field = Enum.find(msg.fields, &(&1.type == Field.type_namespace()))
      assert ns_field.data == "test"

      set_field = Enum.find(msg.fields, &(&1.type == Field.type_table()))
      assert set_field.data == "<ERO~MRT"

      digest_field = Enum.find(msg.fields, &(&1.type == Field.type_digest()))
      assert digest_field.data == mkey.digest
    end

    test "has two operations (put + list append) for first register", %{wire: wire} do
      msg = decode_msg!(wire)
      assert length(msg.operations) == 2
    end

    test "expiration is txn timeout in seconds", %{wire: wire, txn: txn} do
      msg = decode_msg!(wire)
      assert msg.expiration == div(txn.timeout, 1000)
    end
  end

  describe "encode_monitor_msg/5 — subsequent register (monitor exists)" do
    test "only has list append op (no put)" do
      txn = Txn.new()
      cmd_key = Key.new("test", "users", "user:2")
      mkey = TxnMonitor.monitor_key(txn, "test")

      ops = [Op.List.append("keyds", cmd_key.digest, policy: %{order: 1, flags: 13})]

      wire =
        TxnMonitor.encode_monitor_msg(
          "test",
          mkey.digest,
          AsmMsg.info2_write() ||| AsmMsg.info2_respond_all_ops(),
          div(txn.timeout, 1000),
          ops
        )

      msg = decode_msg!(wire)
      assert length(msg.operations) == 1
    end
  end

  describe "encode_monitor_msg/5 — mark_roll_forward shape" do
    setup do
      txn = Txn.new()
      mkey = TxnMonitor.monitor_key(txn, "test")

      ops = [Op.put("fwd", true)]

      wire =
        TxnMonitor.encode_monitor_msg(
          "test",
          mkey.digest,
          AsmMsg.info2_write(),
          0,
          ops
        )

      {:ok, wire: wire, mkey: mkey}
    end

    test "has INFO2_WRITE only", %{wire: wire} do
      msg = decode_msg!(wire)
      assert msg.info2 == AsmMsg.info2_write()
    end

    test "has no info4 MRT flags", %{wire: wire} do
      msg = decode_msg!(wire)
      assert msg.info4 == 0
    end

    test "has one write operation for 'fwd' bin", %{wire: wire} do
      msg = decode_msg!(wire)
      assert length(msg.operations) == 1
      [op] = msg.operations
      assert op.bin_name == "fwd"
    end

    test "expiration is 0", %{wire: wire} do
      msg = decode_msg!(wire)
      assert msg.expiration == 0
    end

    test "no MRT_ID or MRT_DEADLINE fields", %{wire: wire} do
      msg = decode_msg!(wire)
      field_types = Enum.map(msg.fields, & &1.type)
      refute Field.type_mrt_id() in field_types
      refute Field.type_mrt_deadline() in field_types
    end
  end

  describe "encode_monitor_msg/5 — close shape" do
    setup do
      txn = Txn.new()
      mkey = TxnMonitor.monitor_key(txn, "test")

      wire =
        TxnMonitor.encode_monitor_msg(
          "test",
          mkey.digest,
          AsmMsg.info2_write() ||| AsmMsg.info2_delete() ||| AsmMsg.info2_durable_delete(),
          0,
          []
        )

      {:ok, wire: wire, mkey: mkey}
    end

    test "has INFO2_WRITE | INFO2_DELETE | INFO2_DURABLE_DELETE", %{wire: wire} do
      msg = decode_msg!(wire)
      expected = AsmMsg.info2_write() ||| AsmMsg.info2_delete() ||| AsmMsg.info2_durable_delete()
      assert msg.info2 == expected
    end

    test "has no operations", %{wire: wire} do
      msg = decode_msg!(wire)
      assert msg.operations == []
    end

    test "has no info4 MRT flags", %{wire: wire} do
      msg = decode_msg!(wire)
      assert msg.info4 == 0
    end

    test "no MRT_ID or MRT_DEADLINE fields", %{wire: wire} do
      msg = decode_msg!(wire)
      field_types = Enum.map(msg.fields, & &1.type)
      refute Field.type_mrt_id() in field_types
      refute Field.type_mrt_deadline() in field_types
    end

    test "expiration is 0", %{wire: wire} do
      msg = decode_msg!(wire)
      assert msg.expiration == 0
    end

    test "contains namespace, set '<ERO~MRT', and digest", %{wire: wire, mkey: mkey} do
      msg = decode_msg!(wire)

      ns_field = Enum.find(msg.fields, &(&1.type == Field.type_namespace()))
      assert ns_field.data == "test"

      set_field = Enum.find(msg.fields, &(&1.type == Field.type_table()))
      assert set_field.data == "<ERO~MRT"

      digest_field = Enum.find(msg.fields, &(&1.type == Field.type_digest()))
      assert digest_field.data == mkey.digest
    end
  end

  describe "check_result_code/1 — result code to Error conversion" do
    test "result_code 0 returns :ok" do
      assert :ok = TxnMonitor.check_result_code(%AsmMsg{result_code: 0})
    end

    test "result_code 124 (MRT_COMMITTED) returns error with :mrt_committed atom" do
      assert {:error, %Error{code: :mrt_committed}} =
               TxnMonitor.check_result_code(%AsmMsg{result_code: 124})
    end

    test "result_code 125 (MRT_ABORTED) returns error with :mrt_aborted atom" do
      assert {:error, %Error{code: :mrt_aborted}} =
               TxnMonitor.check_result_code(%AsmMsg{result_code: 125})
    end

    test "unknown result code returns error with :server_error atom" do
      assert {:error, %Error{code: :server_error}} =
               TxnMonitor.check_result_code(%AsmMsg{result_code: 99_999})
    end

    test "error message for unknown code includes the integer" do
      {:error, %Error{message: msg}} =
        TxnMonitor.check_result_code(%AsmMsg{result_code: 99_999})

      assert msg =~ "99999"
    end
  end

  describe "transaction preflight and monitor commands" do
    setup do
      conn = :"txn_monitor_test_#{System.unique_integer([:positive, :monotonic])}"
      _pid = start_supervised!({TableOwner, name: conn})

      txn = Txn.new()
      key = Key.new("test", "users", "txn-monitor-key")

      {:ok, conn: conn, txn: txn, key: key}
    end

    test "register_key/4 returns parameter_error when txn was not initialized", %{
      conn: conn,
      txn: txn,
      key: key
    } do
      assert {:error, %Error{code: :parameter_error}} = TxnMonitor.register_key(conn, txn, key)
    end

    test "register_key/4 is a no-op when key was already tracked as a write", %{
      conn: conn,
      txn: txn,
      key: key
    } do
      TxnOps.init_tracking(conn, txn)
      :ok = TxnOps.set_namespace(conn, txn, key.namespace)
      TxnOps.track_write(conn, txn, key, nil, :ok)

      assert :ok = TxnMonitor.register_key(conn, txn, key)
    end

    test "register_key/4 builds and sends monitor create/write wire when monitor does not exist",
         %{
           conn: conn,
           txn: txn,
           key: key
         } do
      TxnOps.init_tracking(conn, txn)
      :ok = TxnOps.set_namespace(conn, txn, key.namespace)

      assert {:error, %Error{}} = TxnMonitor.register_key(conn, txn, key)
    end

    test "register_key/4 uses append-only path when monitor already exists", %{
      conn: conn,
      txn: txn,
      key: key
    } do
      TxnOps.init_tracking(conn, txn)
      :ok = TxnOps.set_namespace(conn, txn, key.namespace)
      TxnOps.set_deadline(conn, txn, 1)

      assert {:error, %Error{}} = TxnMonitor.register_key(conn, txn, key)
    end

    test "mark_roll_forward/3 returns parameter_error when namespace is not set", %{
      conn: conn,
      txn: txn
    } do
      TxnOps.init_tracking(conn, txn)

      assert {:error, %Error{code: :parameter_error}} = TxnMonitor.mark_roll_forward(conn, txn)
    end

    test "mark_roll_forward/3 and close/3 return parameter_error for unknown txn", %{conn: conn} do
      unknown_txn = Txn.new()

      assert {:error, %Error{code: :parameter_error}} =
               TxnMonitor.mark_roll_forward(conn, unknown_txn)

      assert {:error, %Error{code: :parameter_error}} = TxnMonitor.close(conn, unknown_txn)
    end
  end

  defp decode_msg!(wire) do
    {:ok, {2, 3, body}} = Message.decode(wire)
    {:ok, msg} = AsmMsg.decode(body)
    msg
  end
end
