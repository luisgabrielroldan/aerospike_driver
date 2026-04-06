defmodule Aerospike.TxnRollTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Message
  alias Aerospike.TableOwner
  alias Aerospike.Txn
  alias Aerospike.TxnOps
  alias Aerospike.TxnRoll

  setup do
    name = :"txn_roll_test_#{:erlang.unique_integer([:positive])}"
    {:ok, pid} = TableOwner.start_link(name: name)

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5_000)
    end)

    txn = Txn.new()
    key = Key.new("test", "users", "roll-test-key")

    {:ok, conn: name, txn: txn, key: key}
  end

  # ---------------------------------------------------------------------------
  # State machine: commit/2 idempotency and error paths
  # ---------------------------------------------------------------------------

  describe "commit/3 — state guards" do
    test "returns :already_committed when state is :committed", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_state(conn, txn, :committed)

      assert {:ok, :already_committed} = TxnRoll.commit(conn, txn)
    end

    test "returns txn_already_aborted error when state is :aborted", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_state(conn, txn, :aborted)

      assert {:error, %Error{code: :txn_already_aborted}} = TxnRoll.commit(conn, txn)
    end

    test "returns error when txn not initialized", %{conn: conn} do
      txn = Txn.new()
      assert {:error, %Error{code: :parameter_error}} = TxnRoll.commit(conn, txn)
    end
  end

  # ---------------------------------------------------------------------------
  # State machine: abort/2 idempotency and error paths
  # ---------------------------------------------------------------------------

  describe "abort/3 — state guards" do
    test "returns :already_aborted when state is :aborted", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_state(conn, txn, :aborted)

      assert {:ok, :already_aborted} = TxnRoll.abort(conn, txn)
    end

    test "returns txn_already_committed error when state is :committed", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_state(conn, txn, :committed)

      assert {:error, %Error{code: :txn_already_committed}} = TxnRoll.abort(conn, txn)
    end

    test "returns error when txn not initialized", %{conn: conn} do
      txn = Txn.new()
      assert {:error, %Error{code: :parameter_error}} = TxnRoll.abort(conn, txn)
    end

    test "abort with no writes completes without network calls", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)

      assert {:ok, :aborted} = TxnRoll.abort(conn, txn)
      assert {:error, :not_found} = TxnOps.get_tracking(conn, txn)
    end
  end

  # ---------------------------------------------------------------------------
  # State machine: txn_status/2
  # ---------------------------------------------------------------------------

  describe "txn_status/2" do
    test "returns :open for freshly initialized txn", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      assert {:ok, :open} = TxnRoll.txn_status(conn, txn)
    end

    test "returns :verified after set_state", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_state(conn, txn, :verified)
      assert {:ok, :verified} = TxnRoll.txn_status(conn, txn)
    end

    test "returns :committed after set_state", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_state(conn, txn, :committed)
      assert {:ok, :committed} = TxnRoll.txn_status(conn, txn)
    end

    test "returns :aborted after set_state", %{conn: conn, txn: txn} do
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_state(conn, txn, :aborted)
      assert {:ok, :aborted} = TxnRoll.txn_status(conn, txn)
    end

    test "returns error when txn not initialized", %{conn: conn} do
      txn = Txn.new()
      assert {:error, %Error{code: :parameter_error}} = TxnRoll.txn_status(conn, txn)
    end
  end

  # ---------------------------------------------------------------------------
  # Wire encoding: encode_verify_msg/2
  # ---------------------------------------------------------------------------

  describe "encode_verify_msg/2" do
    test "decodes as valid AS_MSG", %{key: key} do
      wire = TxnRoll.encode_verify_msg(key, 42)
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, %AsmMsg{}} = AsmMsg.decode(body)
    end

    test "has INFO1_READ | INFO1_NOBINDATA flags", %{key: key} do
      msg = decode_roll_wire!(TxnRoll.encode_verify_msg(key, 42))
      expected = AsmMsg.info1_read() ||| AsmMsg.info1_nobindata()
      assert msg.info1 == expected
    end

    test "has INFO3_SC_READ_TYPE flag", %{key: key} do
      msg = decode_roll_wire!(TxnRoll.encode_verify_msg(key, 42))
      assert msg.info3 == AsmMsg.info3_sc_read_type()
    end

    test "has INFO4_MRT_VERIFY_READ flag", %{key: key} do
      msg = decode_roll_wire!(TxnRoll.encode_verify_msg(key, 42))
      assert msg.info4 == AsmMsg.info4_mrt_verify_read()
    end

    test "has no info2 flags (read command)", %{key: key} do
      msg = decode_roll_wire!(TxnRoll.encode_verify_msg(key, 42))
      assert msg.info2 == 0
    end

    test "contains RECORD_VERSION field with correct version", %{key: key} do
      version = 9_999
      msg = decode_roll_wire!(TxnRoll.encode_verify_msg(key, version))

      rv_field = find_field(msg, Field.type_record_version())
      assert rv_field != nil
      <<decoded::56-little-unsigned>> = rv_field.data
      assert decoded == version
    end

    test "does NOT contain MRT_ID field", %{key: key} do
      msg = decode_roll_wire!(TxnRoll.encode_verify_msg(key, 1))
      assert find_field(msg, Field.type_mrt_id()) == nil
    end

    test "does NOT contain MRT_DEADLINE field", %{key: key} do
      msg = decode_roll_wire!(TxnRoll.encode_verify_msg(key, 1))
      assert find_field(msg, Field.type_mrt_deadline()) == nil
    end

    test "contains namespace, set, and digest fields", %{key: key} do
      msg = decode_roll_wire!(TxnRoll.encode_verify_msg(key, 1))

      ns_field = find_field(msg, Field.type_namespace())
      assert ns_field.data == key.namespace

      set_field = find_field(msg, Field.type_table())
      assert set_field.data == key.set

      digest_field = find_field(msg, Field.type_digest())
      assert digest_field.data == key.digest
    end

    test "has no operations", %{key: key} do
      msg = decode_roll_wire!(TxnRoll.encode_verify_msg(key, 1))
      assert msg.operations == []
    end
  end

  # ---------------------------------------------------------------------------
  # Wire encoding: encode_roll_msg/3 — forward
  # ---------------------------------------------------------------------------

  describe "encode_roll_msg/3 :forward" do
    test "has INFO2_WRITE | INFO2_DURABLE_DELETE", %{key: key, txn: txn} do
      msg = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :forward))
      expected = AsmMsg.info2_write() ||| AsmMsg.info2_durable_delete()
      assert msg.info2 == expected
    end

    test "has INFO4_MRT_ROLL_FORWARD flag", %{key: key, txn: txn} do
      msg = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :forward))
      assert msg.info4 == AsmMsg.info4_mrt_roll_forward()
    end

    test "has no info1 flags", %{key: key, txn: txn} do
      msg = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :forward))
      assert msg.info1 == 0
    end

    test "contains MRT_ID field with correct little-endian encoding", %{key: key, txn: txn} do
      msg = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :forward))

      mrt_id_field = find_field(msg, Field.type_mrt_id())
      assert mrt_id_field != nil
      <<decoded_id::64-signed-little>> = mrt_id_field.data
      assert decoded_id == txn.id
    end

    test "does NOT contain MRT_DEADLINE field", %{key: key, txn: txn} do
      msg = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :forward))
      assert find_field(msg, Field.type_mrt_deadline()) == nil
    end

    test "contains namespace, set, and digest fields", %{key: key, txn: txn} do
      msg = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :forward))

      assert find_field(msg, Field.type_namespace()).data == key.namespace
      assert find_field(msg, Field.type_table()).data == key.set
      assert find_field(msg, Field.type_digest()).data == key.digest
    end

    test "has no operations", %{key: key, txn: txn} do
      msg = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :forward))
      assert msg.operations == []
    end
  end

  # ---------------------------------------------------------------------------
  # Wire encoding: encode_roll_msg/3 — back
  # ---------------------------------------------------------------------------

  describe "encode_roll_msg/3 :back" do
    test "has INFO2_WRITE | INFO2_DURABLE_DELETE", %{key: key, txn: txn} do
      msg = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :back))
      expected = AsmMsg.info2_write() ||| AsmMsg.info2_durable_delete()
      assert msg.info2 == expected
    end

    test "has INFO4_MRT_ROLL_BACK flag", %{key: key, txn: txn} do
      msg = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :back))
      assert msg.info4 == AsmMsg.info4_mrt_roll_back()
    end

    test "contains MRT_ID field, no MRT_DEADLINE", %{key: key, txn: txn} do
      msg = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :back))

      assert find_field(msg, Field.type_mrt_id()) != nil
      assert find_field(msg, Field.type_mrt_deadline()) == nil
    end

    test "forward and back use different info4 flags", %{key: key, txn: txn} do
      fwd = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :forward))
      bck = decode_roll_wire!(TxnRoll.encode_roll_msg(key, txn.id, :back))

      refute fwd.info4 == bck.info4
    end
  end

  # ---------------------------------------------------------------------------
  # transaction/3 error handling
  # ---------------------------------------------------------------------------

  describe "transaction/3 — error handling paths" do
    test "Aerospike.Error raised in callback aborts and returns {:error, e}", %{conn: conn} do
      error = Error.from_result_code(:key_not_found)

      result =
        TxnRoll.transaction(conn, [], fn _txn ->
          raise error
        end)

      assert {:error, %Error{code: :key_not_found}} = result
    end

    test "RuntimeError raised in callback aborts and re-raises", %{conn: conn} do
      assert_raise RuntimeError, "boom", fn ->
        TxnRoll.transaction(conn, [], fn _txn ->
          raise RuntimeError, "boom"
        end)
      end
    end

    test "throw in callback aborts and re-throws", %{conn: conn} do
      assert catch_throw(
               TxnRoll.transaction(conn, [], fn _txn ->
                 throw(:stop)
               end)
             ) == :stop
    end

    test "ETS tracking is cleaned up after Aerospike.Error in callback", %{conn: conn} do
      captured_txn = :ets.new(:tmp_capture, [:set, :public])

      TxnRoll.transaction(conn, [], fn txn ->
        :ets.insert(captured_txn, {:txn, txn})
        raise Error.from_result_code(:key_not_found)
      end)

      [{:txn, txn}] = :ets.lookup(captured_txn, :txn)
      assert {:error, :not_found} = TxnOps.get_tracking(conn, txn)
    end

    test "ETS tracking is cleaned up after RuntimeError in callback", %{conn: conn} do
      captured_txn = :ets.new(:tmp_capture2, [:set, :public])

      assert_raise RuntimeError, fn ->
        TxnRoll.transaction(conn, [], fn txn ->
          :ets.insert(captured_txn, {:txn, txn})
          raise RuntimeError, "oops"
        end)
      end

      [{:txn, txn}] = :ets.lookup(captured_txn, :txn)
      assert {:error, :not_found} = TxnOps.get_tracking(conn, txn)
    end

    test "transaction/3 with opts creates a txn with those opts", %{conn: conn} do
      captured_txn = :ets.new(:tmp_capture3, [:set, :public])

      TxnRoll.transaction(conn, [timeout: 5_000], fn txn ->
        :ets.insert(captured_txn, {:txn, txn})
        raise Error.from_result_code(:key_not_found)
      end)

      [{:txn, txn}] = :ets.lookup(captured_txn, :txn)
      assert txn.timeout == 5_000
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp decode_roll_wire!(wire) do
    {:ok, {2, 3, body}} = Message.decode(wire)
    {:ok, msg} = AsmMsg.decode(body)
    msg
  end

  defp find_field(%AsmMsg{fields: fields}, type) do
    Enum.find(fields, fn f -> f.type == type end)
  end
end
