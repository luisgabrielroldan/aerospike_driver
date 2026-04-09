defmodule Aerospike.CRUDTxnTest do
  use ExUnit.Case, async: true

  alias Aerospike.CRUD
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Message
  alias Aerospike.Tables
  alias Aerospike.Txn
  alias Aerospike.TxnOps

  @conn :crud_txn_test

  setup do
    table = Tables.txn_tracking(@conn)

    if :ets.whereis(table) == :undefined do
      :ets.new(table, [:set, :public, :named_table])
    else
      :ets.delete_all_objects(table)
    end

    key = Key.new("test", "users", "txn-wire-test")
    txn = %Txn{id: 42_000, timeout: 5_000}
    TxnOps.init_tracking(@conn, txn)

    {:ok, key: key, txn: txn}
  end

  describe "maybe_add_mrt_fields/5 with txn" do
    test "adds MRT_ID field with little-endian encoding", %{key: key, txn: txn} do
      msg = base_write_msg(key)
      result = CRUD.maybe_add_mrt_fields(msg, @conn, key, [txn: txn], true)

      mrt_id_field = find_field(result, Field.type_mrt_id())
      assert mrt_id_field != nil
      assert mrt_id_field.data == <<txn.id::64-signed-little>>
    end

    test "includes MRT_DEADLINE on writes when deadline is set", %{key: key, txn: txn} do
      TxnOps.set_deadline(@conn, txn, 12_345)

      msg = base_write_msg(key)
      result = CRUD.maybe_add_mrt_fields(msg, @conn, key, [txn: txn], true)

      deadline_field = find_field(result, Field.type_mrt_deadline())
      assert deadline_field != nil
      assert deadline_field.data == <<12_345::32-signed-little>>
    end

    test "omits MRT_DEADLINE on reads even when deadline is set", %{key: key, txn: txn} do
      TxnOps.set_deadline(@conn, txn, 12_345)

      msg = base_read_msg(key)
      result = CRUD.maybe_add_mrt_fields(msg, @conn, key, [txn: txn], false)

      assert find_field(result, Field.type_mrt_id()) != nil
      assert find_field(result, Field.type_mrt_deadline()) == nil
    end

    test "omits MRT_DEADLINE on writes when deadline is zero", %{key: key, txn: txn} do
      msg = base_write_msg(key)
      result = CRUD.maybe_add_mrt_fields(msg, @conn, key, [txn: txn], true)

      assert find_field(result, Field.type_mrt_id()) != nil
      assert find_field(result, Field.type_mrt_deadline()) == nil
    end

    test "includes RECORD_VERSION when key has a read version", %{key: key, txn: txn} do
      TxnOps.track_read(@conn, txn, key, 999)

      msg = base_write_msg(key)
      result = CRUD.maybe_add_mrt_fields(msg, @conn, key, [txn: txn], true)

      rv_field = find_field(result, Field.type_record_version())
      assert rv_field != nil
      assert rv_field.data == <<999::56-little-unsigned>>
    end

    test "omits RECORD_VERSION when key has no read version", %{key: key, txn: txn} do
      msg = base_write_msg(key)
      result = CRUD.maybe_add_mrt_fields(msg, @conn, key, [txn: txn], true)

      assert find_field(result, Field.type_record_version()) == nil
    end

    test "preserves original fields", %{key: key, txn: txn} do
      msg = base_write_msg(key)
      original_count = length(msg.fields)

      result = CRUD.maybe_add_mrt_fields(msg, @conn, key, [txn: txn], true)

      assert length(result.fields) > original_count

      assert find_field(result, Field.type_namespace()) != nil
      assert find_field(result, Field.type_table()) != nil
      assert find_field(result, Field.type_digest()) != nil
    end
  end

  describe "maybe_add_mrt_fields/5 without txn" do
    test "returns message unchanged when no txn in opts", %{key: key} do
      msg = base_write_msg(key)
      result = CRUD.maybe_add_mrt_fields(msg, @conn, key, [], true)

      assert result == msg
    end

    test "returns message unchanged when txn is nil", %{key: key} do
      msg = base_read_msg(key)
      result = CRUD.maybe_add_mrt_fields(msg, @conn, key, [txn: nil], false)

      assert result == msg
    end
  end

  describe "MRT fields survive encode/decode round-trip" do
    test "put wire with txn includes MRT_ID (little-endian)", %{key: key, txn: txn} do
      TxnOps.set_deadline(@conn, txn, 7_777)
      TxnOps.track_read(@conn, txn, key, 500)

      msg =
        base_write_msg(key)
        |> CRUD.maybe_add_mrt_fields(@conn, key, [txn: txn], true)

      wire = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, decoded} = AsmMsg.decode(body)

      mrt_id = find_field(decoded, Field.type_mrt_id())
      assert mrt_id != nil
      <<decoded_id::64-signed-little>> = mrt_id.data
      assert decoded_id == txn.id

      rv = find_field(decoded, Field.type_record_version())
      assert rv != nil
      <<decoded_version::56-little-unsigned>> = rv.data
      assert decoded_version == 500

      dl = find_field(decoded, Field.type_mrt_deadline())
      assert dl != nil
      <<decoded_deadline::32-signed-little>> = dl.data
      assert decoded_deadline == 7_777
    end

    test "get wire with txn includes MRT_ID but not MRT_DEADLINE", %{key: key, txn: txn} do
      TxnOps.set_deadline(@conn, txn, 7_777)

      msg =
        base_read_msg(key)
        |> CRUD.maybe_add_mrt_fields(@conn, key, [txn: txn], false)

      wire = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, decoded} = AsmMsg.decode(body)

      assert find_field(decoded, Field.type_mrt_id()) != nil
      assert find_field(decoded, Field.type_mrt_deadline()) == nil
    end

    test "wire without txn has no MRT fields", %{key: key} do
      msg = base_write_msg(key)

      wire = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, decoded} = AsmMsg.decode(body)

      assert find_field(decoded, Field.type_mrt_id()) == nil
      assert find_field(decoded, Field.type_mrt_deadline()) == nil
      assert find_field(decoded, Field.type_record_version()) == nil
    end
  end

  # -- helpers ----------------------------------------------------------------

  defp base_write_msg(key) do
    AsmMsg.write_command(key.namespace, key.set, key.digest, [])
  end

  defp base_read_msg(key) do
    import Bitwise

    %AsmMsg{
      info1: AsmMsg.info1_read() ||| AsmMsg.info1_get_all(),
      fields: [
        Field.namespace(key.namespace),
        Field.set(key.set),
        Field.digest(key.digest)
      ]
    }
  end

  defp find_field(%AsmMsg{fields: fields}, type) do
    Enum.find(fields, fn f -> f.type == type end)
  end
end
