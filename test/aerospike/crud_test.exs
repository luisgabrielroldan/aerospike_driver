defmodule Aerospike.CRUDTest do
  use ExUnit.Case, async: true

  alias Aerospike.CRUD
  alias Aerospike.Error
  alias Aerospike.Exp
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers
  alias Aerospike.Txn
  alias Aerospike.TxnOps

  setup do
    key = Key.new("test", "users", "wire-test-key")
    {:ok, key: key}
  end

  describe "normalize_bins/1" do
    test "converts atom keys to strings" do
      assert CRUD.normalize_bins(%{a: 1, b: 2}) == %{"a" => 1, "b" => 2}
    end

    test "keeps string keys" do
      assert CRUD.normalize_bins(%{"x" => 1}) == %{"x" => 1}
    end

    test "raises on invalid key type" do
      assert_raise ArgumentError, fn -> CRUD.normalize_bins(%{1 => :x}) end
    end
  end

  describe "wire encoding (protocol shape)" do
    test "put message decodes to write with bins", %{key: key} do
      wire = Helpers.put_wire(key, %{"n" => 1, "s" => "x"})
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, msg} = AsmMsg.decode(body)
      assert msg.info2 == AsmMsg.info2_write()
      assert length(msg.operations) == 2
    end

    test "get message decodes to read", %{key: key} do
      wire = Helpers.get_wire(key)
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, msg} = AsmMsg.decode(body)
      import Bitwise
      assert msg.info1 == (AsmMsg.info1_read() ||| AsmMsg.info1_get_all())
      assert msg.operations == []
    end

    test "delete message decodes to delete", %{key: key} do
      wire = Helpers.delete_wire(key)
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, msg} = AsmMsg.decode(body)
      import Bitwise
      assert msg.info2 == (AsmMsg.info2_write() ||| AsmMsg.info2_delete())
    end

    test "exists message decodes to exists", %{key: key} do
      wire = Helpers.exists_wire(key)
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, msg} = AsmMsg.decode(body)
      import Bitwise
      assert msg.info1 == (AsmMsg.info1_read() ||| AsmMsg.info1_nobindata())
    end

    test "touch message decodes to touch op", %{key: key} do
      wire = Helpers.touch_wire(key)
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, msg} = AsmMsg.decode(body)
      assert length(msg.operations) == 1
    end
  end

  describe "apply_filter_exp/2" do
    test "appends FILTER_EXP field (type 43) when filter opt is present", %{key: key} do
      import Bitwise

      exp = Exp.int(1)

      base_msg = %AsmMsg{
        info1: AsmMsg.info1_read() ||| AsmMsg.info1_get_all(),
        fields: [
          Field.namespace(key.namespace),
          Field.set(key.set),
          Field.digest(key.digest)
        ]
      }

      msg = CRUD.apply_filter_exp(base_msg, filter: exp)

      wire = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, decoded} = AsmMsg.decode(body)

      filter_fields = Enum.filter(decoded.fields, &(&1.type == Field.type_filter_exp()))
      assert [%Field{data: data}] = filter_fields
      assert data == exp.wire
    end

    test "does not append FILTER_EXP field when filter opt is absent", %{key: key} do
      import Bitwise

      base_msg = %AsmMsg{
        info1: AsmMsg.info1_read() ||| AsmMsg.info1_get_all(),
        fields: [
          Field.namespace(key.namespace),
          Field.set(key.set),
          Field.digest(key.digest)
        ]
      }

      msg = CRUD.apply_filter_exp(base_msg, [])

      wire = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, decoded} = AsmMsg.decode(body)

      filter_fields = Enum.filter(decoded.fields, &(&1.type == Field.type_filter_exp()))
      assert filter_fields == []
    end
  end

  describe "error paths" do
    test "write response generation error is surfaced as Error struct" do
      msg = %AsmMsg{result_code: 3}
      assert {:error, %Error{code: :generation_error}} = Response.parse_write_response(msg)
    end

    test "invalid write policy options are rejected by NimbleOptions" do
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_write(unknown_opt: true)
    end

    test "nil key namespace is rejected before building wire message" do
      assert_raise ArgumentError, fn ->
        Key.new(nil, "users", "k1")
      end
    end

    test "transaction-aware path adds MRT_ID field to message", %{key: key} do
      conn = unique_conn_name()
      txn = Txn.new()
      create_txn_tracking_table(conn)
      on_exit(fn -> maybe_delete_txn_tracking_table(conn) end)
      TxnOps.init_tracking(conn, txn)

      base_msg = %AsmMsg{
        fields: [Field.namespace(key.namespace), Field.set(key.set), Field.digest(key.digest)]
      }

      applied = CRUD.maybe_add_mrt_fields(base_msg, conn, key, [txn: txn], true)
      mrt_ids = Enum.filter(applied.fields, &(&1.type == Field.type_mrt_id()))

      assert length(mrt_ids) == 1
      assert [%Field{data: <<_::64-little-signed>>}] = mrt_ids
    end

    test "timeout/deadline field is added on writes, not reads", %{key: key} do
      conn = unique_conn_name()
      txn = Txn.new()
      create_txn_tracking_table(conn)
      on_exit(fn -> maybe_delete_txn_tracking_table(conn) end)
      TxnOps.init_tracking(conn, txn)
      TxnOps.set_deadline(conn, txn, 500)

      base_msg = %AsmMsg{
        fields: [Field.namespace(key.namespace), Field.set(key.set), Field.digest(key.digest)]
      }

      write_msg = CRUD.maybe_add_mrt_fields(base_msg, conn, key, [txn: txn], true)
      read_msg = CRUD.maybe_add_mrt_fields(base_msg, conn, key, [txn: txn], false)

      write_deadlines = Enum.filter(write_msg.fields, &(&1.type == Field.type_mrt_deadline()))
      read_deadlines = Enum.filter(read_msg.fields, &(&1.type == Field.type_mrt_deadline()))

      assert [%Field{data: <<500::32-little-signed>>}] = write_deadlines
      assert read_deadlines == []
    end

    test "txn preflight read/write errors are surfaced before routing", %{key: key} do
      conn = unique_conn_name()
      txn = Txn.new()
      create_meta_table(conn)
      create_txn_tracking_table(conn)

      on_exit(fn ->
        maybe_delete_meta_table(conn)
        maybe_delete_txn_tracking_table(conn)
      end)

      assert {:error, %Error{code: :parameter_error}} = CRUD.get(conn, key, txn: txn)
      assert {:error, %Error{code: :parameter_error}} = CRUD.exists(conn, key, txn: txn)
      assert {:error, %Error{code: :parameter_error}} = CRUD.put(conn, key, %{"n" => 1}, txn: txn)
      assert {:error, %Error{code: :parameter_error}} = CRUD.delete(conn, key, txn: txn)
      assert {:error, %Error{code: :parameter_error}} = CRUD.touch(conn, key, txn: txn)

      assert {:error, %Error{code: :parameter_error}} =
               CRUD.operate(conn, key, [Aerospike.Op.get("n")], txn: txn)
    end

    test "apply_udf packs all supported argument shapes before network dispatch", %{key: key} do
      conn = unique_conn_name()
      create_meta_table(conn)
      on_exit(fn -> maybe_delete_meta_table(conn) end)

      args = [
        "txt",
        {:bytes, <<1, 2, 3>>},
        nil,
        true,
        false,
        123,
        1.5,
        [1, "a", {:bytes, <<9>>}],
        %{"x" => 1, "nested" => %{"y" => 2}}
      ]

      assert {:error, %Error{}} = CRUD.apply_udf(conn, key, "pkg", "fun", args, [])
    end

    test "direct write command entrypoints execute and fail cleanly when cluster is not ready", %{
      key: key
    } do
      conn = unique_conn_name()
      create_meta_table(conn)
      on_exit(fn -> maybe_delete_meta_table(conn) end)

      assert {:error, %Error{code: :cluster_not_ready}} = CRUD.put(conn, key, %{"n" => 1})
      assert {:error, %Error{code: :cluster_not_ready}} = CRUD.add(conn, key, %{"n" => 1})
      assert {:error, %Error{code: :cluster_not_ready}} = CRUD.append(conn, key, %{"s" => "x"})
      assert {:error, %Error{code: :cluster_not_ready}} = CRUD.prepend(conn, key, %{"s" => "x"})
      assert {:error, %Error{code: :cluster_not_ready}} = CRUD.exists(conn, key)

      assert {:error, %Error{code: :cluster_not_ready}} =
               CRUD.operate(conn, key, [Aerospike.Op.get("n")])
    end
  end

  defp unique_conn_name do
    :"crud_test_#{System.unique_integer([:positive, :monotonic])}"
  end

  defp create_txn_tracking_table(conn) do
    :ets.new(Tables.txn_tracking(conn), [:named_table, :set, :public])
  end

  defp create_meta_table(conn) do
    :ets.new(Tables.meta(conn), [:named_table, :set, :public])
  end

  defp maybe_delete_txn_tracking_table(conn) do
    if :ets.whereis(Tables.txn_tracking(conn)) != :undefined do
      :ets.delete(Tables.txn_tracking(conn))
    end
  end

  defp maybe_delete_meta_table(conn) do
    if :ets.whereis(Tables.meta(conn)) != :undefined do
      :ets.delete(Tables.meta(conn))
    end
  end
end
