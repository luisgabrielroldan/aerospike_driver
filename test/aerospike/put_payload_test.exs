defmodule Aerospike.PutPayloadTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.Message
  alias Aerospike.Tables
  alias Aerospike.Txn
  alias Aerospike.TxnOps

  # ── Unit: guard / validation ─────────────────────────────────────────────────

  describe "put_payload/4 — payload guard" do
    test "raises FunctionClauseError when payload is not a binary" do
      key = Key.new("test", "pp_unit", "k1")

      assert_raise FunctionClauseError, fn ->
        Aerospike.put_payload(:any_conn, key, 123)
      end

      assert_raise FunctionClauseError, fn ->
        Aerospike.put_payload(:any_conn, key, :not_binary)
      end

      assert_raise FunctionClauseError, fn ->
        Aerospike.put_payload(:any_conn, key, nil)
      end
    end

    test "raises FunctionClauseError when payload is a list (iodata), not a binary" do
      key = Key.new("test", "pp_unit", "k1")

      assert_raise FunctionClauseError, fn ->
        Aerospike.put_payload(:any_conn, key, [<<0, 1, 2>>])
      end
    end

    test "an empty binary is accepted as payload (no non-empty check)" do
      # Empty binary passes the is_binary/1 guard; the server is the validator.
      # Without a running cluster this will fail at routing, not at validation.
      conn = setup_unready_conn()

      key = Key.new("test", "pp_unit", "empty")
      result = Aerospike.put_payload(conn, key, <<>>)
      # Must not be a parameter_error — cluster_not_ready is the expected failure.
      assert {:error, %Error{code: :cluster_not_ready}} = result
    end
  end

  describe "put_payload/4 — policy validation" do
    test "unknown option returns parameter_error" do
      key = Key.new("test", "pp_unit", "k2")

      assert {:error, %Error{code: :parameter_error}} =
               Aerospike.put_payload(:any_conn, key, <<1>>, unknown_opt: true)
    end

    test "valid write policy options are accepted (reach routing, not parameter_error)" do
      conn = setup_unready_conn()
      key = Key.new("test", "pp_unit", "k3")

      result = Aerospike.put_payload(conn, key, <<1>>, timeout: 1_000, send_key: true)
      assert {:error, %Error{code: :cluster_not_ready}} = result
    end
  end

  describe "put_payload/4 — key coercion" do
    test "accepts tuple key form {namespace, set, user_key}" do
      conn = setup_unready_conn()

      result = Aerospike.put_payload(conn, {"test", "pp_unit", "tuple-k"}, <<1>>)
      assert {:error, %Error{code: :cluster_not_ready}} = result
    end

    test "rejects invalid key tuple shape with parameter_error" do
      assert {:error, %Error{code: :parameter_error}} =
               Aerospike.put_payload(:any_conn, {:invalid}, <<1>>)
    end

    test "rejects non-tuple, non-key argument with parameter_error" do
      assert {:error, %Error{code: :parameter_error}} =
               Aerospike.put_payload(:any_conn, "not-a-key", <<1>>)
    end
  end

  describe "put_payload!/4 — bang variant" do
    test "raises Aerospike.Error on invalid policy option" do
      key = Key.new("test", "pp_unit", "bang-k")

      assert_raise Aerospike.Error, fn ->
        Aerospike.put_payload!(:any_conn, key, <<1>>, bad_opt: true)
      end
    end

    test "raises Aerospike.Error on invalid key shape" do
      assert_raise Aerospike.Error, fn ->
        Aerospike.put_payload!(:any_conn, {:invalid}, <<1>>)
      end
    end
  end

  describe "put_payload/4 — txn monitor bypass" do
    test "does not register key in writes set even when :txn option is passed" do
      conn = :"pp_txn_bypass_#{System.unique_integer([:positive, :monotonic])}"
      txn_table = Tables.txn_tracking(conn)
      meta_table = Tables.meta(conn)
      nodes_table = Tables.nodes(conn)
      parts_table = Tables.partitions(conn)

      :ets.new(txn_table, [:set, :public, :named_table])
      :ets.new(meta_table, [:set, :public, :named_table])
      :ets.new(nodes_table, [:set, :public, :named_table, read_concurrency: true])
      :ets.new(parts_table, [:set, :public, :named_table, read_concurrency: true])

      on_exit(fn ->
        for t <- [txn_table, meta_table, nodes_table, parts_table] do
          try do
            :ets.delete(t)
          catch
            :error, :badarg -> :ok
          end
        end
      end)

      txn = Txn.new()
      TxnOps.init_tracking(conn, txn)
      key = Key.new("test", "pp_unit", "txn-bypass")

      # The call will fail at routing (cluster not ready), but the writes set
      # must not have been touched — put_payload bypasses prepare_txn_write.
      _result = Aerospike.put_payload(conn, key, <<1>>, txn: txn)

      writes = TxnOps.get_writes(conn, txn)
      assert writes == []
    end
  end

  # ── Helpers ──────────────────────────────────────────────────────────────────

  # Creates a minimal ETS-backed connection context that is not cluster-ready.
  defp setup_unready_conn do
    conn = :"pp_unit_conn_#{System.unique_integer([:positive, :monotonic])}"
    meta = Tables.meta(conn)
    nodes = Tables.nodes(conn)
    parts = Tables.partitions(conn)

    :ets.new(meta, [:set, :public, :named_table])
    :ets.new(nodes, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(parts, [:set, :public, :named_table, read_concurrency: true])

    ExUnit.Callbacks.on_exit(fn ->
      for t <- [meta, nodes, parts] do
        try do
          :ets.delete(t)
        catch
          :error, :badarg -> :ok
        end
      end
    end)

    conn
  end

  # Build a put wire message from scratch using the same protocol modules
  # that integration tests and the Helpers module use.
  @doc false
  def build_put_wire(key, bins) do
    ops = Value.encode_bin_operations(bins)
    msg = AsmMsg.write_command(key.namespace, key.set, key.digest, ops)
    IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  end

  # Build a delete wire message.
  @doc false
  def build_delete_wire(key) do
    msg = AsmMsg.delete_command(key.namespace, key.set, key.digest)
    IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  end
end
