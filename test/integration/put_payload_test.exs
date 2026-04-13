defmodule Aerospike.Integration.PutPayloadTest do
  use ExUnit.Case, async: false

  alias Aerospike.Error
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.Message
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers
  alias Aerospike.Txn
  alias Aerospike.TxnOps

  @moduletag :integration

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"pp_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      tend_interval: 60_000,
      defaults: [
        read: [timeout: 2_000],
        write: [timeout: 2_000]
      ]
    ]

    {:ok, _sup} = start_supervised({Aerospike, opts})
    await_cluster_ready(name)

    {:ok, conn: name, host: host, port: port}
  end

  # ── Round-trip parity with put/4 ────────────────────────────────────────────

  test "write payload is equivalent to put/4 for the same key and bins",
       %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "pp_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    bins = %{"name" => "payload-write", "n" => 42}

    # Build the wire message using the same protocol building blocks.
    payload = build_put_wire(key, bins)

    assert :ok = Aerospike.put_payload(conn, key, payload)

    # Verify the record landed on the server identically to a put/4.
    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["name"] == "payload-write"
    assert record.bins["n"] == 42
    assert record.generation >= 1
  end

  test "put_payload!/4 bang variant succeeds and record is readable",
       %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "pp_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    payload = build_put_wire(key, %{"x" => 7})
    assert :ok = Aerospike.put_payload!(conn, key, payload)

    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["x"] == 7
  end

  test "put then overwrite with put_payload produces correct generation",
       %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "pp_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"v" => 1})
    assert {:ok, record_v1} = Aerospike.get(conn, key)

    payload = build_put_wire(key, %{"v" => 2})
    assert :ok = Aerospike.put_payload(conn, key, payload)

    assert {:ok, record_v2} = Aerospike.get(conn, key)
    assert record_v2.bins["v"] == 2
    assert record_v2.generation > record_v1.generation
  end

  # ── Delete-shaped payload ────────────────────────────────────────────────────

  test "delete-shaped payload removes the record",
       %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "pp_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    assert :ok = Aerospike.put(conn, key, %{"a" => 1})
    assert {:ok, true} = Aerospike.exists(conn, key)

    delete_payload = build_delete_wire(key)
    assert :ok = Aerospike.put_payload(conn, key, delete_payload)

    assert {:ok, false} = Aerospike.exists(conn, key)
  end

  # ── Server error mapping ─────────────────────────────────────────────────────

  test "generation mismatch surfaces as generation_error, identical to put/4",
       %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "pp_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    # First write establishes generation 1.
    assert :ok = Aerospike.put(conn, key, %{"g" => 1})

    # Build a payload with CHECK_GENERATION (info2_generation) and generation=0.
    # The server will reject it because the current generation is 1.
    payload = build_put_wire_with_generation(key, %{"g" => 2}, 0)

    assert {:error, %Error{code: :generation_error}} =
             Aerospike.put_payload(conn, key, payload)
  end

  # ── Txn monitor bypass ───────────────────────────────────────────────────────

  test "put_payload does not register key in txn writes set even with :txn opt",
       %{conn: conn, host: host, port: port} do
    key = Helpers.unique_key("test", "pp_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    # Set up a txn tracking table so we can inspect the writes set after the call.
    # We use the facade conn name for the txn table since that is what CRUD uses.
    txn = Txn.new()
    TxnOps.init_tracking(conn, txn)

    writes_before = TxnOps.get_writes(conn, txn)

    # put_payload must bypass prepare_txn_write entirely.
    # The call may succeed or fail (txn fields not in payload); either way the
    # writes set must be unchanged.
    payload = build_put_wire(key, %{"t" => 1})
    _result = Aerospike.put_payload(conn, key, payload, txn: txn)

    writes_after = TxnOps.get_writes(conn, txn)
    assert writes_after == writes_before
    assert writes_after == []
  end

  # ── Helpers ──────────────────────────────────────────────────────────────────

  defp build_put_wire(key, bins) do
    ops = Value.encode_bin_operations(bins)
    msg = AsmMsg.write_command(key.namespace, key.set, key.digest, ops)
    IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  end

  defp build_delete_wire(key) do
    msg = AsmMsg.delete_command(key.namespace, key.set, key.digest)
    IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  end

  # Builds a write payload with INFO2_GENERATION and an explicit generation value.
  # The server enforces CHECK_GENERATION when this flag is set.
  defp build_put_wire_with_generation(key, bins, generation) do
    import Bitwise

    ops = Value.encode_bin_operations(bins)

    msg = %AsmMsg{
      info2: AsmMsg.info2_write() ||| AsmMsg.info2_generation(),
      generation: generation,
      fields: [
        Field.namespace(key.namespace),
        Field.set(key.set),
        Field.digest(key.digest)
      ],
      operations: ops
    }

    IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  end

  defp await_cluster_ready(name, attempts \\ 20)

  defp await_cluster_ready(_name, 0), do: flunk("cluster did not become ready in time")

  defp await_cluster_ready(name, n) do
    meta = Tables.meta(name)

    ready =
      :ets.whereis(meta) != :undefined and
        :ets.lookup(meta, Tables.ready_key()) == [{Tables.ready_key(), true}]

    if ready do
      :ok
    else
      Process.sleep(100)
      await_cluster_ready(name, n - 1)
    end
  end
end
