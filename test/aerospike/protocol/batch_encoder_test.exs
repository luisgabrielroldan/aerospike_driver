defmodule Aerospike.Protocol.BatchEncoderTest do
  use ExUnit.Case, async: true

  import Aerospike.Op

  alias Aerospike.Batch
  alias Aerospike.Exp
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.BatchEncoder
  alias Aerospike.Protocol.Message

  setup do
    key = Key.new("testns", "set1", "encoder-ut")
    {:ok, key: key}
  end

  test "encode_batch_get/2 wraps AS_MSG with INFO1_BATCH and batch field", %{key: key} do
    wire = BatchEncoder.encode_batch_get([{0, key}], timeout: 1500)
    assert {:ok, {2, 3, body}} = Message.decode(wire)
    assert byte_size(body) >= 22
    # Inner AS_MSG: first byte is header size, second is info1.
    <<_sz::8, info1::8, _::binary>> = body
    assert info1 == AsmMsg.info1_batch()
    assert :binary.match(body, key.digest) != :nomatch
  end

  test "encode_batch_exists/2 is batch-framed", %{key: key} do
    wire = BatchEncoder.encode_batch_exists([{0, key}], [])
    assert {:ok, {2, 3, body}} = Message.decode(wire)
    <<_sz::8, info1::8, _::binary>> = body
    assert info1 == AsmMsg.info1_batch()
  end

  test "encode_batch_get/2 empty list still builds valid message" do
    wire = BatchEncoder.encode_batch_get([], timeout: 0)
    assert {:ok, {2, 3, _body}} = Message.decode(wire)
  end

  test "encode_batch_get/2 header_only, atom bin names, repeat ns/set, filter and flags", %{
    key: key
  } do
    wire = BatchEncoder.encode_batch_get([{0, key}], header_only: true, timeout: 100)
    assert {:ok, _} = Message.decode(wire)

    wire_bins = BatchEncoder.encode_batch_get([{0, key}], bins: [:z_bin], timeout: 50)
    assert {:ok, _} = Message.decode(wire_bins)

    k2 = Key.new(key.namespace, key.set, "encoder-ut-b")
    wire_rep = BatchEncoder.encode_batch_get([{0, key}, {1, k2}], timeout: 50)
    assert {:ok, _} = Message.decode(wire_rep)

    wire_f =
      BatchEncoder.encode_batch_get([{0, key}], filter: Exp.from_wire(<<1, 2, 3>>), timeout: 0)

    assert {:ok, _} = Message.decode(wire_f)

    wire_ra = BatchEncoder.encode_batch_get([{0, key}], respond_all_keys: false, timeout: 0)
    assert {:ok, _} = Message.decode(wire_ra)
  end

  test "encode_batch_operate/2 covers read, put, delete, udf, operate read and write", %{key: key} do
    k2 = Key.new(key.namespace, key.set, "encoder-op-b")

    wire =
      BatchEncoder.encode_batch_operate(
        [
          {0, Batch.read(key, bins: ["a"])},
          {1,
           Batch.put(k2, %{"a" => 1},
             exists: :create_only,
             generation: 1,
             gen_policy: :expect_gen_equal
           )},
          {2, Batch.delete(k2, durable_delete: true)},
          {3,
           Batch.udf(key, "pkg", "fn", [1, "x"],
             durable_delete: true,
             commit_master: true,
             ttl: 10
           )},
          {4, Batch.operate(key, [get("a")], read_touch_ttl_percent: 50)},
          {5, Batch.operate(k2, [put("w", 1)], exists: :update_only, respond_per_each_op: true)},
          {6, Batch.put(k2, %{"x" => 1}, exists: :replace_only)},
          {7, Batch.put(k2, %{"y" => 1}, exists: :create_or_replace)}
        ],
        []
      )

    assert {:ok, _} = Message.decode(wire)
  end

  test "encode_batch_operate/2 repeat ns/set and send_key", %{key: key} do
    wire_rep_reads =
      BatchEncoder.encode_batch_operate(
        [{0, Batch.read(key)}, {1, Batch.read(key)}],
        []
      )

    assert {:ok, _} = Message.decode(wire_rep_reads)

    wire_rep_puts =
      BatchEncoder.encode_batch_operate(
        [
          {0, Batch.put(key, %{"a" => 1}, send_key: true)},
          {1, Batch.put(key, %{"b" => 2}, send_key: true)}
        ],
        []
      )

    assert {:ok, _} = Message.decode(wire_rep_puts)

    wire_rep_del =
      BatchEncoder.encode_batch_operate(
        [
          {0, Batch.delete(key, send_key: true)},
          {1, Batch.delete(key, send_key: true)}
        ],
        []
      )

    assert {:ok, _} = Message.decode(wire_rep_del)

    wire_rep_op =
      BatchEncoder.encode_batch_operate(
        [
          {0, Batch.operate(key, [get("a")], send_key: true)},
          {1, Batch.operate(key, [get("b")], send_key: true)}
        ],
        []
      )

    assert {:ok, _} = Message.decode(wire_rep_op)

    wire_rep_udf =
      BatchEncoder.encode_batch_operate(
        [
          {0, Batch.udf(key, "p", "f", [], send_key: true)},
          {1, Batch.udf(key, "p", "f", [], send_key: true)}
        ],
        []
      )

    assert {:ok, _} = Message.decode(wire_rep_udf)

    wire_hdr =
      BatchEncoder.encode_batch_operate(
        [{0, Batch.operate(key, [get("only")], header_only: true)}],
        []
      )

    assert {:ok, _} = Message.decode(wire_hdr)
  end
end
