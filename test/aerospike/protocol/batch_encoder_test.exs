defmodule Aerospike.Protocol.BatchEncoderTest do
  use ExUnit.Case, async: true

  import Aerospike.Op
  import Bitwise

  alias Aerospike.Batch
  alias Aerospike.Exp
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.BatchEncoder
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.MessagePack

  # Decode the outer wire message, then extract the BATCH_INDEX field data.
  # Returns {outer_msg, batch_body} where batch_body is the raw batch index payload.
  defp decode_batch_body(wire) do
    {:ok, {2, 3, body}} = wire |> IO.iodata_to_binary() |> Message.decode()
    {:ok, msg} = AsmMsg.decode(body)
    [%Field{type: 41, data: batch_body}] = Enum.filter(msg.fields, &(&1.type == 41))
    {msg, batch_body}
  end

  defp single_operate_entry_inner(batch_body) do
    <<_key_count::32-big, _flags::8, _idx::32-big, _digest::binary-20, inner::binary>> =
      batch_body

    inner
  end

  defp decode_batch_inner_fields(inner) do
    <<_entry_flags::8, _r::8, _w::8, _i::8, _gen::16-big, _exp::32-big, field_count::16-big,
      op_count::16-big, rest::binary>> = inner

    {fields, remaining} = decode_n_fields(rest, field_count, [])
    {field_count, op_count, fields, remaining}
  end

  defp decode_n_fields(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp decode_n_fields(rest, n, acc) when n > 0 do
    {:ok, field, rest_after_field} = Field.decode(rest)
    decode_n_fields(rest_after_field, n - 1, [field | acc])
  end

  setup do
    key = Key.new("testns", "set1", "encoder-ut")
    {:ok, key: key}
  end

  test "encode_batch_get/2 wraps AS_MSG with INFO1_BATCH and batch field", %{key: key} do
    wire = BatchEncoder.encode_batch_get([{0, key}], timeout: 1500) |> IO.iodata_to_binary()
    assert {:ok, {2, 3, body}} = Message.decode(wire)
    assert byte_size(body) >= 22
    # Inner AS_MSG: first byte is header size, second is info1.
    <<_sz::8, info1::8, _::binary>> = body
    assert info1 == AsmMsg.info1_batch()
    assert :binary.match(body, key.digest) != :nomatch
  end

  test "encode_batch_exists/2 is batch-framed", %{key: key} do
    wire = BatchEncoder.encode_batch_exists([{0, key}], []) |> IO.iodata_to_binary()
    assert {:ok, {2, 3, body}} = Message.decode(wire)
    <<_sz::8, info1::8, _::binary>> = body
    assert info1 == AsmMsg.info1_batch()
  end

  test "encode_batch_get/2 empty list still builds valid message" do
    wire = BatchEncoder.encode_batch_get([], timeout: 0) |> IO.iodata_to_binary()
    assert {:ok, {2, 3, _body}} = Message.decode(wire)
  end

  test "encode_batch_get/2 header_only, atom bin names, repeat ns/set, filter and flags", %{
    key: key
  } do
    wire = BatchEncoder.encode_batch_get([{0, key}], header_only: true, timeout: 100)
    assert {:ok, _} = wire |> IO.iodata_to_binary() |> Message.decode()

    wire_bins = BatchEncoder.encode_batch_get([{0, key}], bins: [:z_bin], timeout: 50)
    assert {:ok, _} = wire_bins |> IO.iodata_to_binary() |> Message.decode()

    k2 = Key.new(key.namespace, key.set, "encoder-ut-b")
    wire_rep = BatchEncoder.encode_batch_get([{0, key}, {1, k2}], timeout: 50)
    assert {:ok, _} = wire_rep |> IO.iodata_to_binary() |> Message.decode()

    wire_f =
      BatchEncoder.encode_batch_get([{0, key}], filter: Exp.from_wire(<<1, 2, 3>>), timeout: 0)

    assert {:ok, _} = wire_f |> IO.iodata_to_binary() |> Message.decode()

    wire_ra = BatchEncoder.encode_batch_get([{0, key}], respond_all_keys: false, timeout: 0)
    assert {:ok, _} = wire_ra |> IO.iodata_to_binary() |> Message.decode()
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

    assert {:ok, _} = wire |> IO.iodata_to_binary() |> Message.decode()
  end

  test "encode_batch_operate/2 repeat ns/set and send_key", %{key: key} do
    wire_rep_reads =
      BatchEncoder.encode_batch_operate(
        [{0, Batch.read(key)}, {1, Batch.read(key)}],
        []
      )

    assert {:ok, _} = wire_rep_reads |> IO.iodata_to_binary() |> Message.decode()

    wire_rep_puts =
      BatchEncoder.encode_batch_operate(
        [
          {0, Batch.put(key, %{"a" => 1}, send_key: true)},
          {1, Batch.put(key, %{"b" => 2}, send_key: true)}
        ],
        []
      )

    assert {:ok, _} = wire_rep_puts |> IO.iodata_to_binary() |> Message.decode()

    wire_rep_del =
      BatchEncoder.encode_batch_operate(
        [
          {0, Batch.delete(key, send_key: true)},
          {1, Batch.delete(key, send_key: true)}
        ],
        []
      )

    assert {:ok, _} = wire_rep_del |> IO.iodata_to_binary() |> Message.decode()

    wire_rep_op =
      BatchEncoder.encode_batch_operate(
        [
          {0, Batch.operate(key, [get("a")], send_key: true)},
          {1, Batch.operate(key, [get("b")], send_key: true)}
        ],
        []
      )

    assert {:ok, _} = wire_rep_op |> IO.iodata_to_binary() |> Message.decode()

    wire_rep_udf =
      BatchEncoder.encode_batch_operate(
        [
          {0, Batch.udf(key, "p", "f", [], send_key: true)},
          {1, Batch.udf(key, "p", "f", [], send_key: true)}
        ],
        []
      )

    assert {:ok, _} = wire_rep_udf |> IO.iodata_to_binary() |> Message.decode()

    wire_hdr =
      BatchEncoder.encode_batch_operate(
        [{0, Batch.operate(key, [get("only")], header_only: true)}],
        []
      )

    assert {:ok, _} = wire_hdr |> IO.iodata_to_binary() |> Message.decode()
  end

  describe "byte-layout assertions" do
    test "outer AS_MSG BATCH_INDEX field has type byte 41 (Field.type_batch_index)" do
      key = Key.new("testns", "set1", "enc-t1")
      wire = BatchEncoder.encode_batch_get([{0, key}], [])
      {:ok, {2, 3, body}} = wire |> IO.iodata_to_binary() |> Message.decode()
      {:ok, msg} = AsmMsg.decode(body)
      # There must be exactly one batch_index field (type 41)
      batch_field = Enum.find(msg.fields, &(&1.type == Field.type_batch_index()))
      refute is_nil(batch_field)
    end

    test "batch index body encodes key count as big-endian uint32 in bytes 0..3" do
      key1 = Key.new("testns", "set1", "enc-t2a")
      key2 = Key.new("testns", "set1", "enc-t2b")
      key3 = Key.new("testns", "set1", "enc-t2c")
      wire = BatchEncoder.encode_batch_get([{0, key1}, {1, key2}, {2, key3}], [])
      {_msg, batch_body} = decode_batch_body(wire)
      # bytes 0-3: key count (big-endian uint32)
      <<key_count::32-big, _rest::binary>> = batch_body
      assert key_count == 3
    end

    test "encode_batch_get uses read_attr 0x03 (INFO1_READ | INFO1_GET_ALL) at inner byte 1" do
      key = Key.new("testns", "set1", "enc-t3")
      wire = BatchEncoder.encode_batch_get([{0, key}], [])
      {_msg, batch_body} = decode_batch_body(wire)

      # batch_body layout:
      #   bytes 0-3:  key_count (4)
      #   byte  4:    flags (1)
      #   bytes 5-8:  index[0] (4)
      #   bytes 9-28: digest[0] (20)
      #   byte  29:   inner byte 0 = entry_flags (batch_msg_info | batch_msg_ttl = 0x0A)
      #   byte  30:   inner byte 1 = read_attr
      <<_kc::32, _flags::8, _idx::32, _digest::binary-20, _entry_flags::8, read_attr::8,
        _::binary>> = batch_body

      # INFO1_READ (0x01) | INFO1_GET_ALL (0x02) = 0x03
      assert read_attr == (AsmMsg.info1_read() ||| AsmMsg.info1_get_all())
    end

    test "encode_batch_exists uses read_attr 0x21 (INFO1_READ | INFO1_NOBINDATA) at inner byte 1" do
      key = Key.new("testns", "set1", "enc-t4")
      wire = BatchEncoder.encode_batch_exists([{0, key}], [])
      {_msg, batch_body} = decode_batch_body(wire)

      # Same layout as batch_get; only read_attr changes
      <<_kc::32, _flags::8, _idx::32, _digest::binary-20, _entry_flags::8, read_attr::8,
        _::binary>> = batch_body

      # INFO1_READ (0x01) | INFO1_NOBINDATA (0x20) = 0x21
      assert read_attr == (AsmMsg.info1_read() ||| AsmMsg.info1_nobindata())
    end

    test "encode_batch_get with header_only: true uses read_attr 0x21 (same as exists)" do
      key = Key.new("testns", "set1", "enc-t5")
      wire = BatchEncoder.encode_batch_get([{0, key}], header_only: true)
      {_msg, batch_body} = decode_batch_body(wire)

      <<_kc::32, _flags::8, _idx::32, _digest::binary-20, _entry_flags::8, read_attr::8,
        _::binary>> = batch_body

      # header_only suppresses bin data the same way as exists
      assert read_attr == (AsmMsg.info1_read() ||| AsmMsg.info1_nobindata())
    end

    test "per-key entry digest matches key.digest at bytes 5..24 of entries region" do
      key = Key.new("testns", "set1", "enc-t6")
      wire = BatchEncoder.encode_batch_get([{0, key}], [])
      {_msg, batch_body} = decode_batch_body(wire)

      # batch_body:  key_count(4) + flags(1) + index(4) + digest(20) + ...
      <<_kc::32, _flags::8, _idx::32, digest::binary-20, _::binary>> = batch_body
      assert digest == key.digest
    end

    test "second key with same ns+set emits repeat flag 0x01 as sole inner byte" do
      key1 = Key.new("testns", "set1", "enc-t7a")
      key2 = Key.new("testns", "set1", "enc-t7b")
      wire = BatchEncoder.encode_batch_get([{0, key1}, {1, key2}], [])
      {_msg, batch_body} = decode_batch_body(wire)

      # First entry (key1) is non-repeat. Its inner size for ns="testns"(6), set="set1"(4):
      #   entry header:    8 bytes  (flags, read_attr, 0, 0, ttl::32)
      #   fields_part fc:  4 bytes  (written_fc::16, op_count::16)
      #   namespace field: 11 bytes (size::32=7, type=0, "testns")
      #   set field:        9 bytes (size::32=5, type=1, "set1")
      #   total inner:     32 bytes
      # First entry total: 4 (idx) + 20 (digest) + 32 (inner) = 56 bytes
      <<_kc::32, _flags::8, _entry1::binary-56, _idx2::32, _dig2::binary-20, repeat::8,
        _::binary>> = batch_body

      # @batch_msg_repeat = 0x01
      assert repeat == 0x01
    end

    test "second key with different ns emits non-repeat entry (inner byte 0 = 0x0A)" do
      key1 = Key.new("ns1", "set1", "enc-t8a")
      key2 = Key.new("ns2", "set1", "enc-t8b")
      wire = BatchEncoder.encode_batch_get([{0, key1}, {1, key2}], [])
      {_msg, batch_body} = decode_batch_body(wire)

      # First entry inner size for ns="ns1"(3), set="set1"(4):
      #   entry header:     8 bytes
      #   fields_part fc:   4 bytes
      #   namespace field:  8 bytes (size::32=4, type=0, "ns1")
      #   set field:        9 bytes (size::32=5, type=1, "set1")
      #   total inner:     29 bytes
      # First entry total: 4 (idx) + 20 (digest) + 29 (inner) = 53 bytes
      <<_kc::32, _flags::8, _entry1::binary-53, _idx2::32, _dig2::binary-20, second_flags::8,
        _::binary>> = batch_body

      # Not a repeat; should be batch_msg_info | batch_msg_ttl = 0x0A
      assert second_flags == 0x0A
    end

    test "encode_batch_operate Batch.Put has INFO2_WRITE and INFO2_RESPOND_ALL_OPS in info2 byte" do
      key = Key.new("testns", "set1", "enc-t9")
      wire = BatchEncoder.encode_batch_operate([{0, Batch.put(key, %{"x" => 1})}], [])
      {_msg, batch_body} = decode_batch_body(wire)

      # Batch.Put inner entry layout:
      #   batch_body:  key_count(4) + flags(1) + index(4) + digest(20) + ...
      #   inner:       entry_flags(1) + r/info1(1) + w/info2(1) + i/info3(1) + ...
      <<_kc::32, _flags::8, _idx::32, _digest::binary-20, _entry_flags::8, _r::8, w::8,
        _::binary>> = batch_body

      # INFO2_WRITE (0x01) must be set
      assert (w &&& AsmMsg.info2_write()) != 0
      # INFO2_RESPOND_ALL_OPS (0x80) must be set (put! always requests all-ops response)
      assert (w &&& AsmMsg.info2_respond_all_ops()) != 0
    end

    test "filter expression option adds FILTER_EXP field (type 43) to outer AS_MSG" do
      key = Key.new("testns", "set1", "enc-t10")
      filter = Exp.from_wire(<<0xDE, 0xAD, 0xBE, 0xEF>>)
      wire = BatchEncoder.encode_batch_get([{0, key}], filter: filter)
      {:ok, {2, 3, body}} = wire |> IO.iodata_to_binary() |> Message.decode()
      {:ok, msg} = AsmMsg.decode(body)

      filter_field = Enum.find(msg.fields, &(&1.type == Field.type_filter_exp()))
      refute is_nil(filter_field)
      # filter data must carry the raw wire bytes we provided
      assert filter_field.data == <<0xDE, 0xAD, 0xBE, 0xEF>>
      # outer AS_MSG must have exactly 2 fields: batch_index + filter_exp
      assert length(msg.fields) == 2
    end

    test "empty entries list encodes key_count 0 and valid outer message" do
      wire = BatchEncoder.encode_batch_get([], [])
      {_msg, batch_body} = decode_batch_body(wire)
      # bytes 0-3: key_count = 0
      <<key_count::32-big, _rest::binary>> = batch_body
      assert key_count == 0
    end

    test "batch UDF entry has ns, set, and 3 UDF fields (no UDF_OP) with zero ops", %{
      key: key
    } do
      wire =
        BatchEncoder.encode_batch_operate(
          [{0, Batch.udf(key, "pkg", "fn", ["x"])}],
          []
        )

      {_msg, batch_body} = decode_batch_body(wire)
      inner = single_operate_entry_inner(batch_body)
      {field_count, op_count, fields, remaining} = decode_batch_inner_fields(inner)

      assert op_count == 0
      assert remaining == <<>>
      assert field_count == 5

      batch_udf_types = Enum.map(fields, & &1.type)

      assert batch_udf_types == [
               Field.type_namespace(),
               Field.type_table(),
               Field.type_udf_package_name(),
               Field.type_udf_function(),
               Field.type_udf_arglist()
             ]

      refute Enum.member?(batch_udf_types, Field.type_udf_op())
    end

    test "batch UDF arglist uses particle-string encoding matching apply_udf semantics", %{
      key: key
    } do
      wire =
        BatchEncoder.encode_batch_operate(
          [{0, Batch.udf(key, "pkg", "fn", ["x"])}],
          []
        )

      {_msg, batch_body} = decode_batch_body(wire)
      inner = single_operate_entry_inner(batch_body)
      {_field_count, _op_count, fields, _remaining} = decode_batch_inner_fields(inner)
      arglist_field = Enum.find(fields, &(&1.type == Field.type_udf_arglist()))

      refute is_nil(arglist_field)
      assert arglist_field.data == MessagePack.pack!([{:particle_string, "x"}])
      assert arglist_field.data != MessagePack.pack!(["x"])
    end
  end
end
