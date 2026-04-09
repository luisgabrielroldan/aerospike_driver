defmodule Aerospike.Protocol.ScanResponseTest do
  use ExUnit.Case, async: true

  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.ScanResponse

  @namespace "ns1"
  @set "users"

  defp digest_fixture(seed \\ "fixture") do
    :crypto.hash(:ripemd160, seed)
  end

  defp record_msg(opts) do
    digest = Keyword.get(opts, :digest, digest_fixture())
    gen = Keyword.get(opts, :generation, 5)
    exp = Keyword.get(opts, :expiration, 120)

    %AsmMsg{
      info1: AsmMsg.info1_read(),
      result_code: 0,
      generation: gen,
      expiration: exp,
      fields: [
        Field.namespace(@namespace),
        Field.set(@set),
        Field.digest(digest)
      ],
      operations: [
        %Operation{
          op_type: Operation.op_read(),
          particle_type: Operation.particle_integer(),
          bin_name: "age",
          data: <<42::64-signed-big>>
        }
      ]
    }
  end

  defp last_msg(opts \\ []) do
    rc = Keyword.get(opts, :rc, 0)

    %AsmMsg{
      info3: AsmMsg.info3_last(),
      result_code: rc,
      fields: [],
      operations: []
    }
  end

  defp partition_done_msg(digest, opts \\ []) do
    bval_data = Keyword.get(opts, :bval_data)
    partition_id = Keyword.get(opts, :partition_id, partition_id_from_digest(digest))

    fields =
      [Field.digest(digest)] ++
        if bval_data do
          [%Field{type: Field.type_bval_array(), data: bval_data}]
        else
          []
        end

    %AsmMsg{
      info3: AsmMsg.info3_partition_done(),
      result_code: 0,
      generation: partition_id,
      fields: fields,
      operations: []
    }
  end

  defp partition_id_from_digest(digest) do
    Key.partition_id(Key.from_digest(@namespace, @set, digest))
  end

  defp encode_bin(msg), do: IO.iodata_to_binary(AsmMsg.encode(msg))

  test "parse: single record then LAST" do
    d = digest_fixture("one")
    body = encode_bin(record_msg(digest: d)) <> encode_bin(last_msg())

    assert {:ok, [rec], []} = ScanResponse.parse(body, @namespace, @set)
    assert rec.bins == %{"age" => 42}
    assert rec.generation == 5
    assert rec.ttl == 120
    assert rec.key.digest == d
    assert rec.key.namespace == @namespace
    assert rec.key.set == @set
  end

  test "parse: three records then LAST" do
    d1 = digest_fixture("a")
    d2 = digest_fixture("b")
    d3 = digest_fixture("c")

    body =
      encode_bin(record_msg(digest: d1, generation: 1)) <>
        encode_bin(record_msg(digest: d2, generation: 2)) <>
        encode_bin(record_msg(digest: d3, generation: 3)) <>
        encode_bin(last_msg())

    assert {:ok, records, []} = ScanResponse.parse(body, @namespace, @set)
    assert length(records) == 3
    assert Enum.map(records, & &1.generation) == [1, 2, 3]
  end

  test "parse: empty scan (LAST only, rc 0)" do
    body = encode_bin(last_msg())
    assert {:ok, [], []} = ScanResponse.parse(body, @namespace, @set)
  end

  test "parse: two records without terminal LAST returns error" do
    d1 = digest_fixture("p1")
    d2 = digest_fixture("p2")

    body =
      encode_bin(record_msg(digest: d1, generation: 1)) <>
        encode_bin(record_msg(digest: d2, generation: 2))

    assert {:error, %{code: :parse_error}} = ScanResponse.parse(body, @namespace, @set)

    assert {:ok, records, [], false} = ScanResponse.parse_stream_chunk(body, @namespace, @set)
    assert length(records) == 2
  end

  test "parse: PARTITION_DONE with digest populates partition_done_list" do
    d = digest_fixture("pd")
    expected_id = Key.partition_id(Key.from_digest(@namespace, @set, d))

    body =
      encode_bin(partition_done_msg(d)) <>
        encode_bin(last_msg())

    assert {:ok, [], [info]} = ScanResponse.parse(body, @namespace, @set)
    assert info.id == expected_id
    assert info.digest == d
    assert info.bval == nil
  end

  test "parse: PARTITION_DONE includes bval from BVAL_ARRAY field" do
    d = digest_fixture("bval")
    bval_bin = <<7::64-signed-little>>

    body =
      encode_bin(partition_done_msg(d, bval_data: bval_bin)) <>
        encode_bin(last_msg())

    assert {:ok, [], [info]} = ScanResponse.parse(body, @namespace, @set)
    assert info.bval == 7
  end

  test "parse: LAST frame with fatal result_code returns error" do
    body = encode_bin(last_msg(rc: 9))
    assert {:error, %{code: :timeout}} = ScanResponse.parse(body, @namespace, @set)
  end

  test "parse: LAST frame with key_not_found is acceptable" do
    body = encode_bin(last_msg(rc: 2))
    assert {:ok, [], []} = ScanResponse.parse(body, @namespace, @set)
  end

  test "parse_frame: one record frame" do
    d = digest_fixture("frame")
    bin = encode_bin(record_msg(digest: d))

    assert {:ok, %{records: [rec], partition_done: nil, last?: false}} =
             ScanResponse.parse_frame(bin, @namespace, @set)

    assert rec.bins == %{"age" => 42}
  end

  test "parse_frame: LAST-only frame" do
    bin = encode_bin(last_msg())

    assert {:ok, %{records: [], partition_done: nil, last?: true}} =
             ScanResponse.parse_frame(bin, @namespace, @set)
  end

  test "parse_frame: rejects trailing bytes" do
    bin = encode_bin(last_msg()) <> <<0, 1, 2>>

    assert {:error, %{code: :parse_error}} = ScanResponse.parse_frame(bin, @namespace, @set)
  end

  test "parse: key_not_found RC is skipped (not a stream terminator)" do
    terminal = %AsmMsg{result_code: 2, fields: [], operations: []}
    body = encode_bin(terminal) <> encode_bin(last_msg())

    assert {:ok, [], []} = ScanResponse.parse(body, @namespace, @set)
  end

  test "parse: key_not_found RC without LAST is incomplete" do
    terminal = %AsmMsg{result_code: 2, fields: [], operations: []}
    body = encode_bin(terminal)

    assert {:error, %{code: :parse_error, message: msg}} =
             ScanResponse.parse(body, @namespace, @set)

    assert msg =~ "incomplete"
  end

  describe "count_records/1" do
    test "single record then LAST returns 1" do
      d = digest_fixture("cnt1")
      body = encode_bin(record_msg(digest: d)) <> encode_bin(last_msg())

      assert {:ok, 1} = ScanResponse.count_records(body)
    end

    test "three records then LAST returns 3" do
      d1 = digest_fixture("ca")
      d2 = digest_fixture("cb")
      d3 = digest_fixture("cc")

      body =
        encode_bin(record_msg(digest: d1, generation: 1)) <>
          encode_bin(record_msg(digest: d2, generation: 2)) <>
          encode_bin(record_msg(digest: d3, generation: 3)) <>
          encode_bin(last_msg())

      assert {:ok, 3} = ScanResponse.count_records(body)
    end

    test "empty scan (LAST only) returns 0" do
      body = encode_bin(last_msg())
      assert {:ok, 0} = ScanResponse.count_records(body)
    end

    test "without terminal LAST returns error" do
      d = digest_fixture("cnt_no_last")
      body = encode_bin(record_msg(digest: d))

      assert {:error, %{code: :parse_error}} =
               ScanResponse.count_records(body)
    end

    test "partition_done frames are not counted as records" do
      d = digest_fixture("cnt_pd")

      body =
        encode_bin(partition_done_msg(d)) <>
          encode_bin(last_msg())

      assert {:ok, 0} = ScanResponse.count_records(body)
    end

    test "records mixed with partition_done counts only records" do
      dr = digest_fixture("cnt_rec")
      dp = digest_fixture("cnt_part")

      body =
        encode_bin(record_msg(digest: dr)) <>
          encode_bin(partition_done_msg(dp)) <>
          encode_bin(last_msg())

      assert {:ok, 1} = ScanResponse.count_records(body)
    end

    test "LAST with fatal result_code returns error" do
      body = encode_bin(last_msg(rc: 9))
      assert {:error, %{code: :timeout}} = ScanResponse.count_records(body)
    end

    test "key_not_found RC is skipped (not a stream terminator)" do
      terminal = %AsmMsg{result_code: 2, fields: [], operations: []}
      body = encode_bin(terminal) <> encode_bin(last_msg())

      assert {:ok, 0} = ScanResponse.count_records(body)
    end

    test "key_not_found RC without LAST is incomplete" do
      terminal = %AsmMsg{result_code: 2, fields: [], operations: []}
      body = encode_bin(terminal)

      assert {:error, %{code: :parse_error, message: msg}} =
               ScanResponse.count_records(body)

      assert msg =~ "incomplete"
    end

    test "agrees with parse record count" do
      d1 = digest_fixture("agree_a")
      d2 = digest_fixture("agree_b")

      body =
        encode_bin(record_msg(digest: d1, generation: 1)) <>
          encode_bin(record_msg(digest: d2, generation: 2)) <>
          encode_bin(last_msg())

      {:ok, records, _parts} = ScanResponse.parse(body, @namespace, @set)
      {:ok, count} = ScanResponse.count_records(body)
      assert count == length(records)
    end
  end

  describe "parse_stream_chunk/3" do
    test "returns done? = true when LAST frame terminates the chunk" do
      d = digest_fixture("stream_done")
      body = encode_bin(record_msg(digest: d)) <> encode_bin(last_msg())

      assert {:ok, [rec], [], true} = ScanResponse.parse_stream_chunk(body, @namespace, @set)
      assert rec.bins == %{"age" => 42}
    end

    test "empty body returns done? = false" do
      assert {:ok, [], [], false} = ScanResponse.parse_stream_chunk(<<>>, @namespace, @set)
    end

    test "LAST-only chunk returns done? = true with no records" do
      body = encode_bin(last_msg())
      assert {:ok, [], [], true} = ScanResponse.parse_stream_chunk(body, @namespace, @set)
    end

    test "partition_done + LAST returns done? = true with partition info" do
      d = digest_fixture("stream_pd")
      body = encode_bin(partition_done_msg(d)) <> encode_bin(last_msg())

      assert {:ok, [], [info], true} = ScanResponse.parse_stream_chunk(body, @namespace, @set)
      assert info.digest == d
    end

    test "fatal LAST returns error" do
      body = encode_bin(last_msg(rc: 9))

      assert {:error, %{code: :timeout}} =
               ScanResponse.parse_stream_chunk(body, @namespace, @set)
    end
  end

  describe "parse with user key restoration" do
    test "integer user key is restored from KEY field" do
      d = digest_fixture("int_uk")

      msg = %AsmMsg{
        info1: AsmMsg.info1_read(),
        result_code: 0,
        generation: 1,
        expiration: 60,
        fields: [
          Field.namespace(@namespace),
          Field.set(@set),
          Field.digest(d),
          Field.key(1, <<99::64-signed-big>>)
        ],
        operations: [
          %Operation{
            op_type: Operation.op_read(),
            particle_type: Operation.particle_integer(),
            bin_name: "x",
            data: <<1::64-signed-big>>
          }
        ]
      }

      body = encode_bin(msg) <> encode_bin(last_msg())
      assert {:ok, [rec], []} = ScanResponse.parse(body, @namespace, @set)
      assert rec.key.user_key == 99
    end

    test "string user key is restored from KEY field" do
      d = digest_fixture("str_uk")

      msg = %AsmMsg{
        info1: AsmMsg.info1_read(),
        result_code: 0,
        generation: 1,
        expiration: 60,
        fields: [
          Field.namespace(@namespace),
          Field.set(@set),
          Field.digest(d),
          Field.key(3, "mykey")
        ],
        operations: [
          %Operation{
            op_type: Operation.op_read(),
            particle_type: Operation.particle_string(),
            bin_name: "s",
            data: "val"
          }
        ]
      }

      body = encode_bin(msg) <> encode_bin(last_msg())
      assert {:ok, [rec], []} = ScanResponse.parse(body, @namespace, @set)
      assert rec.key.user_key == "mykey"
    end

    test "missing KEY field leaves user_key as nil" do
      d = digest_fixture("no_uk")
      body = encode_bin(record_msg(digest: d)) <> encode_bin(last_msg())

      assert {:ok, [rec], []} = ScanResponse.parse(body, @namespace, @set)
      assert rec.key.user_key == nil
    end
  end

  describe "lazy_stream_chunk_terminal?/1" do
    test "returns true for LAST frame" do
      body = encode_bin(last_msg())
      assert ScanResponse.lazy_stream_chunk_terminal?(body) == true
    end

    test "returns false for record without LAST" do
      d = digest_fixture("lazy_cont")
      body = encode_bin(record_msg(digest: d))
      assert ScanResponse.lazy_stream_chunk_terminal?(body) == false
    end

    test "returns true for record then LAST" do
      d = digest_fixture("lazy_done")
      body = encode_bin(record_msg(digest: d)) <> encode_bin(last_msg())
      assert ScanResponse.lazy_stream_chunk_terminal?(body) == true
    end

    test "returns true for error result code" do
      error_msg = %AsmMsg{result_code: 9, fields: [], operations: []}
      body = encode_bin(error_msg)
      assert ScanResponse.lazy_stream_chunk_terminal?(body) == true
    end

    test "returns false for key_not_found frame (per-record skip, not terminal)" do
      msg = %AsmMsg{result_code: 2, fields: [], operations: []}
      body = encode_bin(msg)
      assert ScanResponse.lazy_stream_chunk_terminal?(body) == false
    end

    test "returns false for filtered_out frame (per-record skip, not terminal)" do
      msg = %AsmMsg{result_code: 27, fields: [], operations: []}
      body = encode_bin(msg)
      assert ScanResponse.lazy_stream_chunk_terminal?(body) == false
    end

    test "returns true on short/invalid data" do
      assert ScanResponse.lazy_stream_chunk_terminal?(<<1, 2, 3>>) == true
    end

    test "returns true for LAST with trailing bytes" do
      body = encode_bin(last_msg()) <> <<0, 0>>
      assert ScanResponse.lazy_stream_chunk_terminal?(body) == true
    end

    test "returns true for partition_done + LAST" do
      d = digest_fixture("lazy_pd")
      body = encode_bin(partition_done_msg(d)) <> encode_bin(last_msg())
      assert ScanResponse.lazy_stream_chunk_terminal?(body) == true
    end
  end

  describe "parse: set_name defaults" do
    test "set_name defaults to nil (empty string internally)" do
      d = digest_fixture("default_set")
      body = encode_bin(record_msg(digest: d)) <> encode_bin(last_msg())

      assert {:ok, [rec], []} = ScanResponse.parse(body, @namespace)
      assert rec.key.namespace == @namespace
    end
  end

  describe "malformed input" do
    @info3_last 0x01

    defp malformed_header(opts) do
      info3 = Keyword.get(opts, :info3, 0)
      rc = Keyword.get(opts, :rc, 0)
      gen = Keyword.get(opts, :generation, 0)
      exp = Keyword.get(opts, :expiration, 0)
      timeout = Keyword.get(opts, :timeout, 0)
      field_count = Keyword.get(opts, :field_count, 0)
      op_count = Keyword.get(opts, :op_count, 0)

      <<22::8, 0::8, 0::8, info3::8, 0::8, rc::8, gen::32-big, exp::32-big,
        timeout::32-signed-big, field_count::16-big, op_count::16-big>>
    end

    test "shorter-than-header payload returns parse_error" do
      assert {:error, %{code: :parse_error}} = ScanResponse.parse(<<1, 2, 3>>, @namespace, @set)
    end

    test "field_count larger than available fields returns parse_error" do
      # field_count claims 3 fields, but only one field is present.
      header = malformed_header(field_count: 3, info3: @info3_last)
      one_field = <<4::32-big, Field.type_namespace(), "ns1">>

      assert {:error, %{code: :parse_error}} =
               ScanResponse.parse(header <> one_field, @namespace, @set)
    end

    test "op_count larger than available operations returns parse_error" do
      # op_count claims 2 operations, but only one operation is present.
      header = malformed_header(op_count: 2, info3: @info3_last)
      one_op = Operation.encode(Operation.read("bin"))

      assert {:error, %{code: :parse_error}} =
               ScanResponse.parse(header <> one_op, @namespace, @set)
    end

    test "LAST with unknown result_code 255 returns server_error" do
      body = encode_bin(last_msg(rc: 255))

      assert {:error, %{code: :server_error}} = ScanResponse.parse(body, @namespace, @set)
    end

    test "field advertises huge size but payload is truncated" do
      header = malformed_header(field_count: 1, info3: @info3_last)
      huge_field = <<2_147_483_647::32-big, Field.type_digest(), 1, 2, 3, 4, 5>>

      assert {:error, %{code: :parse_error}} =
               ScanResponse.parse(header <> huge_field, @namespace, @set)
    end

    test "duplicate LAST frames in one body return parse_error" do
      body = encode_bin(last_msg()) <> encode_bin(last_msg())

      assert {:error, %{code: :parse_error}} = ScanResponse.parse(body, @namespace, @set)
    end
  end

  describe "skip_fields edge cases (via count_records)" do
    defp raw_header(opts) do
      info3 = Keyword.get(opts, :info3, 0)
      rc = Keyword.get(opts, :rc, 0)
      gen = Keyword.get(opts, :generation, 0)
      exp = Keyword.get(opts, :expiration, 0)
      timeout = Keyword.get(opts, :timeout, 0)
      field_count = Keyword.get(opts, :field_count, 0)
      op_count = Keyword.get(opts, :op_count, 0)

      <<22::8, 0::8, 0::8, info3::8, 0::8, rc::8, gen::32-big, exp::32-big,
        timeout::32-signed-big, field_count::16-big, op_count::16-big>>
    end

    @info3_last 0x01

    test "field size claims more bytes than remain (truncated mid-field)" do
      header = raw_header(field_count: 1, info3: @info3_last)
      field_bytes = <<100::32-big, 1, 2, 3, 4, 5>>
      body = header <> field_bytes

      assert {:error, %{code: :parse_error}} = ScanResponse.count_records(body)
    end

    test "field_count: 2 but only one field present" do
      header = raw_header(field_count: 2, info3: @info3_last)
      one_field = <<4::32-big, 0, 1, 2, 3>>
      body = header <> one_field

      assert {:error, %{code: :parse_error}} = ScanResponse.count_records(body)
    end

    test "zero-size field" do
      header = raw_header(field_count: 1, info3: @info3_last)
      zero_field = <<0::32-big, 99>>
      body = header <> zero_field

      assert {:error, %{code: :parse_error}} = ScanResponse.count_records(body)
    end

    test "no field bytes at all with field_count > 0" do
      header = raw_header(field_count: 1, info3: @info3_last)

      assert {:error, %{code: :parse_error}} = ScanResponse.count_records(header)
    end
  end
end
