defmodule Aerospike.Protocol.ScanResponseTest do
  use ExUnit.Case, async: true

  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.ScanResponse

  @namespace "ns1"
  @set "users"
  @info3_partition_done 0x04

  defp digest_fixture(seed) do
    :crypto.hash(:ripemd160, seed)
  end

  defp encode_bin(msg), do: IO.iodata_to_binary(AsmMsg.encode(msg))

  defp record_msg(opts \\ []) do
    digest = Keyword.get(opts, :digest, digest_fixture("record"))
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
          particle_type: 1,
          bin_name: "age",
          data: <<42::64-signed-big>>
        }
      ]
    }
  end

  defp last_msg(opts \\ []) do
    %AsmMsg{
      info3: AsmMsg.info3_last(),
      result_code: Keyword.get(opts, :rc, 0),
      fields: [],
      operations: []
    }
  end

  defp partition_done_msg(digest, opts \\ []) do
    fields = [Field.digest(digest)]

    fields =
      case Keyword.get(opts, :bval) do
        nil ->
          fields

        bval ->
          fields ++ [%Field{type: Field.type_bval_array(), data: <<bval::64-signed-little>>}]
      end

    %AsmMsg{
      info3: @info3_partition_done,
      result_code: Keyword.get(opts, :rc, 0),
      generation:
        Keyword.get(
          opts,
          :partition_id,
          Key.partition_id(%Key{namespace: @namespace, set: @set, digest: digest})
        ),
      fields: fields,
      operations: []
    }
  end

  defp aggregate_msg(result, opts \\ []) do
    %AsmMsg{
      info1: AsmMsg.info1_read(),
      result_code: Keyword.get(opts, :rc, 0),
      fields: [],
      operations: [
        %Operation{
          op_type: Operation.op_read(),
          particle_type: 3,
          bin_name: Keyword.get(opts, :bin_name, "SUCCESS"),
          data: result
        }
      ]
    }
  end

  test "parse/3 returns records, partition progress, and terminal state" do
    digest = digest_fixture("one")
    body = encode_bin(record_msg(digest: digest)) <> encode_bin(last_msg())

    assert {:ok, [record], []} = ScanResponse.parse(body, @namespace, @set)
    assert record.key.digest == digest
    assert record.key.namespace == @namespace
    assert record.key.set == @set
    assert record.bins == %{"age" => 42}
  end

  test "parse_stream_chunk/3 surfaces chunk completion" do
    digest = digest_fixture("stream")
    body = encode_bin(record_msg(digest: digest)) <> encode_bin(last_msg())

    assert {:ok, [record], [], true} = ScanResponse.parse_stream_chunk(body, @namespace, @set)
    assert record.bins == %{"age" => 42}
  end

  test "parse/3 rejects incomplete and fatal terminal responses" do
    digest = digest_fixture("incomplete")
    incomplete_body = encode_bin(record_msg(digest: digest))
    fatal_body = encode_bin(last_msg(rc: 213))

    assert {:error, %{code: :parse_error, message: incomplete_message}} =
             ScanResponse.parse(incomplete_body, @namespace, @set)

    assert incomplete_message =~ "incomplete scan response"

    assert {:error, %{code: :query_generic}} = ScanResponse.parse(fatal_body, @namespace, @set)
  end

  test "parse/3 captures partition_done metadata" do
    digest = digest_fixture("part")
    body = encode_bin(partition_done_msg(digest)) <> encode_bin(last_msg())

    assert {:ok, [], [info]} = ScanResponse.parse(body, @namespace, @set)
    assert info.digest == digest
    assert info.unavailable? == false
    assert info.bval == nil
  end

  test "parse_frame/3 decodes partition unavailable metadata including bval" do
    digest = digest_fixture("unavailable")
    body = encode_bin(partition_done_msg(digest, partition_id: 42, rc: 11, bval: 7))

    assert {:ok, %{records: [], partition_done: [info], last?: false}} =
             ScanResponse.parse_frame(body, @namespace, @set)

    assert info.id == 42
    assert info.digest == digest
    assert info.bval == 7
    assert info.unavailable? == true
  end

  test "parse_frame/3 rejects short headers" do
    assert {:error, %{code: :parse_error, message: ":short_header"}} =
             ScanResponse.parse_frame(<<1, 2, 3>>, @namespace, @set)
  end

  test "parse_stream_chunk/3 rejects record frames missing a digest" do
    body =
      encode_bin(%AsmMsg{
        info1: AsmMsg.info1_read(),
        result_code: 0,
        fields: [Field.namespace(@namespace), Field.set(@set)],
        operations: [
          %Operation{
            op_type: Operation.op_read(),
            particle_type: 1,
            bin_name: "age",
            data: <<42::64-signed-big>>
          }
        ]
      })

    assert {:error, %{code: :parse_error, message: "scan record missing DIGEST_RIPE"}} =
             ScanResponse.parse_stream_chunk(body, @namespace, @set)
  end

  test "parse_frame/3 rejects trailing bytes" do
    body = encode_bin(last_msg()) <> <<1, 2, 3>>

    assert {:error, %{code: :parse_error}} = ScanResponse.parse_frame(body, @namespace, @set)
  end

  test "parse_aggregate_stream_chunk/3 decodes success and failure payloads" do
    ok_body = encode_bin(aggregate_msg("42")) <> encode_bin(last_msg())
    fail_body = encode_bin(aggregate_msg("boom", bin_name: "FAILURE")) <> encode_bin(last_msg())

    assert {:ok, ["42"], [], true} =
             ScanResponse.parse_aggregate_stream_chunk(ok_body, @namespace, @set)

    assert {:error, %{code: :query_generic, message: "boom"}} =
             ScanResponse.parse_aggregate_stream_chunk(fail_body, @namespace, @set)
  end

  test "parse_aggregate_stream_chunk/3 rejects unexpected aggregate bins" do
    body = encode_bin(aggregate_msg("42", bin_name: "OTHER")) <> encode_bin(last_msg())

    assert {:error, %{code: :query_generic, message: %Aerospike.Error{} = nested}} =
             ScanResponse.parse_aggregate_stream_chunk(body, @namespace, @set)

    assert nested.code == :parse_error
    assert nested.message =~ "expected SUCCESS bin"
  end

  test "parse_aggregate_stream_chunk/3 rejects aggregate frames with no result operations" do
    body =
      encode_bin(%AsmMsg{info1: AsmMsg.info1_read(), result_code: 0, fields: [], operations: []}) <>
        encode_bin(last_msg())

    assert {:error, %{code: :parse_error, message: message}} =
             ScanResponse.parse_aggregate_stream_chunk(body, @namespace, @set)

    assert message =~ "exactly one operation"
  end

  test "lazy_stream_chunk_terminal?/1 treats terminal frames and malformed input as terminal" do
    assert ScanResponse.lazy_stream_chunk_terminal?(encode_bin(last_msg())) == true
    assert ScanResponse.lazy_stream_chunk_terminal?(encode_bin(record_msg())) == false
    assert ScanResponse.lazy_stream_chunk_terminal?(<<1, 2, 3>>) == true
  end

  test "count_records/1 ignores partition_done frames" do
    digest = digest_fixture("count")

    body =
      encode_bin(record_msg(digest: digest)) <>
        encode_bin(partition_done_msg(digest)) <>
        encode_bin(last_msg())

    assert {:ok, 1} = ScanResponse.count_records(body)
  end

  test "count_records/1 rejects incomplete and malformed responses" do
    digest = digest_fixture("count-incomplete")

    assert {:error, %{code: :parse_error, message: incomplete_message}} =
             ScanResponse.count_records(encode_bin(record_msg(digest: digest)))

    assert incomplete_message =~ "incomplete scan response"

    assert {:error, %{code: :parse_error, message: ":short_header"}} =
             ScanResponse.count_records(<<1, 2, 3>>)
  end
end
