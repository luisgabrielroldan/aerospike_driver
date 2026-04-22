defmodule Aerospike.Protocol.ScanQueryTest do
  use ExUnit.Case, async: true

  alias Aerospike.Filter
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Filter, as: FilterCodec
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.MessagePack
  alias Aerospike.Policy
  alias Aerospike.Protocol.ScanQuery
  alias Aerospike.Query
  alias Aerospike.Scan

  @info3_partition_done 0x04

  defp decode_as_msg(wire) do
    assert {:ok, {_version, 3, body}} = wire |> IO.iodata_to_binary() |> Message.decode()
    assert {:ok, msg} = AsmMsg.decode(body)
    msg
  end

  defp field_data(msg, type) do
    Enum.find_value(msg.fields, fn
      %Field{type: ^type, data: data} -> data
      _ -> nil
    end)
  end

  test "build_scan/3 encodes partition fields and selected bins" do
    scan =
      Scan.new("testns", "users")
      |> Scan.select(["a", "b"])
      |> Scan.records_per_second(50)

    {:ok, policy} =
      Policy.scan_query_runtime(timeout: 12_345, task_id: 9_001_234_567_890)

    wire =
      ScanQuery.build_scan(
        scan,
        %{
          parts_full: [1, 2, 0x0102],
          parts_partial: [%{id: 10, digest: :crypto.strong_rand_bytes(20), bval: -1}],
          record_max: 100
        },
        policy
      )

    msg = decode_as_msg(wire)

    assert msg.info1 == AsmMsg.info1_read()
    assert msg.info3 == @info3_partition_done
    assert msg.timeout == 12_345
    assert Enum.map(msg.operations, & &1.bin_name) == ["a", "b"]

    assert field_data(msg, Field.type_namespace()) == "testns"
    assert field_data(msg, Field.type_table()) == "users"

    assert field_data(msg, Field.type_pid_array()) ==
             <<1::16-little, 2::16-little, 0x0102::16-little>>

    assert byte_size(field_data(msg, Field.type_digest_array())) == 20
    assert field_data(msg, Field.type_max_records()) == <<100::64-signed-big>>
    assert field_data(msg, Field.type_records_per_second()) == <<50::32-signed-big>>
    assert field_data(msg, Field.type_socket_timeout()) == <<12_345::32-signed-big>>
    assert field_data(msg, Field.type_query_id()) == <<9_001_234_567_890::64-unsigned-big>>
  end

  test "build_query/3 encodes a filter struct and keeps query bins" do
    query =
      Query.new("testns", "users")
      |> Query.where(Filter.range("age", 10, 20))
      |> Query.select(["score"])
      |> Query.records_per_second(7)

    {:ok, policy} = Policy.scan_query_runtime(timeout: 5_000, task_id: 42)

    wire =
      ScanQuery.build_query(
        query,
        %{
          parts_full: [3],
          parts_partial: [%{id: 11, digest: :crypto.strong_rand_bytes(20), bval: 99}],
          record_max: 4
        },
        policy
      )

    msg = decode_as_msg(wire)

    assert msg.info1 == AsmMsg.info1_read()
    assert msg.info3 == @info3_partition_done
    assert Enum.map(msg.operations, & &1.bin_name) == ["score"]

    assert field_data(msg, Field.type_index_range()) ==
             FilterCodec.encode(Filter.range("age", 10, 20))

    assert field_data(msg, Field.type_bval_array()) == <<99::64-signed-little>>
    assert field_data(msg, Field.type_max_records()) == <<4::64-signed-big>>
    assert field_data(msg, Field.type_records_per_second()) == <<7::32-signed-big>>
  end

  test "build_query_execute/4 encodes write ops on the background query path" do
    query =
      Query.new("testns", "users")
      |> Query.where(Filter.range("age", 10, 20))
      |> Query.max_records(4)

    {:ok, write_op} = Operation.write("state", "executed")
    {:ok, policy} = Policy.scan_query_runtime(timeout: 5_000, task_id: 42)

    wire =
      ScanQuery.build_query_execute(
        query,
        %{parts_full: [4], parts_partial: [], record_max: 9},
        [write_op],
        policy
      )

    msg = decode_as_msg(wire)

    assert msg.info1 == 0
    assert msg.info2 == AsmMsg.info2_write()
    assert msg.info3 == @info3_partition_done
    assert Enum.map(msg.operations, & &1.bin_name) == ["state"]

    assert field_data(msg, Field.type_index_range()) ==
             FilterCodec.encode(Filter.range("age", 10, 20))

    assert field_data(msg, Field.type_query_id()) == <<42::64-unsigned-big>>
  end

  test "build_query_udf/6 encodes the UDF field block with write flags" do
    query =
      Query.new("testns", "users")
      |> Query.where(Filter.range("age", 10, 20))

    {:ok, policy} = Policy.scan_query_runtime(timeout: 5_000, task_id: 7)

    wire =
      ScanQuery.build_query_udf(
        query,
        %{parts_full: [4], parts_partial: [], record_max: 9},
        "demo",
        "echo",
        [1, true, nil],
        policy
      )

    msg = decode_as_msg(wire)

    assert msg.info1 == 0
    assert msg.info2 == AsmMsg.info2_write()
    assert msg.info3 == @info3_partition_done
    assert msg.operations == []
    assert field_data(msg, Field.type_udf_op()) == <<2>>
    assert field_data(msg, Field.type_udf_package_name()) == "demo"
    assert field_data(msg, Field.type_udf_function()) == "echo"
    assert field_data(msg, Field.type_udf_arglist()) == MessagePack.pack!([1, true, nil])
  end

  test "build_query_aggregate/6 encodes the aggregate UDF field block without write ops" do
    query =
      Query.new("testns", "users")
      |> Query.where(Filter.range("age", 10, 20))
      |> Query.select(["score"])

    {:ok, policy} = Policy.scan_query_runtime(timeout: 5_000, task_id: 11)

    wire =
      ScanQuery.build_query_aggregate(
        query,
        %{parts_full: [4], parts_partial: [], record_max: 9},
        "demo",
        "sum",
        ["score"],
        policy
      )

    msg = decode_as_msg(wire)

    assert msg.info1 == AsmMsg.info1_read()
    assert msg.info2 == 0
    assert msg.info3 == @info3_partition_done
    assert msg.operations == []
    assert field_data(msg, Field.type_udf_op()) == <<1>>
    assert field_data(msg, Field.type_udf_package_name()) == "demo"
    assert field_data(msg, Field.type_udf_function()) == "sum"

    assert field_data(msg, Field.type_udf_arglist()) ==
             MessagePack.pack!([{:particle_string, "score"}])
  end
end
