defmodule Aerospike.Protocol.ScanQueryTest do
  use ExUnit.Case, async: true

  alias Aerospike.Exp
  alias Aerospike.Filter
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.ScanQuery
  alias Aerospike.Query
  alias Aerospike.Scan

  defp decode_as_msg(wire) do
    assert {:ok, {_v, 3, body}} = wire |> IO.iodata_to_binary() |> Message.decode()
    assert {:ok, msg} = AsmMsg.decode(body)
    msg
  end

  defp field_types(msg) do
    Enum.map(msg.fields, & &1.type)
  end

  defp field_data(msg, type) do
    Enum.find_value(msg.fields, fn
      %Field{type: ^type, data: d} -> d
      _ -> false
    end)
  end

  @partition_ids [1, 2, 0x0102]
  @digest_a :crypto.strong_rand_bytes(20)
  @digest_b :crypto.strong_rand_bytes(20)

  @np %{
    parts_full: @partition_ids,
    parts_partial: [
      %{id: 10, digest: @digest_a, bval: -1},
      %{id: 11, digest: @digest_b, bval: nil}
    ],
    record_max: 100
  }

  test "build_scan/3 decodes to AS_MSG with expected flags and fields" do
    scan =
      Scan.new("testns", "users")
      |> Scan.select(["a", "b"])
      |> Scan.records_per_second(50)

    wire =
      ScanQuery.build_scan(scan, @np,
        timeout: 12_345,
        task_id: 9_001_234_567_890
      )

    msg = decode_as_msg(wire)

    assert Bitwise.band(msg.info1, AsmMsg.info1_read()) != 0
    assert Bitwise.band(msg.info3, AsmMsg.info3_partition_done()) != 0
    assert msg.timeout == 12_345
    assert length(msg.operations) == 2
    assert Enum.map(msg.operations, & &1.bin_name) |> Enum.sort() == ["a", "b"]

    assert Field.type_namespace() in field_types(msg)
    assert Field.type_table() in field_types(msg)
    assert Field.type_pid_array() in field_types(msg)
    assert Field.type_digest_array() in field_types(msg)
    assert Field.type_max_records() in field_types(msg)
    assert Field.type_records_per_second() in field_types(msg)
    assert Field.type_socket_timeout() in field_types(msg)
    assert Field.type_query_id() in field_types(msg)

    assert field_data(msg, Field.type_namespace()) == "testns"
    assert field_data(msg, Field.type_table()) == "users"
    assert field_data(msg, Field.type_socket_timeout()) == <<12_345::32-signed-big>>
    assert field_data(msg, Field.type_query_id()) == <<9_001_234_567_890::64-unsigned-big>>
    assert field_data(msg, Field.type_max_records()) == <<100::64-signed-big>>
    assert field_data(msg, Field.type_records_per_second()) == <<50::32-signed-big>>
  end

  test "PID_ARRAY encodes uint16 little-endian partition ids" do
    scan = Scan.new("ns")
    wire = ScanQuery.build_scan(scan, %{@np | parts_partial: [], record_max: 0}, task_id: 1)
    msg = decode_as_msg(wire)
    pid_data = field_data(msg, Field.type_pid_array())

    assert pid_data == <<1::16-little, 2::16-little, 0x0102::16-little>>
  end

  test "DIGEST_ARRAY concatenates 20-byte digests in order" do
    scan = Scan.new("ns")
    wire = ScanQuery.build_scan(scan, %{@np | parts_full: [], record_max: 0}, task_id: 1)
    msg = decode_as_msg(wire)
    d = field_data(msg, Field.type_digest_array())
    assert d == @digest_a <> @digest_b
    assert byte_size(d) == 40
  end

  test "scan without set omits TABLE field" do
    scan = Scan.new("lonely")

    wire =
      ScanQuery.build_scan(scan, %{parts_full: [0], parts_partial: [], record_max: 0}, task_id: 1)

    msg = decode_as_msg(wire)
    refute Field.type_table() in field_types(msg)
  end

  test "build_query/3 includes INDEX_RANGE and sets query flags" do
    q =
      Query.new("ns", "set1")
      |> Query.where(Filter.equal("k", "v"))
      |> Query.select(["x"])

    wire =
      ScanQuery.build_query(q, %{parts_full: [3], parts_partial: [], record_max: 7},
        timeout: 5_000,
        task_id: 42
      )

    msg = decode_as_msg(wire)

    assert Bitwise.band(msg.info1, AsmMsg.info1_read()) != 0
    assert Bitwise.band(msg.info1, AsmMsg.info1_short_query()) != 0
    assert Bitwise.band(msg.info2, AsmMsg.info2_relax_ap_long_query()) != 0
    assert Bitwise.band(msg.info3, AsmMsg.info3_partition_done()) != 0

    assert Field.type_index_range() in field_types(msg)
    range_data = field_data(msg, Field.type_index_range())
    assert range_data == Aerospike.Protocol.Filter.encode(Filter.equal("k", "v"))

    assert field_data(msg, Field.type_namespace()) == "ns"
    assert field_data(msg, Field.type_table()) == "set1"
    assert field_data(msg, Field.type_pid_array()) == <<3::16-little>>
  end

  test "build_query/3 with partials includes BVAL_ARRAY after DIGEST_ARRAY order in field list" do
    q = Query.new("ns", "s") |> Query.where(Filter.range("n", 1, 2))

    partials = [%{id: 0, digest: @digest_a, bval: 99}]

    wire =
      ScanQuery.build_query(q, %{parts_full: [], parts_partial: partials, record_max: 0},
        task_id: 1
      )

    msg = decode_as_msg(wire)

    assert Bitwise.band(msg.info1, AsmMsg.info1_short_query()) == 0
    assert Bitwise.band(msg.info2, AsmMsg.info2_relax_ap_long_query()) != 0

    types = field_types(msg)
    idx_digest = Enum.find_index(types, &(&1 == Field.type_digest_array()))
    idx_bval = Enum.find_index(types, &(&1 == Field.type_bval_array()))
    assert idx_digest < idx_bval

    assert field_data(msg, Field.type_bval_array()) == <<99::64-signed-little>>
  end

  test "build_query/3 without index_filter raises" do
    q = Query.new("ns", "s")

    assert_raise ArgumentError, fn ->
      ScanQuery.build_query(q, %{parts_full: [], parts_partial: [], record_max: 0}, [])
    end
  end

  test "scan attaches FILTER_EXP from expression filters" do
    scan =
      Scan.new("ns")
      |> Scan.filter(Exp.from_wire(<<0xC3>>))
      |> Scan.filter(Exp.from_wire(<<0xC2>>))

    wire =
      ScanQuery.build_scan(scan, %{parts_full: [0], parts_partial: [], record_max: 0}, task_id: 1)

    msg = decode_as_msg(wire)

    data = field_data(msg, Field.type_filter_exp())
    assert is_binary(data)
    # AND wrapper: fixarray(3) + small-int opcode 16 + left + right
    assert binary_part(data, 0, 2) == <<0x93, 16>>
  end
end
