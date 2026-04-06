defmodule Aerospike.Protocol.BatchResponseTest do
  use ExUnit.Case, async: true

  alias Aerospike.Batch
  alias Aerospike.BatchResult
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.BatchResponse

  defp header(opts) do
    i3 = Keyword.get(opts, :i3, 0)
    rc = Keyword.get(opts, :rc, 0)
    bidx = Keyword.get(opts, :bidx, 0)
    fc = Keyword.get(opts, :fc, 0)
    oc = Keyword.get(opts, :oc, 0)

    <<
      22::8,
      0::8,
      0::8,
      i3::8,
      0::8,
      rc::8,
      0::32-big,
      0::32-big,
      bidx::32-big,
      fc::16-big,
      oc::16-big
    >>
  end

  defp last_header do
    header(i3: AsmMsg.info3_last())
  end

  describe "parse_batch_get/2" do
    test "empty keys" do
      assert {:ok, []} = BatchResponse.parse_batch_get(<<>>, [])
    end

    test "missing key becomes nil" do
      key = Key.new("n", "s", "x")
      body = header(rc: 2, bidx: 0) <> last_header()
      assert {:ok, [nil]} = BatchResponse.parse_batch_get(body, [key])
    end

    test "short header is error" do
      key = Key.new("n", "s", "x")
      assert {:error, %{code: :parse_error}} = BatchResponse.parse_batch_get(<<1, 2, 3>>, [key])
    end

    test "invalid batch index in get_found_record" do
      key = Key.new("n", "s", "x")
      body = header(rc: 0, bidx: 9, fc: 0, oc: 0) <> last_header()
      assert {:error, %{code: :parse_error}} = BatchResponse.parse_batch_get(body, [key])
    end

    test "batch_read_rc error stops parse" do
      key = Key.new("n", "s", "x")
      body = header(rc: 1, bidx: 0) <> last_header()
      assert {:error, %{code: :server_error}} = BatchResponse.parse_batch_get(body, [key])
    end

    test "filtered_out then last" do
      key = Key.new("n", "s", "x")
      seg1 = header(rc: 27, bidx: 0, fc: 0, oc: 0)
      body = seg1 <> last_header()
      assert {:ok, [nil]} = BatchResponse.parse_batch_get(body, [key])
    end
  end

  describe "parse_batch_exists/2" do
    test "empty count" do
      assert {:ok, []} = BatchResponse.parse_batch_exists(<<>>, 0)
    end

    test "rc 0 marks index as existing" do
      body = header(rc: 0, bidx: 1) <> last_header()
      assert {:ok, [false, true, false]} = BatchResponse.parse_batch_exists(body, 3)
    end

    test "short header is error" do
      assert {:error, %{code: :parse_error}} = BatchResponse.parse_batch_exists(<<1, 2>>, 2)
    end

    test "exists_after_header error from batch_read_rc" do
      body = header(rc: 1, bidx: 0) <> last_header()
      assert {:error, %{code: :server_error}} = BatchResponse.parse_batch_exists(body, 3)
    end
  end

  describe "parse_batch_operate/2" do
    test "read key_not_found -> ok nil" do
      key = Key.new("n", "s", "x")
      op = Batch.read(key)
      body = header(rc: 2, bidx: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :ok, record: nil}]} =
               BatchResponse.parse_batch_operate(body, [op])
    end

    test "put success -> ok without record" do
      key = Key.new("n", "s", "x")
      op = Batch.put(key, %{"a" => 1})
      body = header(rc: 0, bidx: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :ok, record: nil}]} =
               BatchResponse.parse_batch_operate(body, [op])
    end

    test "operate success with no ops returns empty bins on record" do
      key = Key.new("n", "s", "x")
      op = Batch.operate(key, [])
      body = header(rc: 0, bidx: 0, oc: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :ok, record: %Aerospike.Record{bins: bins}}]} =
               BatchResponse.parse_batch_operate(body, [op])

      assert bins == %{}
    end

    test "short header is error" do
      key = Key.new("n", "s", "x")
      op = Batch.read(key)
      assert {:error, %{code: :parse_error}} = BatchResponse.parse_batch_operate(<<1>>, [op])
    end

    test "invalid batch index" do
      key = Key.new("n", "s", "x")
      op = Batch.read(key)
      body = header(rc: 0, bidx: 3, fc: 0, oc: 0) <> last_header()
      assert {:error, %{code: :parse_error}} = BatchResponse.parse_batch_operate(body, [op])
    end

    test "read returns error for non-ok rc" do
      key = Key.new("n", "s", "x")
      op = Batch.read(key)
      body = header(rc: 4, bidx: 0, fc: 0, oc: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :error, error: %{code: :parameter_error}}]} =
               BatchResponse.parse_batch_operate(body, [op])
    end

    test "put returns error for non-ok rc" do
      key = Key.new("n", "s", "x")
      op = Batch.put(key, %{"a" => 1})
      body = header(rc: 4, bidx: 0, fc: 0, oc: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :error, error: %{code: :parameter_error}}]} =
               BatchResponse.parse_batch_operate(body, [op])
    end

    test "delete key_not_found ok" do
      key = Key.new("n", "s", "x")
      op = Batch.delete(key)
      body = header(rc: 2, bidx: 0, fc: 0, oc: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :ok, record: nil}]} =
               BatchResponse.parse_batch_operate(body, [op])
    end

    test "udf ok without returned bins" do
      key = Key.new("n", "s", "x")
      op = Batch.udf(key, "p", "f", [])
      body = header(rc: 0, bidx: 0, fc: 0, oc: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :ok, record: nil}]} =
               BatchResponse.parse_batch_operate(body, [op])
    end

    test "operate non-ok rc" do
      key = Key.new("n", "s", "x")
      op = Batch.operate(key, [Aerospike.Op.get("a")])
      body = header(rc: 9, bidx: 0, fc: 0, oc: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :error, error: %{code: :timeout}}]} =
               BatchResponse.parse_batch_operate(body, [op])
    end

    test "empty batch ops returns empty list" do
      assert {:ok, []} = BatchResponse.parse_batch_operate(<<>>, [])
    end

    test "read bin_not_found returns ok nil" do
      key = Key.new("n", "s", "x")
      op = Batch.read(key)
      body = header(rc: 17, bidx: 0, fc: 0, oc: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :ok, record: nil}]} =
               BatchResponse.parse_batch_operate(body, [op])
    end

    test "delete with non-ok/non-key_not_found rc returns error" do
      key = Key.new("n", "s", "x")
      op = Batch.delete(key)
      body = header(rc: 4, bidx: 0, fc: 0, oc: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :error, error: %{code: :parameter_error}}]} =
               BatchResponse.parse_batch_operate(body, [op])
    end

    test "udf with returned bins populates record" do
      key = Key.new("n", "s", "x")
      op = Batch.udf(key, "p", "f", [])

      op_bin =
        Operation.encode(%Operation{
          op_type: Operation.op_read(),
          particle_type: Operation.particle_string(),
          bin_name: "result",
          data: "ok"
        })

      body = header(rc: 0, bidx: 0, fc: 0, oc: 1) <> op_bin <> last_header()

      assert {:ok, [%BatchResult{status: :ok, record: %Aerospike.Record{bins: bins}}]} =
               BatchResponse.parse_batch_operate(body, [op])

      assert bins == %{"result" => "ok"}
    end

    test "udf error returns error result" do
      key = Key.new("n", "s", "x")
      op = Batch.udf(key, "p", "f", [])
      body = header(rc: 4, bidx: 0, fc: 0, oc: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :error, error: %{code: :parameter_error}}]} =
               BatchResponse.parse_batch_operate(body, [op])
    end

    test "unknown result code maps to server_error in read" do
      key = Key.new("n", "s", "x")
      op = Batch.read(key)
      body = header(rc: 254, bidx: 0, fc: 0, oc: 0) <> last_header()

      assert {:ok, [%BatchResult{status: :error, error: %{code: :server_error}}]} =
               BatchResponse.parse_batch_operate(body, [op])
    end
  end

  describe "edge cases" do
    test "out-of-order batch indices are placed in original slots for operate" do
      key0 = Key.new("n", "s", "k0")
      key1 = Key.new("n", "s", "k1")
      key2 = Key.new("n", "s", "k2")
      ops = [Batch.read(key0), Batch.read(key1), Batch.read(key2)]

      body =
        header(rc: 0, bidx: 2, fc: 0, oc: 0) <>
          header(rc: 3, bidx: 0, fc: 0, oc: 0) <>
          header(rc: 2, bidx: 1, fc: 0, oc: 0) <>
          last_header()

      assert {:ok, [slot0, slot1, slot2]} = BatchResponse.parse_batch_operate(body, ops)
      assert %BatchResult{status: :error, error: %{code: :generation_error}} = slot0
      assert %BatchResult{status: :ok, record: nil} = slot1

      assert %BatchResult{status: :ok, record: %Aerospike.Record{key: %Key{user_key: "k2"}}} =
               slot2
    end

    test "batch get returns parse_error when an out-of-range index appears mid-stream" do
      key0 = Key.new("n", "s", "k0")
      key1 = Key.new("n", "s", "k1")

      body =
        header(rc: 2, bidx: 0, fc: 0, oc: 0) <>
          header(rc: 2, bidx: 9, fc: 0, oc: 0) <>
          last_header()

      assert {:error, %{code: :parse_error}} = BatchResponse.parse_batch_get(body, [key0, key1])
    end

    test "truncated batch frame returns parse_error for get" do
      key = Key.new("n", "s", "k0")
      body = header(rc: 0, bidx: 0, fc: 0, oc: 1) <> last_header()

      assert {:error, :incomplete_operation} = BatchResponse.parse_batch_get(body, [key])
    end

    test "empty non-terminal body yields default nil slots for batch get" do
      keys = [Key.new("n", "s", "k0"), Key.new("n", "s", "k1"), Key.new("n", "s", "k2")]
      assert {:ok, [nil, nil, nil]} = BatchResponse.parse_batch_get(last_header(), keys)
    end

    test "empty non-terminal body yields default false slots for batch exists" do
      assert {:ok, [false, false, false]} = BatchResponse.parse_batch_exists(last_header(), 3)
    end

    test "duplicate batch index in exists is last-write-wins" do
      body =
        header(rc: 0, bidx: 1, fc: 0, oc: 0) <>
          header(rc: 2, bidx: 1, fc: 0, oc: 0) <> last_header()

      assert {:ok, [false, false, false]} = BatchResponse.parse_batch_exists(body, 3)
    end
  end
end
