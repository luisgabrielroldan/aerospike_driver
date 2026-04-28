defmodule Aerospike.Integration.StreamTransportTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.ScanResponse
  alias Aerospike.Scan
  alias Aerospike.Test.IntegrationSupport
  alias Aerospike.Test.StreamProof
  alias Aerospike.Transport.Tcp

  @host "localhost"
  @port 3000
  @namespace "test"

  setup_all do
    IntegrationSupport.probe_aerospike!(@host, @port)

    IntegrationSupport.wait_for_cluster_ready!([{@host, @port}], @namespace, 60_000,
      expected_size: 3
    )

    :ok
  end

  test "scan streams decodable frames and all seeded records over the real TCP transport" do
    set = IntegrationSupport.unique_name("stream_proof")
    bin_name = "payload"

    {:ok, conn} = Tcp.connect(@host, @port, connect_timeout_ms: 1_000)

    on_exit(fn ->
      :ok = Tcp.close(conn)
    end)

    :ok =
      StreamProof.seed_records_on_master_partitions!(conn, @namespace, set, bin_name, 128, fn i ->
        String.duplicate("stream_record_#{i}_", 256)
      end)

    cluster = IntegrationSupport.unique_atom("stream_transport_cluster")

    {:ok, sup} =
      Aerospike.start_link(
        name: cluster,
        transport: Tcp,
        hosts: ["#{@host}:#{@port}"],
        namespaces: [@namespace],
        tend_trigger: :manual
      )

    on_exit(fn ->
      IntegrationSupport.stop_supervisor_quietly(sup)
    end)

    IntegrationSupport.wait_for_tender_ready!(cluster, 5_000)

    IntegrationSupport.assert_eventually(
      "seeded stream proof records become visible to scans",
      fn ->
        case Aerospike.scan_count(cluster, Scan.new(@namespace, set)) do
          {:ok, 128} -> true
          _ -> false
        end
      end
    )

    task_id = StreamProof.random_task_id()
    request = StreamProof.scan_request(conn, @namespace, set, task_id)
    assert {:ok, stream} = Tcp.stream_open(conn, request, 5_000, [])

    frames = StreamProof.collect_stream!(stream)

    refute frames == [],
           "expected the scan to produce at least one frame, got #{length(frames)}"

    total_records =
      Enum.reduce(frames, 0, fn frame, acc ->
        {:ok, {2, 3, body}} = Message.decode(frame)
        {:ok, records, _parts, _done?} = ScanResponse.parse_stream_chunk(body, @namespace, set)
        acc + length(records)
      end)

    assert total_records == 128

    assert :ok = Tcp.stream_close(stream)
  end
end
