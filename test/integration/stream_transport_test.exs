defmodule Aerospike.Integration.StreamTransportTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Protocol.Message
  alias Aerospike.Test.StreamProof
  alias Aerospike.Transport.Tcp

  @host "localhost"
  @port 3000
  @namespace "test"

  setup_all do
    probe_aerospike!(@host, @port)
    :ok
  end

  test "scan streams multiple frames over the real TCP transport" do
    set = "stream_proof_#{System.unique_integer([:positive])}"
    bin_name = "payload"

    {:ok, conn} = Tcp.connect(@host, @port, connect_timeout_ms: 1_000)

    on_exit(fn ->
      :ok = Tcp.close(conn)
    end)

    records =
      for i <- 1..4 do
        {i, "stream_record_#{i}"}
      end

    :ok = StreamProof.seed_records!(conn, @namespace, set, bin_name, records)
    :ok = Tcp.close(conn)

    {:ok, scan_conn} = Tcp.connect(@host, @port, connect_timeout_ms: 1_000)

    on_exit(fn ->
      :ok = Tcp.close(scan_conn)
    end)

    task_id = System.unique_integer([:positive])
    request = StreamProof.scan_request(@namespace, set, task_id)
    assert {:ok, stream} = Tcp.stream_open(scan_conn, request, 5_000, [])

    frames = StreamProof.collect_stream!(stream)

    assert length(frames) >= 2,
           "expected the scan to produce multiple frames, got #{length(frames)}"

    Enum.each(frames, fn frame ->
      assert {:ok, {2, 3, body}} = Message.decode(frame)
      assert {:ok, msg} = Aerospike.Protocol.AsmMsg.decode(body)
      assert msg.result_code == 0
    end)

    assert :ok = Tcp.stream_close(stream)
  end

  defp probe_aerospike!(host, port) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 1_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, reason} ->
        raise "Aerospike not reachable at #{host}:#{port} (#{inspect(reason)}). " <>
                "Run `docker compose up -d` first."
    end
  end
end
