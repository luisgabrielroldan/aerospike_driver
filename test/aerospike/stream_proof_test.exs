defmodule Aerospike.Test.StreamProofTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Test.StreamProof
  alias Aerospike.Transport.Tcp

  test "random_task_id/0 returns a positive unsigned task id" do
    assert StreamProof.random_task_id() > 0
  end

  test "seed_records!/5 writes records and validates successful AS_MSG replies" do
    with_tcp_server([ok_as_msg_reply()], fn conn ->
      assert :ok =
               StreamProof.seed_records!(conn, "test", "stream_proof", "payload", [{"k1", "v1"}])
    end)
  end

  test "seed_records!/5 raises when the write reply carries an Aerospike error code" do
    with_tcp_server([as_msg_reply(2)], fn conn ->
      assert_raise RuntimeError, ~r/with result code 2/, fn ->
        StreamProof.seed_records!(conn, "test", "stream_proof", "payload", [{"k1", "v1"}])
      end
    end)
  end

  test "scan_request/4 derives master partitions from node info" do
    with_tcp_server([replicas_info_reply()], fn conn ->
      request = StreamProof.scan_request(conn, "test", "stream_proof", 123)

      assert IO.iodata_to_binary(request) =~ "stream_proof"
    end)
  end

  test "seed_records_on_master_partitions!/6 derives keys on master partitions before writing" do
    with_tcp_server([replicas_info_reply(), ok_as_msg_reply(), ok_as_msg_reply()], fn conn ->
      assert :ok =
               StreamProof.seed_records_on_master_partitions!(
                 conn,
                 "test",
                 "stream_proof",
                 "payload",
                 2,
                 &"value-#{&1}"
               )
    end)
  end

  test "scan_request/4 raises when replicas info is incomplete" do
    with_tcp_server([Message.encode_info("node\tA1\n")], fn conn ->
      assert_raise RuntimeError, ~r/could not fetch node\/replicas info/, fn ->
        StreamProof.scan_request(conn, "test", "stream_proof", 123)
      end
    end)
  end

  test "collect_stream!/1 drains frames until the stream completes" do
    {:ok, stream} = __MODULE__.StreamStub.start_link([{:ok, "one"}, {:ok, "two"}, :done])

    assert StreamProof.collect_stream!(stream) == ["one", "two"]
  end

  test "collect_stream!/1 raises transport errors with context" do
    error = Error.from_result_code(:network_error, message: "socket closed")
    {:ok, stream} = __MODULE__.StreamStub.start_link([{:ok, "one"}, {:error, error}])

    assert_raise RuntimeError, ~r/stream read failed/, fn ->
      StreamProof.collect_stream!(stream)
    end
  end

  defmodule StreamStub do
    use GenServer

    def start_link(events), do: GenServer.start_link(__MODULE__, events)

    @impl true
    def init(events), do: {:ok, :queue.from_list(events)}

    @impl true
    def handle_call({:read, _deadline_ms}, _from, queue) do
      {{:value, event}, queue} = :queue.out(queue)
      {:reply, event, queue}
    end
  end

  defp with_tcp_server(replies, fun) when is_list(replies) and is_function(fun, 1) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])
    {:ok, {_addr, port}} = :inet.sockname(listen)

    task =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listen)

        Enum.each(replies, fn reply ->
          {:ok, _request} = :gen_tcp.recv(socket, 0, 2_000)
          :ok = :gen_tcp.send(socket, reply)
        end)

        :gen_tcp.close(socket)
        :gen_tcp.close(listen)
      end)

    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

    try do
      fun.(%Tcp{socket: socket, info_timeout: 2_000})
    after
      :gen_tcp.close(socket)
      Task.await(task, 2_000)
    end
  end

  defp ok_as_msg_reply, do: as_msg_reply(0)

  defp replicas_info_reply do
    replicas = ReplicasFixture.build("test", 1, [Enum.to_list(0..4095), []])
    Message.encode_info("node\tA1\nreplicas\t#{replicas}\n")
  end

  defp as_msg_reply(result_code) do
    %AsmMsg{result_code: result_code}
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
    |> IO.iodata_to_binary()
  end
end
