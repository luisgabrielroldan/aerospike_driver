defmodule Aerospike.Test.StreamProof do
  @moduledoc false

  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Transport.Tcp

  @write_timeout_ms 5_000
  @scan_timeout_ms 5_000
  @write_op_type 2
  @partition_done 0x04

  @spec seed_records!(
          Tcp.conn(),
          String.t(),
          String.t(),
          String.t(),
          [{String.t() | integer(), binary() | integer()}]
        ) :: :ok
  def seed_records!(conn, namespace, set, bin_name, records) when is_list(records) do
    Enum.each(records, fn {user_key, value} ->
      key = Key.new(namespace, set, user_key)
      request = put_request(key, bin_name, value)

      case Tcp.command(conn, request, @write_timeout_ms, []) do
        {:ok, reply} ->
          assert_write_ok!(reply, key, bin_name, value)

        {:error, error} ->
          raise "seed write failed for #{inspect(key)}: #{inspect(error)}"
      end
    end)

    :ok
  end

  @spec scan_request(String.t(), String.t(), non_neg_integer()) :: iodata()
  def scan_request(namespace, set, task_id) when is_binary(namespace) and is_binary(set) do
    msg = %AsmMsg{
      info1: AsmMsg.info1_read(),
      info3: @partition_done,
      expiration: -1,
      timeout: @scan_timeout_ms,
      fields: [
        Field.namespace(namespace),
        Field.set(set),
        pid_array_field(),
        uint32_field(Field.type_socket_timeout(), @scan_timeout_ms),
        uint64_field(Field.type_query_id(), task_id)
      ]
    }

    Message.encode_as_msg_iodata(AsmMsg.encode(msg))
  end

  @spec collect_stream!(Tcp.stream()) :: [binary()]
  def collect_stream!(stream) do
    collect_stream!(stream, [])
  end

  defp collect_stream!(stream, acc) do
    case Tcp.stream_read(stream, @scan_timeout_ms) do
      {:ok, frame} ->
        collect_stream!(stream, [frame | acc])

      :done ->
        Enum.reverse(acc)

      {:error, error} ->
        raise "stream read failed: #{inspect(error)}"
    end
  end

  defp put_request(%Key{} = key, bin_name, value) do
    key_field = Field.key_from_user_key(key)

    fields =
      [
        Field.namespace(key.namespace),
        Field.set(key.set),
        Field.digest(key.digest)
      ] ++ if key_field, do: [key_field], else: []

    msg = %AsmMsg{
      info2: 0x01,
      expiration: 0,
      timeout: @write_timeout_ms,
      fields: fields,
      operations: [write_operation(bin_name, value)]
    }

    Message.encode_as_msg_iodata(AsmMsg.encode(msg))
  end

  defp write_operation(bin_name, value) when is_binary(value) do
    %Operation{
      op_type: @write_op_type,
      particle_type: 3,
      bin_name: bin_name,
      data: value
    }
  end

  defp write_operation(bin_name, value) when is_integer(value) do
    %Operation{
      op_type: @write_op_type,
      particle_type: 1,
      bin_name: bin_name,
      data: <<value::64-signed-big>>
    }
  end

  defp uint32_field(type, value) do
    %Field{type: type, data: <<value::32-big>>}
  end

  defp uint64_field(type, value) do
    %Field{type: type, data: <<value::64-big>>}
  end

  defp pid_array_field do
    pids =
      for pid <- 0..4_095, into: <<>> do
        <<pid::16-little>>
      end

    %Field{type: Field.type_pid_array(), data: pids}
  end

  defp assert_write_ok!(reply, key, bin_name, value) do
    case AsmMsg.decode(reply) do
      {:ok, %AsmMsg{result_code: 0}} ->
        :ok

      {:ok, %AsmMsg{result_code: result_code}} ->
        raise """
        seed write failed for #{inspect(key)} bin #{inspect(bin_name)}=#{inspect(value)} \
        with result code #{result_code}
        """

      {:error, reason} ->
        raise "seed write reply could not be decoded for #{inspect(key)}: #{inspect(reason)}"
    end
  end
end
