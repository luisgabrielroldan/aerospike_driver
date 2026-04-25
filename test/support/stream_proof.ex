defmodule Aerospike.Test.StreamProof do
  @moduledoc false

  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.PartitionMap, as: PartitionMapParser
  alias Aerospike.Protocol.ScanQuery
  alias Aerospike.Scan
  alias Aerospike.Transport.Tcp

  @write_timeout_ms 5_000
  @scan_timeout_ms 5_000

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

  @spec seed_records_on_master_partitions!(
          Tcp.conn(),
          String.t(),
          String.t(),
          String.t(),
          non_neg_integer(),
          (non_neg_integer() -> binary() | integer())
        ) :: :ok
  def seed_records_on_master_partitions!(
        %Tcp{} = conn,
        namespace,
        set,
        bin_name,
        count,
        value_fun
      )
      when is_binary(namespace) and is_binary(set) and is_integer(count) and count > 0 and
             is_function(value_fun, 1) do
    allowed = MapSet.new(master_partitions!(conn, namespace))
    records = records_on_master_partitions!(allowed, namespace, set, count, value_fun)

    Enum.each(records, fn {%Key{} = key, value} ->
      write_seed_record!(conn, key, bin_name, value)
    end)

    :ok
  end

  defp records_on_master_partitions!(allowed, namespace, set, count, value_fun) do
    0..100_000
    |> Enum.reduce_while({0, []}, fn suffix, {found, acc} ->
      key = Key.new(namespace, set, "stream-record-#{suffix}")

      maybe_keep_master_key(allowed, count, value_fun, key, found, acc)
    end)
    |> case do
      records when is_list(records) and length(records) == count ->
        records

      _ ->
        raise "stream scan proof could not derive #{count} keys on master partitions"
    end
  end

  defp maybe_keep_master_key(allowed, count, value_fun, key, found, acc) do
    if MapSet.member?(allowed, Key.partition_id(key)) do
      maybe_halt_master_key_search(count, value_fun, key, found, acc)
    else
      {:cont, {found, acc}}
    end
  end

  defp maybe_halt_master_key_search(count, value_fun, key, found, acc) do
    next_found = found + 1
    next_acc = [{key, value_fun.(next_found)} | acc]

    if next_found == count do
      {:halt, Enum.reverse(next_acc)}
    else
      {:cont, {next_found, next_acc}}
    end
  end

  defp write_seed_record!(conn, key, bin_name, value) do
    request = put_request(key, bin_name, value)

    case Tcp.command(conn, request, @write_timeout_ms, []) do
      {:ok, reply} ->
        assert_write_ok!(reply, key, bin_name, value)

      {:error, error} ->
        raise "seed write failed for #{inspect(key)}: #{inspect(error)}"
    end
  end

  @spec scan_request(Tcp.conn(), String.t(), String.t(), non_neg_integer()) :: iodata()
  def scan_request(%Tcp{} = conn, namespace, set, task_id)
      when is_binary(namespace) and is_binary(set) do
    scan = Scan.new(namespace, set)
    parts_full = master_partitions!(conn, namespace)

    ScanQuery.build_scan(
      scan,
      %{parts_full: parts_full, parts_partial: [], record_max: 0},
      %Policy.ScanQueryRuntime{
        timeout: @scan_timeout_ms,
        task_timeout: @scan_timeout_ms,
        pool_checkout_timeout: @scan_timeout_ms,
        max_concurrent_nodes: 0,
        task_id: task_id,
        cursor: nil
      }
    )
  end

  @spec random_task_id() :: pos_integer()
  def random_task_id do
    case :crypto.strong_rand_bytes(8) do
      <<0::64>> -> random_task_id()
      <<task_id::64-unsigned-big>> -> task_id
    end
  end

  defp master_partitions!(%Tcp{} = conn, namespace) do
    case Tcp.info(conn, ["node", "replicas"]) do
      {:ok, %{"node" => node_name, "replicas" => replicas}} ->
        replicas
        |> PartitionMapParser.parse_replicas_value(node_name)
        |> Enum.flat_map(fn
          {^namespace, partition_id, 0, ^node_name} -> [partition_id]
          _ -> []
        end)
        |> Enum.uniq()
        |> case do
          [] ->
            raise "stream scan proof could not derive master partitions for #{namespace} on #{node_name}"

          partitions ->
            partitions
        end

      other ->
        raise "stream scan proof could not fetch node/replicas info: #{inspect(other)}"
    end
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
    {:ok, operation} = Operation.write(bin_name, value)

    msg =
      AsmMsg.key_command(key, [operation],
        write: true,
        send_key: true,
        ttl: 0,
        timeout: @write_timeout_ms
      )

    Message.encode_as_msg_iodata(AsmMsg.encode(msg))
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
