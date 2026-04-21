defmodule Aerospike.Protocol.ScanQuery do
  @moduledoc false

  import Bitwise

  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Query
  alias Aerospike.Scan

  @info3_partition_done 0x04

  @type node_partitions :: %{
          required(:parts_full) => [non_neg_integer()],
          required(:parts_partial) => [partition_entry()],
          required(:record_max) => non_neg_integer()
        }

  @type partition_entry :: %{
          required(:id) => non_neg_integer(),
          optional(:digest) => binary() | nil,
          optional(:bval) => integer() | nil
        }

  @spec build_scan(Scan.t(), node_partitions(), keyword()) :: iodata()
  def build_scan(%Scan{} = scan, node_partitions, opts \\ []) when is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 30_000)
    query_id = task_id_u64(opts)

    %AsmMsg{
      info1: scan_info1(scan),
      info3: @info3_partition_done,
      timeout: timeout,
      fields:
        [
          Field.namespace(scan.namespace)
        ]
        |> maybe_append_table(scan.set)
        |> Kernel.++(pid_array_fields(node_partitions.parts_full))
        |> Kernel.++(digest_array_fields(node_partitions.parts_partial))
        |> Kernel.++(max_records_fields(node_partitions.record_max))
        |> Kernel.++(records_per_second_fields(scan.records_per_second))
        |> Kernel.++([socket_timeout_field(timeout), query_id_field(query_id)]),
      operations: scan_operations(scan)
    }
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  @spec build_query(Query.t(), node_partitions(), keyword()) :: iodata()
  def build_query(%Query{} = query, node_partitions, opts \\ []) when is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 30_000)
    query_id = task_id_u64(opts)

    %AsmMsg{
      info1: query_info1(query),
      info3: @info3_partition_done,
      timeout: timeout,
      fields:
        [
          Field.namespace(query.namespace),
          Field.set(query.set),
          query_id_field(query_id)
        ]
        |> Kernel.++(index_range_fields(query.index_filter))
        |> Kernel.++(pid_array_fields(node_partitions.parts_full))
        |> Kernel.++(digest_array_fields(node_partitions.parts_partial))
        |> Kernel.++(bval_array_fields(node_partitions.parts_partial))
        |> Kernel.++(max_records_fields(node_partitions.record_max))
        |> Kernel.++(records_per_second_fields(query.records_per_second))
        |> Kernel.++([socket_timeout_field(timeout)]),
      operations: query_operations(query)
    }
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp task_id_u64(opts) do
    case Keyword.get(opts, :task_id) do
      id when is_integer(id) and id >= 0 ->
        id

      _ ->
        <<id::64-unsigned-big>> = :crypto.strong_rand_bytes(8)
        id
    end
  end

  defp scan_info1(%Scan{no_bins: true}) do
    AsmMsg.info1_read() ||| AsmMsg.info1_nobindata()
  end

  defp scan_info1(%Scan{bin_names: []}) do
    AsmMsg.info1_read() ||| AsmMsg.info1_get_all()
  end

  defp scan_info1(%Scan{}), do: AsmMsg.info1_read()

  defp query_info1(%Query{no_bins: true}) do
    AsmMsg.info1_read() ||| AsmMsg.info1_nobindata()
  end

  defp query_info1(%Query{bin_names: bins}) when bins != [] do
    AsmMsg.info1_read()
  end

  defp query_info1(%Query{}), do: AsmMsg.info1_read() ||| AsmMsg.info1_get_all()

  defp scan_operations(%Scan{no_bins: true}), do: []
  defp scan_operations(%Scan{bin_names: []}), do: []

  defp scan_operations(%Scan{bin_names: bins}) do
    Enum.map(bins, &Operation.read/1)
  end

  defp query_operations(%Query{no_bins: true}), do: []
  defp query_operations(%Query{bin_names: []}), do: []

  defp query_operations(%Query{bin_names: bins}) do
    Enum.map(bins, &Operation.read/1)
  end

  defp maybe_append_table(fields, nil), do: fields
  defp maybe_append_table(fields, set) when is_binary(set), do: fields ++ [Field.set(set)]

  defp pid_array_fields([]), do: []

  defp pid_array_fields(ids) when is_list(ids) do
    data = for id <- ids, into: <<>>, do: <<id::16-little>>
    [%Field{type: Field.type_pid_array(), data: data}]
  end

  defp digest_array_fields([]), do: []

  defp digest_array_fields(partials) when is_list(partials) do
    data =
      partials
      |> Enum.map(fn %{digest: digest} -> digest end)
      |> Enum.filter(&is_binary/1)
      |> IO.iodata_to_binary()

    if data == <<>> do
      []
    else
      [%Field{type: Field.type_digest_array(), data: data}]
    end
  end

  defp bval_array_fields([]), do: []

  defp bval_array_fields(partials) when is_list(partials) do
    data =
      for partition <- partials, into: <<>> do
        bval =
          case Map.get(partition, :bval) do
            value when is_integer(value) -> value
            _ -> 0
          end

        <<bval::64-signed-little>>
      end

    [%Field{type: Field.type_bval_array(), data: data}]
  end

  defp index_range_fields(nil) do
    raise ArgumentError, "build_query/3 requires query.index_filter set to encoded wire bytes"
  end

  defp index_range_fields(%Field{data: data}), do: [index_range_field(data)]
  defp index_range_fields(data) when is_binary(data), do: [index_range_field(data)]

  defp index_range_fields(data) when is_list(data),
    do: [index_range_field(IO.iodata_to_binary(data))]

  defp index_range_fields(other) do
    raise ArgumentError,
          "build_query/3 requires query.index_filter to be encoded wire bytes, got: #{inspect(other)}"
  end

  defp index_range_field(data) do
    %Field{type: Field.type_index_range(), data: IO.iodata_to_binary(data)}
  end

  defp socket_timeout_field(ms) when is_integer(ms) do
    %Field{type: Field.type_socket_timeout(), data: <<ms::32-signed-big>>}
  end

  defp query_id_field(id) when is_integer(id) and id >= 0 do
    %Field{type: Field.type_query_id(), data: <<id::64-unsigned-big>>}
  end

  defp max_records_fields(n) when is_integer(n) and n > 0 do
    [%Field{type: Field.type_max_records(), data: <<n::64-signed-big>>}]
  end

  defp max_records_fields(_), do: []

  defp records_per_second_fields(0), do: []

  defp records_per_second_fields(n) when is_integer(n) and n > 0 do
    [%Field{type: Field.type_records_per_second(), data: <<n::32-signed-big>>}]
  end

  defp records_per_second_fields(_), do: []
end
