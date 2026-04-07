defmodule Aerospike.Protocol.ScanQuery do
  @moduledoc false

  import Bitwise

  alias Aerospike.Exp
  alias Aerospike.Filter
  alias Aerospike.PartitionFilter
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Query
  alias Aerospike.Scan

  @exp_op_and 16

  @type node_partitions :: %{
          required(:parts_full) => [non_neg_integer()],
          required(:parts_partial) => [partial_partition()],
          required(:record_max) => non_neg_integer()
        }

  @type partial_partition :: PartitionFilter.partition_entry()

  @doc """
  Builds a framed AS_MSG wire binary for a partition scan on one node.

  `node_partitions` supplies PID_ARRAY / DIGEST_ARRAY data and the per-node `record_max`
  cap (from the orchestrator). `opts`: `:timeout` (default `30_000` ms), `:task_id` (`uint64`).
  """
  @spec build_scan(Scan.t(), node_partitions(), keyword()) :: binary()
  def build_scan(%Scan{} = scan, node_partitions, opts \\ []) when is_list(opts) do
    sock_ms = Keyword.get(opts, :timeout, 30_000)
    query_id = task_id_u64(opts)

    info1 = scan_info1(scan)
    info3 = AsmMsg.info3_partition_done()

    exp_bin = merge_exp_filters(scan.filters)

    fields =
      [
        Field.namespace(scan.namespace)
      ]
      |> maybe_append_table(scan.set)
      |> Kernel.++(pid_array_fields(node_partitions.parts_full))
      |> Kernel.++(digest_array_fields(node_partitions.parts_partial))
      |> Kernel.++(filter_exp_fields(exp_bin))
      |> Kernel.++(max_records_fields(:scan, node_partitions.record_max))
      |> Kernel.++(records_per_second_fields(scan.records_per_second))
      |> Kernel.++([
        socket_timeout_field(sock_ms),
        query_id_field(query_id)
      ])

    ops = scan_operations(scan)

    %AsmMsg{
      info1: info1,
      info2: 0,
      info3: info3,
      info4: 0,
      timeout: sock_ms,
      fields: fields,
      operations: ops
    }
    |> AsmMsg.encode()
    |> Message.encode_as_msg()
  end

  @doc """
  Builds a framed AS_MSG wire binary for a secondary-index query on one node.

  Requires `query.index_filter`. See `build_scan/3` for `node_partitions` and `opts`.
  """
  @spec build_query(Query.t(), node_partitions(), keyword()) :: binary()
  def build_query(%Query{} = query, node_partitions, opts \\ []) when is_list(opts) do
    case query.index_filter do
      nil ->
        raise ArgumentError, "build_query/3 requires query.index_filter set via Query.where/2"

      %Filter{} = index_filter ->
        build_query_with_filter(query, node_partitions, opts, index_filter)
    end
  end

  defp build_query_with_filter(query, node_partitions, opts, index_filter) do
    sock_ms = Keyword.get(opts, :timeout, 30_000)
    query_id = task_id_u64(opts)

    info1 =
      query_info1(query)
      |> query_info1_short_query(node_partitions.record_max)

    info3 = AsmMsg.info3_partition_done()
    exp_bin = merge_exp_filters(query.filters)

    fields =
      [
        Field.namespace(query.namespace),
        Field.set(query.set),
        query_id_field(query_id)
      ]
      |> Kernel.++(index_type_fields(index_filter))
      |> Kernel.++([
        %Field{
          type: Field.type_index_range(),
          data: Aerospike.Protocol.Filter.encode(index_filter)
        }
      ])
      |> Kernel.++(index_name_fields(index_filter))
      |> Kernel.++(pid_array_fields(node_partitions.parts_full))
      |> Kernel.++(digest_array_fields(node_partitions.parts_partial))
      |> Kernel.++(bval_array_fields(index_filter, node_partitions.parts_partial))
      |> Kernel.++(max_records_fields(:query, node_partitions.record_max))
      |> Kernel.++([
        socket_timeout_field(sock_ms),
        records_per_second_field(query.records_per_second)
      ])
      |> Enum.reject(&is_nil/1)
      |> Kernel.++(filter_exp_fields(exp_bin))

    ops = query_operations(query)

    %AsmMsg{
      info1: info1,
      info2: AsmMsg.info2_relax_ap_long_query(),
      info3: info3,
      info4: 0,
      timeout: sock_ms,
      fields: fields,
      operations: ops
    }
    |> AsmMsg.encode()
    |> Message.encode_as_msg()
  end

  defp task_id_u64(opts) do
    case Keyword.get(opts, :task_id) do
      n when is_integer(n) and n >= 0 ->
        n

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

  defp scan_info1(%Scan{}) do
    AsmMsg.info1_read()
  end

  defp query_info1(%Query{no_bins: true}) do
    AsmMsg.info1_read() ||| AsmMsg.info1_nobindata()
  end

  defp query_info1(%Query{bin_names: bins}) when bins != [] do
    AsmMsg.info1_read()
  end

  defp query_info1(%Query{}) do
    AsmMsg.info1_read() ||| AsmMsg.info1_get_all()
  end

  # Aligns with Java QueryPolicy short-path: small per-node caps use INFO1_SHORT_QUERY so the
  # server may inline the query (avoids query-thread timeouts on modest SI predicates).
  defp query_info1_short_query(info1, record_max)
       when is_integer(record_max) and record_max > 0 and record_max <= 100_000 do
    info1 ||| AsmMsg.info1_short_query()
  end

  defp query_info1_short_query(info1, _), do: info1

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
      |> Enum.map(fn %{digest: d} -> d end)
      |> IO.iodata_to_binary()

    [%Field{type: Field.type_digest_array(), data: data}]
  end

  defp bval_array_fields(_index_filter, []), do: []

  defp bval_array_fields(%Filter{}, partials) when is_list(partials) do
    data =
      for p <- partials, into: <<>> do
        n =
          case Map.get(p, :bval) do
            v when is_integer(v) -> v
            _ -> 0
          end

        <<n::64-signed-little>>
      end

    [%Field{type: Field.type_bval_array(), data: data}]
  end

  defp index_type_fields(%Filter{index_type: :default}), do: []

  defp index_type_fields(%Filter{index_type: :list}) do
    [%Field{type: Field.type_index_type(), data: <<1::8>>}]
  end

  defp index_type_fields(%Filter{index_type: :mapkeys}) do
    [%Field{type: Field.type_index_type(), data: <<2::8>>}]
  end

  defp index_type_fields(%Filter{index_type: :mapvalues}) do
    [%Field{type: Field.type_index_type(), data: <<3::8>>}]
  end

  defp index_type_fields(%Filter{index_type: t})
       when t in [:geo_within, :geo_contains] do
    []
  end

  defp index_name_fields(%Filter{index_name: name}) when is_binary(name) and name != "" do
    [%Field{type: Field.type_index_name(), data: name}]
  end

  defp index_name_fields(%Filter{}), do: []

  defp socket_timeout_field(ms) when is_integer(ms) do
    %Field{type: Field.type_socket_timeout(), data: <<ms::32-signed-big>>}
  end

  defp query_id_field(id) when is_integer(id) and id >= 0 do
    %Field{type: Field.type_query_id(), data: <<id::64-unsigned-big>>}
  end

  defp max_records_fields(:scan, n) when is_integer(n) and n > 0 do
    [%Field{type: Field.type_max_records(), data: <<n::64-signed-big>>}]
  end

  defp max_records_fields(:scan, _), do: []

  defp max_records_fields(:query, n) when is_integer(n) and n > 0 do
    [%Field{type: Field.type_max_records(), data: <<n::64-signed-big>>}]
  end

  defp max_records_fields(:query, _), do: []

  defp records_per_second_fields(0), do: []

  defp records_per_second_fields(n) when is_integer(n) and n > 0 do
    [records_per_second_field(n)]
  end

  defp records_per_second_field(n) when is_integer(n) and n > 0 do
    %Field{type: Field.type_records_per_second(), data: <<n::32-signed-big>>}
  end

  defp records_per_second_field(_), do: nil

  defp filter_exp_fields(nil), do: []

  defp filter_exp_fields(wire) when is_binary(wire) and wire != <<>> do
    [%Field{type: Field.type_filter_exp(), data: wire}]
  end

  defp merge_exp_filters([]), do: nil

  defp merge_exp_filters([%Exp{wire: w}]), do: w

  defp merge_exp_filters([%Exp{wire: first} | rest]) do
    Enum.reduce(rest, first, fn %Exp{wire: w}, acc ->
      # MessagePack fixarray(3): [EXP_OP_AND, left, right] — matches Go `Expression.packExpression`.
      <<0x93, @exp_op_and::8>> <> acc <> w
    end)
  end
end
