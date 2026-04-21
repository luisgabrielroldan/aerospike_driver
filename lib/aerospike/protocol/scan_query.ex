defmodule Aerospike.Protocol.ScanQuery do
  @moduledoc false

  import Bitwise

  alias Aerospike.Filter
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Filter, as: FilterCodec
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.MessagePack
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

    filter =
      query.index_filter ||
        raise ArgumentError, "build_query/3 requires query.index_filter set via Query.where/2"

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
        |> Kernel.++(index_type_fields(filter))
        |> Kernel.++([index_range_field(filter)])
        |> Kernel.++(index_context_fields(filter))
        |> Kernel.++(index_name_fields(filter))
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

  @spec build_query_execute(Query.t(), node_partitions(), [Operation.t()], keyword()) :: iodata()
  def build_query_execute(%Query{} = query, node_partitions, operations, opts \\ [])
      when is_list(operations) and is_list(opts) do
    build_background_query(query, node_partitions, opts, operations: operations)
  end

  @spec build_query_udf(Query.t(), node_partitions(), String.t(), String.t(), list(), keyword()) ::
          iodata()
  def build_query_udf(%Query{} = query, node_partitions, package, function, args, opts \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    build_background_query(query, node_partitions, opts, udf: {2, package, function, args})
  end

  @spec build_query_aggregate(
          Query.t(),
          node_partitions(),
          String.t(),
          String.t(),
          list(),
          keyword()
        ) :: iodata()
  def build_query_aggregate(
        %Query{} = query,
        node_partitions,
        package,
        function,
        args,
        opts \\ []
      )
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    case query.index_filter do
      nil ->
        raise ArgumentError,
              "build_query_aggregate/6 requires query.index_filter set via Query.where/2"

      %Filter{} = index_filter ->
        build_query_with_filter(query, node_partitions, opts, index_filter,
          udf: {1, package, function, args}
        )
    end
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

  defp build_background_query(query, node_partitions, opts, background) when is_list(opts) do
    case query.index_filter do
      nil ->
        raise ArgumentError, "background query requires query.index_filter set via Query.where/2"

      %Filter{} = index_filter ->
        build_background_query_with_filter(query, node_partitions, opts, index_filter, background)
    end
  end

  defp build_query_with_filter(query, node_partitions, opts, %Filter{} = index_filter, background) do
    timeout = Keyword.get(opts, :timeout, 30_000)
    query_id = task_id_u64(opts)

    fields =
      [
        Field.namespace(query.namespace),
        Field.set(query.set),
        query_id_field(query_id)
      ]
      |> Kernel.++(index_type_fields(index_filter))
      |> Kernel.++([index_range_field(index_filter)])
      |> Kernel.++(index_context_fields(index_filter))
      |> Kernel.++(index_name_fields(index_filter))
      |> Kernel.++(pid_array_fields(node_partitions.parts_full))
      |> Kernel.++(digest_array_fields(node_partitions.parts_partial))
      |> Kernel.++(bval_array_fields(node_partitions.parts_partial))
      |> Kernel.++(max_records_fields(node_partitions.record_max))
      |> Kernel.++(records_per_second_fields(query.records_per_second))
      |> Kernel.++([socket_timeout_field(timeout)])
      |> Kernel.++(background_query_fields(background))

    %AsmMsg{
      info1: query_info1(query),
      info2: 0,
      info3: @info3_partition_done,
      timeout: timeout,
      fields: fields,
      operations: background_query_operations(background)
    }
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp build_background_query_with_filter(
         query,
         node_partitions,
         opts,
         %Filter{} = index_filter,
         background
       ) do
    timeout = Keyword.get(opts, :timeout, 30_000)
    query_id = task_id_u64(opts)

    fields =
      [
        Field.namespace(query.namespace),
        Field.set(query.set),
        query_id_field(query_id)
      ]
      |> Kernel.++(index_type_fields(index_filter))
      |> Kernel.++([index_range_field(index_filter)])
      |> Kernel.++(index_context_fields(index_filter))
      |> Kernel.++(index_name_fields(index_filter))
      |> Kernel.++(pid_array_fields(node_partitions.parts_full))
      |> Kernel.++(digest_array_fields(node_partitions.parts_partial))
      |> Kernel.++(bval_array_fields(node_partitions.parts_partial))
      |> Kernel.++(max_records_fields(node_partitions.record_max))
      |> Kernel.++(records_per_second_fields(query.records_per_second))
      |> Kernel.++([socket_timeout_field(timeout)])
      |> Kernel.++(background_query_fields(background))

    %AsmMsg{
      info1: 0,
      info2: AsmMsg.info2_write(),
      info3: @info3_partition_done,
      timeout: timeout,
      fields: fields,
      operations: background_query_operations(background)
    }
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp background_query_fields(operations: _operations), do: []

  defp background_query_fields(udf: {mode, package, function, args}) do
    query_udf_fields(udf: {mode, package, function, args})
  end

  defp background_query_operations(operations: operations), do: operations
  defp background_query_operations(udf: _udf), do: []

  defp query_udf_fields(udf: {mode, package, function, args}) do
    arglist = MessagePack.pack!(Enum.map(args, &pack_udf_arg/1))

    [
      Field.udf_op(mode),
      Field.udf_package_name(package),
      Field.udf_function(function),
      Field.udf_arglist(arglist)
    ]
  end

  defp pack_udf_arg(s) when is_binary(s), do: {:particle_string, s}
  defp pack_udf_arg({:bytes, b}) when is_binary(b), do: {:bytes, b}
  defp pack_udf_arg(nil), do: nil
  defp pack_udf_arg(true), do: true
  defp pack_udf_arg(false), do: false
  defp pack_udf_arg(n) when is_integer(n), do: n
  defp pack_udf_arg(f) when is_float(f), do: f
  defp pack_udf_arg(list) when is_list(list), do: Enum.map(list, &pack_udf_arg/1)

  defp pack_udf_arg(%{} = map) do
    Map.new(map, fn {k, v} -> {pack_udf_arg(k), pack_udf_arg(v)} end)
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

  defp index_type_fields(%Filter{index_type: index_type})
       when index_type in [:list, :mapkeys, :mapvalues] do
    [%Field{type: Field.type_index_type(), data: index_type_string(index_type)}]
  end

  defp index_type_fields(_), do: []

  defp index_range_field(%Filter{} = filter) do
    %Field{type: Field.type_index_range(), data: FilterCodec.encode(filter)}
  end

  defp index_context_fields(%Filter{ctx: nil}), do: []

  defp index_context_fields(%Filter{ctx: ctx}) when is_list(ctx) do
    [%Field{type: Field.type_index_context(), data: FilterCodec.encode_ctx(ctx)}]
  end

  defp index_name_fields(%Filter{index_name: nil}), do: []

  defp index_name_fields(%Filter{index_name: index_name}) do
    [%Field{type: Field.type_index_name(), data: index_name}]
  end

  defp index_type_string(:list), do: "LIST"
  defp index_type_string(:mapkeys), do: "MAPKEYS"
  defp index_type_string(:mapvalues), do: "MAPVALUES"

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
