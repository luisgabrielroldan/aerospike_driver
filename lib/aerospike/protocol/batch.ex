defmodule Aerospike.Protocol.Batch do
  @moduledoc false

  import Bitwise

  alias Aerospike.Command.BatchCommand
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Command.BatchCommand.NodeRequest
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.OperateFlags
  alias Aerospike.Protocol.ResultCode
  alias Aerospike.Protocol.UdfArgs
  alias Aerospike.Record

  @batch_msg_info 0x02
  @batch_msg_gen 0x04
  @batch_msg_ttl 0x08

  @batch_row_read 0x00
  @batch_row_repeat 0x01
  @msg_remaining_header_size 22

  @type layout :: :batch_index | :batch_index_with_set

  defmodule Reply do
    @moduledoc false

    @enforce_keys [:results]
    defstruct [:results]

    @type t :: %__MODULE__{results: [BatchCommand.Result.t()]}
  end

  @spec encode_request(NodeRequest.t(), keyword()) :: iodata()
  def encode_request(%NodeRequest{} = node_request, opts \\ []) when is_list(opts) do
    layout = Keyword.get(opts, :layout, :batch_index)
    timeout = Keyword.get(opts, :timeout, 0)
    flags = batch_flags(opts)
    {info1, field_type, field_data} = batch_field(node_request, layout, flags)

    %AsmMsg{
      info1: info1,
      timeout: timeout,
      fields: [%Field{type: field_type, data: field_data}]
    }
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  @spec parse_response(binary(), NodeRequest.t()) :: {:ok, Reply.t()} | {:error, Error.t()}
  def parse_response(body, %NodeRequest{} = node_request) when is_binary(body) do
    allowed = entries_lookup(node_request.entries)

    with {:ok, results} <- decode_rows(body, allowed, []) do
      {:ok, %Reply{results: Enum.reverse(results)}}
    end
  end

  defp batch_field(%NodeRequest{entries: entries}, :batch_index, flags) do
    field_data =
      [<<length(entries)::32-big, flags::8>> | Enum.map(entries, &encode_mixed_entry/1)]
      |> IO.iodata_to_binary()

    {AsmMsg.info1_batch(), Field.type_batch_index(), field_data}
  end

  defp batch_field(%NodeRequest{entries: entries}, :batch_index_with_set, flags) do
    field_data =
      [<<length(entries)::32-big, flags::8>> | encode_read_entries(entries, nil)]
      |> IO.iodata_to_binary()

    {AsmMsg.info1_read() ||| AsmMsg.info1_batch(), Field.type_batch_index_with_set(), field_data}
  end

  defp encode_read_entries([], _prev_key), do: []

  defp encode_read_entries([%Entry{} = entry | rest], nil) do
    [encode_read_entry(entry, nil) | encode_read_entries(rest, entry.key)]
  end

  defp encode_read_entries([%Entry{} = entry | rest], %Key{} = prev_key) do
    [encode_read_entry(entry, prev_key) | encode_read_entries(rest, entry.key)]
  end

  defp encode_read_entry(
         %Entry{index: index, key: %Key{} = key, kind: kind, payload: payload},
         nil
       )
       when kind in [:read, :read_header, :exists] do
    [<<index::32-big, key.digest::binary, @batch_row_read::8>>, read_row_body(key, kind, payload)]
  end

  defp encode_read_entry(
         %Entry{index: index, key: %Key{} = key, kind: kind, payload: payload},
         %Key{} = prev_key
       )
       when kind in [:read, :read_header, :exists] do
    if repeat_key_scope?(prev_key, key) do
      <<index::32-big, key.digest::binary, @batch_row_repeat::8>>
    else
      [
        <<index::32-big, key.digest::binary, @batch_row_read::8>>,
        read_row_body(key, kind, payload)
      ]
    end
  end

  defp encode_mixed_entry(%Entry{index: index, key: %Key{} = key} = entry) do
    [<<index::32-big, key.digest::binary>>, mixed_row_body(entry)]
  end

  defp mixed_row_body(%Entry{key: %Key{} = key, kind: kind, payload: payload})
       when kind in [:read, :read_header, :exists] do
    payload = payload_opts(payload)
    operations = read_operations(payload)
    ttl = Map.get(payload, :read_touch_ttl_percent, 0)
    info1 = read_attr(kind, operations, payload)
    info3 = read_info3(payload)

    [
      <<@batch_msg_info ||| @batch_msg_ttl::8, info1::8, 0::8, info3::8, ttl::32-big>>,
      batch_key_fields(key, 0, length(operations), payload),
      Enum.map(operations, &Operation.encode/1)
    ]
  end

  defp mixed_row_body(%Entry{key: %Key{} = key, kind: :put, payload: payload}) do
    payload = payload_opts(payload)
    operations = put_operations(payload)

    {info2, info3, generation} =
      write_flags(payload, AsmMsg.info2_write() ||| AsmMsg.info2_respond_all_ops())

    ttl = Map.get(payload, :ttl, 0)

    [
      <<@batch_msg_info ||| @batch_msg_gen ||| @batch_msg_ttl::8, 0::8, info2::8, info3::8,
        generation::16-big, ttl::32-big>>,
      batch_key_fields(key, 0, length(operations), payload),
      Enum.map(operations, &Operation.encode/1)
    ]
  end

  defp mixed_row_body(%Entry{key: %Key{} = key, kind: :delete, payload: payload}) do
    payload = payload_opts(payload)

    info2 =
      AsmMsg.info2_write()
      |> bor(AsmMsg.info2_delete())
      |> bor(AsmMsg.info2_respond_all_ops())
      |> maybe_flag(Map.get(payload, :durable_delete, false), AsmMsg.info2_durable_delete())

    {info2, info3, generation} = write_flags(payload, info2)
    ttl = Map.get(payload, :ttl, 0)

    [
      <<@batch_msg_info ||| @batch_msg_gen ||| @batch_msg_ttl::8, 0::8, info2::8, info3::8,
        generation::16-big, ttl::32-big>>,
      batch_key_fields(key, 0, 0, payload)
    ]
  end

  defp mixed_row_body(%Entry{key: %Key{} = key, kind: :operate, payload: payload}) do
    payload = payload_opts(payload)
    operations = Map.fetch!(payload, :operations)
    flags = Map.get(payload, :flags, OperateFlags.scan_ops(operations))

    if flags.has_write? do
      info1 =
        if flags.read_bin? or flags.read_header? do
          maybe_header_flag(flags.info1, flags.header_only?)
        else
          0
        end

      info2 =
        flags.info2
        |> bor(AsmMsg.info2_write())
        |> maybe_flag(
          flags.respond_all? or Map.get(payload, :respond_per_op, false),
          AsmMsg.info2_respond_all_ops()
        )

      {info2, info3, generation} = write_flags(payload, info2)
      ttl = Map.get(payload, :ttl, 0)

      [
        <<@batch_msg_info ||| @batch_msg_gen ||| @batch_msg_ttl::8, info1::8, info2::8,
          bor(flags.info3, info3)::8, generation::16-big, ttl::32-big>>,
        batch_key_fields(key, 0, length(operations), payload),
        Enum.map(operations, &Operation.encode/1)
      ]
    else
      ttl = Map.get(payload, :read_touch_ttl_percent, 0)
      info1 = maybe_header_flag(flags.info1, flags.header_only?)
      info1 = read_attr(info1, payload)
      info3 = read_info3(payload) ||| flags.info3

      [
        <<@batch_msg_info ||| @batch_msg_ttl::8, info1::8, 0::8, info3::8, ttl::32-big>>,
        batch_key_fields(key, 0, length(operations), payload),
        Enum.map(operations, &Operation.encode/1)
      ]
    end
  end

  defp mixed_row_body(%Entry{key: %Key{} = key, kind: :udf, payload: payload}) do
    payload = payload_opts(payload)
    {info2, info3, generation} = write_flags(payload, AsmMsg.info2_write())
    ttl = Map.get(payload, :ttl, 0)
    udf_fields = udf_fields(payload)

    [
      <<@batch_msg_info ||| @batch_msg_gen ||| @batch_msg_ttl::8, 0::8, info2::8, info3::8,
        generation::16-big, ttl::32-big>>,
      batch_key_fields(key, length(udf_fields), 0, payload),
      udf_fields
    ]
  end

  defp read_row_body(%Key{} = key, kind, payload) when kind in [:read, :read_header, :exists] do
    payload = payload_opts(payload)

    [
      <<read_attr(kind, [], payload)::8, 2::16-big, 0::16-big>>,
      Field.encode(Field.namespace(key.namespace)),
      Field.encode(Field.set(key.set))
    ]
  end

  defp repeat_key_scope?(%Key{} = left, %Key{} = right) do
    left.namespace == right.namespace and left.set == right.set
  end

  defp read_attr(:read, [], payload),
    do: read_attr(AsmMsg.info1_read() ||| AsmMsg.info1_get_all(), payload)

  defp read_attr(:read, _ops, payload), do: read_attr(AsmMsg.info1_read(), payload)

  defp read_attr(:read_header, _ops, payload),
    do: read_attr(AsmMsg.info1_read() ||| AsmMsg.info1_nobindata(), payload)

  defp read_attr(:exists, _ops, payload),
    do: read_attr(AsmMsg.info1_read() ||| AsmMsg.info1_nobindata(), payload)

  defp read_attr(info1, payload) do
    info1
    |> maybe_flag(Map.get(payload, :read_mode_ap) == :all, AsmMsg.info1_read_mode_ap_all())
  end

  defp read_info3(payload) do
    case Map.get(payload, :read_mode_sc, :session) do
      :session -> 0
      :linearize -> AsmMsg.info3_sc_read_type()
      :allow_replica -> AsmMsg.info3_sc_read_relax()
      :allow_unavailable -> AsmMsg.info3_sc_read_type() ||| AsmMsg.info3_sc_read_relax()
    end
  end

  defp read_operations(payload) do
    cond do
      match?(%{operations: operations} when is_list(operations), payload) ->
        Map.fetch!(payload, :operations)

      match?(%{bins: bins} when is_list(bins), payload) ->
        payload
        |> Map.fetch!(:bins)
        |> Enum.map(&normalize_bin_name/1)
        |> Enum.sort()
        |> Enum.map(&Operation.read/1)

      true ->
        []
    end
  end

  defp put_operations(%{operations: operations}) when is_list(operations), do: operations

  defp put_operations(%{bins: bins}) when is_map(bins) do
    bins
    |> Enum.map(fn {name, value} ->
      {:ok, operation} = Operation.write(normalize_bin_name(name), value)
      operation
    end)
  end

  defp put_operations(_payload), do: []

  defp batch_key_fields(%Key{} = key, extra_field_count, op_count, payload) do
    send_key? = Map.get(payload, :send_key, false)
    key_field = if send_key?, do: Field.key_from_user_key(%{user_key: key.user_key}), else: nil
    filter_field = filter_field(Map.get(payload, :filter))

    field_count =
      2 + extra_field_count + if(key_field, do: 1, else: 0) + if(filter_field, do: 1, else: 0)

    [
      <<field_count::16-big, op_count::16-big>>,
      Field.encode(Field.namespace(key.namespace)),
      Field.encode(Field.set(key.set))
      | maybe_encoded_fields([key_field, filter_field])
    ]
  end

  defp filter_field(nil), do: nil
  defp filter_field(%Aerospike.Exp{wire: wire}) when is_binary(wire), do: Field.filter_exp(wire)

  defp maybe_encoded_fields(fields) do
    fields
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&Field.encode/1)
  end

  defp udf_fields(payload) do
    package = Map.fetch!(payload, :package)
    function = Map.fetch!(payload, :function)
    args = payload |> Map.get(:args, []) |> UdfArgs.pack!()

    [
      Field.encode(Field.udf_package_name(package)),
      Field.encode(Field.udf_function(function)),
      Field.encode(Field.udf_arglist(args))
    ]
  end

  defp maybe_header_flag(info1, true), do: info1 ||| AsmMsg.info1_nobindata()
  defp maybe_header_flag(info1, false), do: info1

  defp write_flags(payload, info2) do
    generation = Map.get(payload, :generation, 0)

    generation_policy =
      Map.get(payload, :generation_policy, default_generation_policy(generation))

    exists = Map.get(payload, :exists, :update)

    info2 =
      info2
      |> maybe_generation_flag(generation_policy)
      |> maybe_flag(exists == :create_only, AsmMsg.info2_create_only())
      |> maybe_flag(Map.get(payload, :durable_delete, false), AsmMsg.info2_durable_delete())
      |> maybe_flag(Map.get(payload, :respond_per_op, false), AsmMsg.info2_respond_all_ops())

    info3 =
      0
      |> maybe_flag(exists == :update_only, AsmMsg.info3_update_only())
      |> maybe_flag(exists == :create_or_replace, AsmMsg.info3_create_or_replace())
      |> maybe_flag(exists == :replace_only, AsmMsg.info3_replace_only())
      |> maybe_flag(
        Map.get(payload, :commit_level, :all) == :master,
        AsmMsg.info3_commit_master()
      )
      |> bor(read_info3(payload))

    {info2, info3, generation}
  end

  defp maybe_generation_flag(info2, :none), do: info2
  defp maybe_generation_flag(info2, :expect_equal), do: info2 ||| AsmMsg.info2_generation()
  defp maybe_generation_flag(info2, :expect_gt), do: info2 ||| AsmMsg.info2_generation_gt()

  defp default_generation_policy(generation) when generation > 0, do: :expect_equal
  defp default_generation_policy(_generation), do: :none

  defp batch_flags(opts) do
    0
    |> maybe_flag(Keyword.get(opts, :allow_inline, true), 0x01)
    |> maybe_flag(Keyword.get(opts, :allow_inline_ssd, false), 0x02)
    |> maybe_flag(Keyword.get(opts, :respond_all_keys, true), 0x04)
  end

  defp maybe_flag(bits, true, flag), do: bits ||| flag
  defp maybe_flag(bits, false, _flag), do: bits

  defp payload_opts(nil), do: %{}
  defp payload_opts(payload) when is_map(payload), do: payload
  defp payload_opts(payload) when is_list(payload), do: Map.new(payload)

  defp normalize_bin_name(bin_name) when is_atom(bin_name), do: Atom.to_string(bin_name)
  defp normalize_bin_name(bin_name), do: bin_name

  defp decode_rows(<<>>, _allowed, _acc) do
    {:error,
     Error.from_result_code(:parse_error,
       message: "batch reply ended without a terminal marker"
     )}
  end

  defp decode_rows(
         <<
           @msg_remaining_header_size::8,
           _info1::8,
           _info2::8,
           info3::8,
           _info4::8,
           result_code::8,
           generation::32-big,
           expiration::32-big,
           batch_index::32-big,
           field_count::16-big,
           op_count::16-big,
           rest::binary
         >>,
         allowed,
         acc
       ) do
    if (info3 &&& AsmMsg.info3_last()) == AsmMsg.info3_last() do
      decode_last_marker(rest, result_code, field_count, op_count, acc)
    else
      with {:ok, entry} <- fetch_entry(allowed, batch_index),
           {:ok, fields_rest} <- skip_fields(rest, field_count),
           {:ok, result, remaining} <-
             decode_row_result(entry, result_code, generation, expiration, fields_rest, op_count) do
        decode_rows(remaining, allowed, [result | acc])
      end
    end
  end

  defp decode_rows(<<header_size::8, _rest::binary>>, _allowed, _acc) do
    {:error,
     Error.from_result_code(:parse_error,
       message: "expected batch row header size 22, got #{header_size}"
     )}
  end

  defp decode_rows(_other, _allowed, _acc) do
    {:error, Error.from_result_code(:parse_error, message: "incomplete batch row header")}
  end

  defp decode_last_marker(<<>>, 0, 0, 0, acc), do: {:ok, acc}

  defp decode_last_marker(_rest, result_code, _field_count, _op_count, _acc)
       when result_code != 0 do
    {:error, result_error(result_code, "batch terminal marker returned an error")}
  end

  defp decode_last_marker(_rest, _result_code, field_count, op_count, _acc) do
    {:error,
     Error.from_result_code(:parse_error,
       message:
         "batch terminal marker expected field_count=0 and op_count=0, got #{field_count}/#{op_count}"
     )}
  end

  defp entries_lookup(entries) do
    if contiguous_entries?(entries) do
      [%Entry{index: offset} | _rest] = entries
      {:contiguous, offset, List.to_tuple(entries)}
    else
      {:map, Map.new(entries, &{&1.index, &1})}
    end
  end

  defp contiguous_entries?([]), do: false

  defp contiguous_entries?([%Entry{index: index} | rest]) do
    contiguous_entries?(rest, index + 1)
  end

  defp contiguous_entries?([], _next_index), do: true

  defp contiguous_entries?([%Entry{index: index} | rest], next_index) when index == next_index do
    contiguous_entries?(rest, next_index + 1)
  end

  defp contiguous_entries?([%Entry{} | _rest], _next_index), do: false

  defp fetch_entry({:contiguous, offset, entries}, batch_index) do
    position = batch_index - offset

    if position >= 0 and position < tuple_size(entries) do
      case elem(entries, position) do
        %Entry{index: ^batch_index} = entry -> {:ok, entry}
        %Entry{} -> unknown_batch_index(batch_index)
      end
    else
      unknown_batch_index(batch_index)
    end
  end

  defp fetch_entry({:map, allowed}, batch_index) do
    case Map.fetch(allowed, batch_index) do
      {:ok, %Entry{} = entry} ->
        {:ok, entry}

      :error ->
        unknown_batch_index(batch_index)
    end
  end

  defp unknown_batch_index(batch_index) do
    {:error,
     Error.from_result_code(:parse_error,
       message: "batch reply referenced unknown batch index #{batch_index}"
     )}
  end

  defp build_result(%Entry{} = entry, 0, generation, expiration, operations) do
    success_result(entry, generation, expiration, operations)
  end

  defp build_result(%Entry{} = entry, result_code, _generation, _expiration, _operations) do
    {:ok,
     %BatchCommand.Result{
       index: entry.index,
       key: entry.key,
       kind: entry.kind,
       status: :error,
       record: nil,
       error: result_error(result_code, "batch row returned an error"),
       in_doubt: false
     }}
  end

  defp decode_row_result(%Entry{} = entry, result_code, generation, expiration, binary, op_count)
       when result_code != 0 do
    with {:ok, operations, remaining} <- decode_operations(binary, op_count),
         {:ok, result} <- build_result(entry, result_code, generation, expiration, operations) do
      {:ok, result, remaining}
    end
  end

  defp decode_row_result(
         %Entry{kind: kind} = entry,
         0,
         generation,
         expiration,
         binary,
         op_count
       )
       when kind in [:read, :operate] do
    with {:ok, bins, remaining} <- decode_bins_from_operations(binary, op_count) do
      {:ok,
       %BatchCommand.Result{
         index: entry.index,
         key: entry.key,
         kind: entry.kind,
         status: :ok,
         record: %Record{key: entry.key, bins: bins, generation: generation, ttl: expiration},
         error: nil,
         in_doubt: false
       }, remaining}
    end
  end

  defp decode_row_result(%Entry{kind: :udf} = entry, 0, generation, expiration, binary, op_count) do
    with {:ok, bins, remaining} <- decode_bins_from_operations(binary, op_count) do
      record =
        case bins do
          %{} when op_count == 0 -> nil
          %{} -> %Record{key: entry.key, bins: bins, generation: generation, ttl: expiration}
        end

      {:ok,
       %BatchCommand.Result{
         index: entry.index,
         key: entry.key,
         kind: entry.kind,
         status: :ok,
         record: record,
         error: nil,
         in_doubt: false
       }, remaining}
    end
  end

  defp decode_row_result(%Entry{kind: kind} = entry, 0, generation, expiration, binary, op_count)
       when kind in [:put, :delete] do
    with {:ok, remaining} <- skip_operations(binary, op_count),
         {:ok, result} <- success_result(entry, generation, expiration, []) do
      {:ok, result, remaining}
    end
  end

  defp decode_row_result(
         %Entry{kind: kind},
         0,
         _generation,
         _expiration,
         _binary,
         op_count
       )
       when kind in [:read_header, :exists] and op_count > 0 do
    message =
      case kind do
        :read_header -> "header-only batch read reply contained #{op_count} operations"
        :exists -> "exists batch reply contained #{op_count} operations"
      end

    {:error, Error.from_result_code(:parse_error, message: message)}
  end

  defp decode_row_result(%Entry{kind: kind} = entry, 0, generation, expiration, binary, 0)
       when kind in [:read_header, :exists] do
    {:ok, result} = success_result(entry, generation, expiration, [])
    {:ok, result, binary}
  end

  defp success_result(%Entry{kind: :read} = entry, generation, expiration, operations) do
    with {:ok, bins} <- decode_bins(operations) do
      {:ok,
       %BatchCommand.Result{
         index: entry.index,
         key: entry.key,
         kind: entry.kind,
         status: :ok,
         record: %Record{key: entry.key, bins: bins, generation: generation, ttl: expiration},
         error: nil,
         in_doubt: false
       }}
    end
  end

  defp success_result(%Entry{kind: kind} = entry, generation, expiration, [])
       when kind in [:read_header, :exists] do
    {:ok,
     %BatchCommand.Result{
       index: entry.index,
       key: entry.key,
       kind: entry.kind,
       status: :ok,
       record: %{generation: generation, ttl: expiration},
       error: nil,
       in_doubt: false
     }}
  end

  defp success_result(%Entry{kind: kind}, _generation, _expiration, operations)
       when kind in [:read_header, :exists] do
    message =
      case kind do
        :read_header -> "header-only batch read reply contained #{length(operations)} operations"
        :exists -> "exists batch reply contained #{length(operations)} operations"
      end

    {:error, Error.from_result_code(:parse_error, message: message)}
  end

  defp success_result(%Entry{kind: kind} = entry, _generation, _expiration, _operations)
       when kind in [:put, :delete] do
    {:ok,
     %BatchCommand.Result{
       index: entry.index,
       key: entry.key,
       kind: entry.kind,
       status: :ok,
       record: nil,
       error: nil,
       in_doubt: false
     }}
  end

  defp success_result(%Entry{kind: :operate} = entry, generation, expiration, operations) do
    with {:ok, bins} <- decode_bins(operations) do
      {:ok,
       %BatchCommand.Result{
         index: entry.index,
         key: entry.key,
         kind: entry.kind,
         status: :ok,
         record: %Record{key: entry.key, bins: bins, generation: generation, ttl: expiration},
         error: nil,
         in_doubt: false
       }}
    end
  end

  defp success_result(%Entry{kind: :udf} = entry, generation, expiration, operations) do
    record =
      case decode_bins(operations) do
        {:ok, %{}} when operations == [] ->
          nil

        {:ok, bins} ->
          %Record{key: entry.key, bins: bins, generation: generation, ttl: expiration}

        {:error, %Error{} = error} ->
          throw({:decode_error, error})
      end

    {:ok,
     %BatchCommand.Result{
       index: entry.index,
       key: entry.key,
       kind: entry.kind,
       status: :ok,
       record: record,
       error: nil,
       in_doubt: false
     }}
  catch
    {:decode_error, %Error{} = error} -> {:error, error}
  end

  defp skip_fields(binary, 0), do: {:ok, binary}

  defp skip_fields(binary, count) when count > 0 do
    case Field.decode(binary) do
      {:ok, _field, rest} -> skip_fields(rest, count - 1)
      {:error, reason} -> {:error, parse_field_error(reason)}
    end
  end

  defp decode_operations(binary, 0), do: {:ok, [], binary}

  defp decode_operations(binary, count) when count > 0 do
    decode_operations(binary, count, [])
  end

  defp decode_operations(binary, 0, acc), do: {:ok, Enum.reverse(acc), binary}

  defp decode_operations(binary, count, acc) do
    case Operation.decode(binary) do
      {:ok, operation, rest} -> decode_operations(rest, count - 1, [operation | acc])
      {:error, reason} -> {:error, parse_operation_error(reason)}
    end
  end

  defp decode_bins(operations) do
    Enum.reduce_while(operations, {:ok, %{bins: %{}, counts: %{}}}, fn
      %Operation{bin_name: ""}, {:ok, acc} ->
        {:cont, {:ok, acc}}

      %Operation{bin_name: name} = operation, {:ok, acc} ->
        {:ok, value} = Value.decode_value(operation.particle_type, operation.data)
        {:cont, {:ok, put_bin_value(acc, name, value)}}
    end)
    |> case do
      {:ok, %{bins: bins}} -> {:ok, bins}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp decode_bins_from_operations(binary, count) do
    decode_bins_from_operations(binary, count, %{bins: %{}, counts: %{}})
  end

  defp decode_bins_from_operations(binary, 0, %{bins: bins}), do: {:ok, bins, binary}

  defp decode_bins_from_operations(
         <<size::32-big, _op_type::8, particle_type::8, _reserved::8, name_len::8, rest::binary>>,
         count,
         acc
       )
       when count > 0 and size >= 4 do
    data_len = size - 4 - name_len

    cond do
      data_len < 0 ->
        {:error, parse_operation_error(:invalid_operation_size)}

      byte_size(rest) < name_len + data_len ->
        {:error, parse_operation_error(:incomplete_operation)}

      true ->
        <<bin_name::binary-size(name_len), data::binary-size(data_len), remaining::binary>> = rest

        acc =
          if bin_name == "" do
            acc
          else
            {:ok, value} = Value.decode_value(particle_type, data)
            put_bin_value(acc, bin_name, value)
          end

        decode_bins_from_operations(remaining, count - 1, acc)
    end
  end

  defp decode_bins_from_operations(<<_size::32-big, _rest::binary>>, count, _acc)
       when count > 0 do
    {:error, parse_operation_error(:invalid_operation_size)}
  end

  defp decode_bins_from_operations(_binary, count, _acc) when count > 0 do
    {:error, parse_operation_error(:incomplete_operation_header)}
  end

  defp skip_operations(binary, 0), do: {:ok, binary}

  defp skip_operations(
         <<size::32-big, _op_type::8, _particle_type::8, _reserved::8, name_len::8,
           rest::binary>>,
         count
       )
       when count > 0 and size >= 4 do
    data_len = size - 4 - name_len

    cond do
      data_len < 0 ->
        {:error, parse_operation_error(:invalid_operation_size)}

      byte_size(rest) < name_len + data_len ->
        {:error, parse_operation_error(:incomplete_operation)}

      true ->
        <<_bin_name::binary-size(name_len), _data::binary-size(data_len), remaining::binary>> =
          rest

        skip_operations(remaining, count - 1)
    end
  end

  defp skip_operations(<<_size::32-big, _rest::binary>>, count) when count > 0 do
    {:error, parse_operation_error(:invalid_operation_size)}
  end

  defp skip_operations(_binary, count) when count > 0 do
    {:error, parse_operation_error(:incomplete_operation_header)}
  end

  defp put_bin_value(%{bins: bins, counts: counts} = acc, name, value) do
    count = Map.get(counts, name, 0) + 1

    next_bins =
      case count do
        1 -> Map.put(bins, name, value)
        2 -> Map.put(bins, name, [Map.fetch!(bins, name), value])
        _ -> Map.update!(bins, name, &(&1 ++ [value]))
      end

    %{acc | bins: next_bins, counts: Map.put(counts, name, count)}
  end

  defp result_error(result_code, prefix) do
    case ResultCode.from_integer(result_code) do
      {:ok, code} ->
        Error.from_result_code(code, message: "#{prefix}: #{ResultCode.message(code)}")

      {:error, unknown} ->
        Error.from_result_code(:server_error,
          message: "#{prefix}: unknown result code #{unknown}"
        )
    end
  end

  defp parse_field_error(reason) do
    Error.from_result_code(:parse_error, message: "failed to parse batch fields: #{reason}")
  end

  defp parse_operation_error(reason) do
    Error.from_result_code(:parse_error,
      message: "failed to parse batch operations: #{reason}"
    )
  end
end
