defmodule Aerospike.Protocol.BatchRead do
  @moduledoc false

  alias Aerospike.BatchCommand.NodeRequest
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.ResultCode

  import Bitwise

  @batch_row_read 0x00
  @batch_row_repeat 0x01
  @msg_remaining_header_size 22

  defmodule RecordResult do
    @moduledoc false

    @enforce_keys [:index, :key, :result]
    defstruct [:index, :key, :result, :generation, :ttl, :bins]

    @type t :: %__MODULE__{
            index: non_neg_integer(),
            key: Key.t(),
            result: :ok | {:error, Error.t()},
            generation: non_neg_integer() | nil,
            ttl: non_neg_integer() | nil,
            bins: map() | nil
          }
  end

  defmodule Reply do
    @moduledoc false

    @enforce_keys [:records]
    defstruct [:records]

    @type t :: %__MODULE__{records: [RecordResult.t()]}
  end

  @type mode :: :all_bins | :header

  @doc """
  Encodes one homogeneous batch read/header request for a grouped node.
  """
  @spec encode_request(NodeRequest.t(), keyword()) :: iodata()
  def encode_request(%NodeRequest{} = node_request, opts \\ []) when is_list(opts) do
    mode = Keyword.get(opts, :mode, :all_bins)

    node_request
    |> batch_field_data(mode)
    |> AsmMsg.batch_read_command()
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  @doc """
  Parses a multi-record batch read reply into per-record results keyed by input index.
  """
  @spec parse_response(binary(), NodeRequest.t(), keyword()) ::
          {:ok, Reply.t()} | {:error, Error.t()}
  def parse_response(body, %NodeRequest{} = node_request, opts \\ []) when is_binary(body) do
    mode = Keyword.get(opts, :mode, :all_bins)

    with {:ok, records} <-
           decode_rows(body, node_request, mode, allowed_entries(node_request), []) do
      {:ok, %Reply{records: Enum.reverse(records)}}
    end
  end

  defp batch_field_data(%NodeRequest{entries: entries}, mode) do
    count = length(entries)
    flags = 0

    [<<count::32-big, flags::8>> | encode_entries(entries, mode, nil)]
    |> IO.iodata_to_binary()
  end

  defp encode_entries([], _mode, _prev_key), do: []

  defp encode_entries([entry | rest], mode, nil) do
    [encode_entry(entry, mode, nil) | encode_entries(rest, mode, entry.key)]
  end

  defp encode_entries([entry | rest], mode, %Key{} = prev_key) do
    [encode_entry(entry, mode, prev_key) | encode_entries(rest, mode, entry.key)]
  end

  defp encode_entry(%{index: index, key: %Key{} = key}, mode, nil) do
    [<<index::32-big, key.digest::binary, @batch_row_read::8>>, row_body(key, mode)]
  end

  defp encode_entry(%{index: index, key: %Key{} = key}, mode, %Key{} = prev_key) do
    if repeat_key_scope?(prev_key, key) do
      <<index::32-big, key.digest::binary, @batch_row_repeat::8>>
    else
      [<<index::32-big, key.digest::binary, @batch_row_read::8>>, row_body(key, mode)]
    end
  end

  defp row_body(%Key{} = key, :all_bins) do
    [
      <<batch_read_attr(:all_bins)::8, 2::16-big, 0::16-big>>,
      Field.encode(Field.namespace(key.namespace)),
      Field.encode(Field.set(key.set))
    ]
  end

  defp row_body(%Key{} = key, :header) do
    [
      <<batch_read_attr(:header)::8, 2::16-big, 0::16-big>>,
      Field.encode(Field.namespace(key.namespace)),
      Field.encode(Field.set(key.set))
    ]
  end

  defp batch_read_attr(:all_bins) do
    AsmMsg.info1_read() ||| AsmMsg.info1_get_all()
  end

  defp batch_read_attr(:header) do
    AsmMsg.info1_read() ||| AsmMsg.info1_nobindata()
  end

  defp repeat_key_scope?(%Key{} = left, %Key{} = right) do
    left.namespace == right.namespace and left.set == right.set
  end

  defp allowed_entries(%NodeRequest{entries: entries}) do
    Map.new(entries, fn %{index: index, key: key} -> {index, key} end)
  end

  defp decode_rows(<<>>, _node_request, _mode, _allowed, _acc) do
    {:error,
     Error.from_result_code(:parse_error,
       message: "batch read reply ended without a terminal marker"
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
         %NodeRequest{} = node_request,
         mode,
         allowed,
         acc
       ) do
    if (info3 &&& AsmMsg.info3_last()) == AsmMsg.info3_last() do
      decode_last_marker(rest, result_code, field_count, op_count, acc)
    else
      with {:ok, fields_rest} <- skip_fields(rest, field_count),
           {:ok, record, remaining} <-
             decode_record_result(
               fields_rest,
               op_count,
               result_code,
               batch_index,
               generation,
               expiration,
               allowed,
               mode
             ) do
        decode_rows(remaining, node_request, mode, allowed, [record | acc])
      end
    end
  end

  defp decode_rows(<<header_size::8, _rest::binary>>, _node_request, _mode, _allowed, _acc) do
    {:error,
     Error.from_result_code(:parse_error,
       message: "expected batch read row header size 22, got #{header_size}"
     )}
  end

  defp decode_rows(_other, _node_request, _mode, _allowed, _acc) do
    {:error, Error.from_result_code(:parse_error, message: "incomplete batch read row header")}
  end

  defp decode_last_marker(<<>>, 0, 0, 0, acc), do: {:ok, acc}

  defp decode_last_marker(_rest, result_code, _field_count, _op_count, _acc)
       when result_code != 0 do
    {:error, result_error(result_code, "batch read terminal marker returned an error")}
  end

  defp decode_last_marker(_rest, _result_code, field_count, op_count, _acc) do
    {:error,
     Error.from_result_code(:parse_error,
       message:
         "batch read terminal marker expected field_count=0 and op_count=0, got #{field_count}/#{op_count}"
     )}
  end

  defp decode_record_result(
         row_body,
         op_count,
         result_code,
         batch_index,
         generation,
         expiration,
         allowed,
         mode
       ) do
    case Map.fetch(allowed, batch_index) do
      {:ok, %Key{} = key} ->
        decode_record_body(
          row_body,
          op_count,
          result_code,
          batch_index,
          generation,
          expiration,
          key,
          mode
        )

      :error ->
        {:error,
         Error.from_result_code(:parse_error,
           message: "batch read reply referenced unknown batch index #{batch_index}"
         )}
    end
  end

  defp decode_record_body(
         row_body,
         op_count,
         result_code,
         batch_index,
         generation,
         expiration,
         %Key{} = key,
         mode
       ) do
    with {:ok, operations, remaining} <- decode_operations(row_body, op_count),
         {:ok, result} <-
           build_record_result(
             batch_index,
             key,
             result_code,
             generation,
             expiration,
             operations,
             mode
           ) do
      {:ok, result, remaining}
    end
  end

  defp build_record_result(
         batch_index,
         %Key{} = key,
         0,
         generation,
         expiration,
         operations,
         :all_bins
       ) do
    with {:ok, bins} <- decode_bins(operations) do
      {:ok,
       %RecordResult{
         index: batch_index,
         key: key,
         result: :ok,
         generation: generation,
         ttl: expiration,
         bins: bins
       }}
    end
  end

  defp build_record_result(batch_index, %Key{} = key, 0, generation, expiration, [], :header) do
    {:ok,
     %RecordResult{
       index: batch_index,
       key: key,
       result: :ok,
       generation: generation,
       ttl: expiration,
       bins: nil
     }}
  end

  defp build_record_result(_batch_index, _key, 0, _generation, _expiration, operations, :header) do
    {:error,
     Error.from_result_code(:parse_error,
       message: "header-only batch read reply contained #{length(operations)} operations"
     )}
  end

  defp build_record_result(
         batch_index,
         %Key{} = key,
         result_code,
         _generation,
         _expiration,
         [],
         _mode
       ) do
    {:ok,
     %RecordResult{
       index: batch_index,
       key: key,
       result: {:error, result_error(result_code, "batch read row returned an error")},
       generation: nil,
       ttl: nil,
       bins: nil
     }}
  end

  defp build_record_result(
         _batch_index,
         _key,
         result_code,
         _generation,
         _expiration,
         operations,
         _mode
       ) do
    {:error,
     Error.from_result_code(:parse_error,
       message:
         "batch read row with result code #{result_code} contained #{length(operations)} operations"
     )}
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
    Error.from_result_code(:parse_error, message: "failed to parse batch read fields: #{reason}")
  end

  defp parse_operation_error(reason) do
    Error.from_result_code(:parse_error,
      message: "failed to parse batch read operations: #{reason}"
    )
  end
end
