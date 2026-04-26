Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.Micro.ProtocolMessage do
  @moduledoc false

  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response

  @namespace "bench"
  @set "protocol"
  @bin_counts [1, 4, 16]
  @iterations 256

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config, %{iterations_per_sample: @iterations})

    Runtime.print_metadata(metadata, %{workload: :protocol_message_micro})

    Benchee.run(
      jobs(),
      Aerospike.Bench.benchee_options(
        config,
        title: "L1 protocol message baseline",
        inputs: inputs(config),
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp jobs do
    %{
      "PM-001 Value Encode x#{@iterations}" => fn %{payloads: payloads} ->
        repeat(payloads, fn payload ->
          {:ok, {_particle_type, _data}} = Value.encode_value(payload)
        end)
      end,
      "PM-002 Operation Build x#{@iterations}" => fn %{bin_batches: bin_batches} ->
        repeat(bin_batches, fn bins ->
          Enum.map(bins, fn {bin_name, value} ->
            {:ok, operation} = Operation.write(bin_name, value)
            operation
          end)
        end)
      end,
      "PM-003 AS_MSG Encode x#{@iterations}" => fn %{request_msgs: request_msgs} ->
        repeat(request_msgs, fn request_msg ->
          request_msg
          |> AsmMsg.encode()
          |> Message.encode_as_msg_iodata()
        end)
      end,
      "PM-004 AS_MSG Decode x#{@iterations}" => fn %{response_bodies: response_bodies} ->
        repeat(response_bodies, fn response_body ->
          {:ok, %AsmMsg{}} = AsmMsg.decode(response_body)
        end)
      end,
      "PM-005 Record Parse x#{@iterations}" => fn %{record_parse_inputs: record_parse_inputs} ->
        repeat(record_parse_inputs, fn {key, response_msg} ->
          {:ok, _record} = Response.parse_record_response(response_msg, key)
        end)
      end
    }
  end

  defp inputs(config) do
    for payload_size <- Map.fetch!(config, :payload_sizes),
        bin_count <- @bin_counts,
        into: %{} do
      {"#{bin_count} bins/#{payload_size}B payload", build_input(bin_count, payload_size)}
    end
  end

  defp build_input(bin_count, payload_size) do
    payload = String.duplicate("x", payload_size)
    bins = bins(bin_count, payload)
    operations = write_operations(bins)

    keys = Enum.map(1..@iterations, &key(bin_count, payload_size, &1))
    request_msgs = Enum.map(keys, &request_msg(&1, operations))
    response_msgs = Enum.map(1..@iterations, fn _ -> response_msg(operations) end)
    response_bodies = Enum.map(response_msgs, &response_body/1)

    %{
      bin_batches: List.duplicate(bins, @iterations),
      payloads: List.duplicate(payload, @iterations),
      record_parse_inputs: Enum.zip(keys, response_msgs),
      request_msgs: request_msgs,
      response_bodies: response_bodies
    }
  end

  defp key(bin_count, payload_size, index) do
    Key.new(@namespace, @set, "protocol:#{bin_count}:#{payload_size}:#{index}")
  end

  defp request_msg(key, operations) do
    AsmMsg.key_command(key, operations,
      write: true,
      send_key: true,
      ttl: 0,
      generation: 0
    )
  end

  defp response_msg(operations) do
    %AsmMsg{result_code: 0, generation: 1, expiration: 0, operations: operations}
  end

  defp response_body(response_msg) do
    response_msg
    |> AsmMsg.encode()
    |> IO.iodata_to_binary()
  end

  defp bins(bin_count, payload) do
    Map.new(1..bin_count, fn index ->
      {"bin#{index}", payload}
    end)
  end

  defp write_operations(bins) do
    Enum.map(bins, fn {bin_name, value} ->
      {:ok, operation} = Operation.write(bin_name, value)
      operation
    end)
  end

  defp repeat(inputs, fun) do
    Enum.reduce(inputs, nil, fn input, _acc -> fun.(input) end)
  end
end

Aerospike.Bench.Micro.ProtocolMessage.run()
