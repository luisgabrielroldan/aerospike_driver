Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.Micro.BatchProtocol do
  @moduledoc false

  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Command.BatchCommand.NodeRequest
  alias Aerospike.Key
  alias Aerospike.Protocol.Batch
  alias Aerospike.Protocol.BatchRead

  @namespace "bench"
  @set "batch_protocol"
  @batch_sizes [100, 1_000]
  @iterations 64

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config, %{iterations_per_sample: @iterations})

    Runtime.print_metadata(metadata, %{workload: :batch_protocol_micro})

    Benchee.run(
      jobs(),
      Aerospike.Bench.benchee_options(
        config,
        title: "L1 batch protocol baseline",
        inputs: inputs(),
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp jobs do
    %{
      "BP-001 Encode batch_index x#{@iterations}" => fn %{node_requests: node_requests} ->
        repeat(node_requests, fn node_request ->
          Batch.encode_request(node_request, layout: :batch_index, timeout: 0)
        end)
      end,
      "BP-002 Encode batch_index_with_set x#{@iterations}" => fn %{node_requests: node_requests} ->
        repeat(node_requests, fn node_request ->
          BatchRead.encode_request(node_request, timeout: 0)
        end)
      end
    }
  end

  defp inputs do
    Map.new(@batch_sizes, fn batch_size ->
      {"#{batch_size} keys", build_input(batch_size)}
    end)
  end

  defp build_input(batch_size) do
    %{
      node_requests:
        Enum.map(1..@iterations, fn iteration ->
          %NodeRequest{
            node_name: "bench-node",
            entries: entries(batch_size, iteration),
            payload: nil
          }
        end)
    }
  end

  defp entries(batch_size, iteration) do
    Enum.map(1..batch_size, fn index ->
      %Entry{
        index: index - 1,
        key: Key.new(@namespace, @set, "bp:#{batch_size}:#{iteration}:#{index}"),
        kind: :read,
        dispatch: {:read, :master, 0},
        payload: nil
      }
    end)
  end

  defp repeat(inputs, fun) do
    Enum.reduce(inputs, nil, fn input, _acc -> fun.(input) end)
  end
end

Aerospike.Bench.Micro.BatchProtocol.run()
