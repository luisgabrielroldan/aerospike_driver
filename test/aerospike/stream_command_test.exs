defmodule Aerospike.StreamCommandTest do
  use ExUnit.Case, async: true

  alias Aerospike.StreamCommand
  alias Aerospike.StreamCommand.Input
  alias Aerospike.StreamCommand.NodeChunk
  alias Aerospike.StreamCommand.NodeRequest
  alias Aerospike.StreamCommand.NodeResult

  describe "new!/1" do
    test "builds a contract with explicit transport mode" do
      command =
        StreamCommand.new!(
          name: __MODULE__,
          transport_mode: :stream,
          build_request: fn request ->
            send(self(), {:build_request, request.node_name})
            "request"
          end,
          parse_chunk: fn chunk, request ->
            send(self(), {:parse_chunk, request.node_name, chunk})
            {:ok, %NodeChunk{records: [chunk], done?: true, partition_done: nil}}
          end,
          merge_results: fn results, input ->
            send(self(), {:merge_results, results, input.scannable})
            :merged
          end
        )

      assert command.name == __MODULE__
      assert command.transport_mode == :stream
      assert is_function(command.build_request, 1)
      assert is_function(command.parse_chunk, 2)
      assert is_function(command.merge_results, 2)
    end

    test "rejects unsupported transport modes" do
      assert_raise ArgumentError, fn ->
        StreamCommand.new!(
          name: __MODULE__,
          transport_mode: :invalid,
          build_request: fn _ -> "" end,
          parse_chunk: fn _, _ ->
            {:ok, %NodeChunk{records: [], done?: true, partition_done: nil}}
          end,
          merge_results: fn _, _ -> :ok end
        )
      end
    end
  end

  describe "merge_results/3" do
    test "delegates to the contract callback" do
      command =
        StreamCommand.new!(
          name: __MODULE__,
          build_request: fn _ -> "" end,
          parse_chunk: fn _, _ ->
            {:ok, %NodeChunk{records: [], done?: true, partition_done: nil}}
          end,
          merge_results: fn results, input -> {results, input} end
        )

      request = %NodeRequest{node_name: "A1", input: %Input{scannable: :scan}, payload: "wire"}
      result = %NodeResult{request: request, result: {:ok, [:record]}, attempt: 0}

      assert {[^result], %Input{scannable: :scan}} =
               StreamCommand.merge_results(command, [result], %Input{scannable: :scan})
    end
  end
end
