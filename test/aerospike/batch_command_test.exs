defmodule Aerospike.BatchCommandTest do
  use ExUnit.Case, async: true

  alias Aerospike.BatchCommand
  alias Aerospike.BatchCommand.Entry
  alias Aerospike.BatchCommand.NodeRequest
  alias Aerospike.Error

  describe "run_transport/6" do
    test "builds one request per grouped node request and preserves healthy workers on success" do
      command =
        BatchCommand.new!(
          name: __MODULE__,
          transport_mode: :command_stream,
          build_request: fn request ->
            send(
              self(),
              {:build_request, request.node_name, Enum.map(request.entries, & &1.index)}
            )

            "request-for-" <> request.node_name
          end,
          parse_response: fn body, request ->
            send(self(), {:parse_response, request.node_name, body})
            {:ok, {:parsed, request.node_name, body}}
          end,
          merge_results: fn _results, _input -> :ok end
        )

      node_request = %NodeRequest{
        node_name: "A1",
        entries: [%Entry{index: 0, payload: :first}, %Entry{index: 2, payload: :third}]
      }

      assert {{:ok, {:parsed, "A1", "wire-body"}}, :conn_a1} =
               BatchCommand.run_transport(
                 command,
                 __MODULE__.TransportStub,
                 :conn_a1,
                 node_request,
                 25,
                 attempt: 0
               )

      assert_received {:build_request, "A1", [0, 2]}
      assert_received {:parse_response, "A1", "wire-body"}
    end

    test "closes failed workers on transport-class errors" do
      command =
        BatchCommand.new!(
          name: __MODULE__,
          transport_mode: :command_stream,
          build_request: fn _request -> "request" end,
          parse_response: fn _body, _request ->
            flunk("parse should not run on transport error")
          end,
          merge_results: fn _results, _input -> :ok end
        )

      node_request = %NodeRequest{node_name: "A1", entries: [%Entry{index: 0}]}

      assert {{:error, %Error{code: :network_error, message: "boom"}}, {:close, :failure}} =
               BatchCommand.run_transport(
                 command,
                 __MODULE__.TransportErrorStub,
                 :conn_a1,
                 node_request,
                 25,
                 attempt: 0
               )
    end
  end

  defmodule TransportStub do
    def command_stream(:conn_a1, "request-for-A1", 25, attempt: 0), do: {:ok, "wire-body"}
  end

  defmodule TransportErrorStub do
    def command_stream(:conn_a1, "request", 25, attempt: 0) do
      {:error, Error.from_result_code(:network_error, message: "boom")}
    end
  end
end
