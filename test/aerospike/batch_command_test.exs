defmodule Aerospike.Command.BatchCommandTest do
  use ExUnit.Case, async: true

  alias Aerospike.BatchResult
  alias Aerospike.Command.BatchCommand
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Command.BatchCommand.NodeRequest
  alias Aerospike.Command.BatchCommand.Result
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Record

  describe "Result" do
    test "captures stable per-entry outcome metadata for mixed batches" do
      key = Key.new("test", "users", "user-1")

      assert %Result{
               index: 4,
               key: ^key,
               kind: :delete,
               status: :error,
               record: nil,
               error: :unknown_node,
               in_doubt: true
             } = %Result{
               index: 4,
               key: key,
               kind: :delete,
               status: :error,
               record: nil,
               error: :unknown_node,
               in_doubt: true
             }
    end
  end

  describe "BatchResult.from_command_result/1" do
    test "preserves successful records" do
      key = Key.new("test", "users", "user-1")
      record = %Record{key: key, bins: %{"name" => "Ada"}, generation: 4, ttl: 120}

      result =
        BatchResult.from_command_result(%Result{
          index: 0,
          key: key,
          kind: :read,
          status: :ok,
          record: record,
          error: nil,
          in_doubt: false
        })

      assert %BatchResult{
               key: ^key,
               status: :ok,
               record: ^record,
               error: nil,
               in_doubt: false
             } = result
    end

    test "preserves successful write-like outcomes without records" do
      key = Key.new("test", "users", "user-1")

      result =
        BatchResult.from_command_result(%Result{
          index: 0,
          key: key,
          kind: :delete,
          status: :ok,
          record: nil,
          error: nil,
          in_doubt: false
        })

      assert %BatchResult{
               key: ^key,
               status: :ok,
               record: nil,
               error: nil,
               in_doubt: false
             } = result
    end

    test "preserves server errors" do
      key = Key.new("test", "users", "missing")
      error = Error.from_result_code(:key_not_found)

      result =
        BatchResult.from_command_result(%Result{
          index: 0,
          key: key,
          kind: :read,
          status: :error,
          record: nil,
          error: error,
          in_doubt: false
        })

      assert %BatchResult{
               key: ^key,
               status: :error,
               record: nil,
               error: ^error,
               in_doubt: false
             } = result
    end

    test "preserves routing failures, transport errors, parse omissions, and in-doubt metadata" do
      key = Key.new("test", "users", "user-1")
      routing_failure = :no_master
      transport_error = Error.from_result_code(:network_error, message: "socket closed")

      parse_error =
        Error.from_result_code(:parse_error,
          message: "batch reply omitted requested index 1"
        )

      in_doubt_error = Error.from_result_code(:timeout, in_doubt: true)

      for {kind, error, in_doubt} <- [
            {:read, routing_failure, false},
            {:read, transport_error, false},
            {:read, parse_error, false},
            {:delete, in_doubt_error, true}
          ] do
        result =
          BatchResult.from_command_result(%Result{
            index: 1,
            key: key,
            kind: kind,
            status: :error,
            record: nil,
            error: error,
            in_doubt: in_doubt
          })

        assert %BatchResult{
                 key: ^key,
                 status: :error,
                 record: nil,
                 error: ^error,
                 in_doubt: ^in_doubt
               } = result
      end
    end
  end

  describe "run_transport/6" do
    test "builds one request per grouped node request and preserves healthy workers on success" do
      key_a = Key.new("test", "users", "user-a")
      key_c = Key.new("test", "users", "user-c")

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
        entries: [
          %Entry{
            index: 0,
            key: key_a,
            kind: :read,
            dispatch: {:read, :master, 0},
            payload: :first
          },
          %Entry{
            index: 2,
            key: key_c,
            kind: :read,
            dispatch: {:read, :master, 0},
            payload: :third
          }
        ]
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
      key = Key.new("test", "users", "user-a")

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

      node_request = %NodeRequest{
        node_name: "A1",
        entries: [%Entry{index: 0, key: key, kind: :read, dispatch: {:read, :master, 0}}]
      }

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

    test "supports single-frame command transport and normalizes parser error shapes" do
      key = Key.new("test", "users", "user-a")

      command =
        BatchCommand.new!(
          name: __MODULE__,
          build_request: fn _request -> "request" end,
          parse_response: fn
            "error-struct", _request -> Error.from_result_code(:timeout, message: "slow")
            "error-tuple", _request -> {:error, Error.from_result_code(:parse_error)}
            "error-atom", _request -> {:error, :unknown_node}
            "ok", _request -> {:ok, :parsed}
          end,
          merge_results: fn _results, _input -> :ok end
        )

      node_request = %NodeRequest{
        node_name: "A1",
        entries: [%Entry{index: 0, key: key, kind: :read, dispatch: {:read, :master, 0}}]
      }

      assert {{:ok, :parsed}, :conn_a1} =
               BatchCommand.run_transport(
                 command,
                 __MODULE__.CommandTransportStub,
                 :conn_a1,
                 node_request,
                 25,
                 reply: "ok"
               )

      assert {{:error, %Error{code: :timeout}}, {:close, :failure}} =
               BatchCommand.run_transport(
                 command,
                 __MODULE__.CommandTransportStub,
                 :conn_a1,
                 node_request,
                 25,
                 reply: "error-struct"
               )

      assert {{:error, :unknown_node}, :conn_a1} =
               BatchCommand.run_transport(
                 command,
                 __MODULE__.CommandTransportStub,
                 :conn_a1,
                 node_request,
                 25,
                 reply: "error-atom"
               )

      assert {{:error, %Error{code: :parse_error}}, :close} =
               BatchCommand.run_transport(
                 command,
                 __MODULE__.CommandTransportStub,
                 :conn_a1,
                 node_request,
                 25,
                 reply: "error-tuple"
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

  defmodule CommandTransportStub do
    def command(:conn_a1, "request", 25, reply: reply), do: {:ok, reply}
  end
end
