defmodule Aerospike.Command.UnaryCommandTest do
  use ExUnit.Case, async: true

  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Error

  defmodule TestTransport do
    def command(_conn, _request, _deadline_ms, _command_opts) do
      Process.get(:transport_result)
    end
  end

  test "success keeps the checked-out worker" do
    Process.put(:transport_result, {:ok, "wire-reply"})

    command =
      UnaryCommand.new!(
        name: __MODULE__,
        build_request: fn key -> "get:" <> key end,
        parse_response: fn "wire-reply", key -> {:ok, %{key: key}} end
      )

    assert {{:ok, %{key: "k1"}}, :conn} =
             UnaryCommand.run_transport(command, TestTransport, :conn, "k1", 50, attempt: 0)
  end

  test "transport node-failure closes the worker with failure accounting" do
    Process.put(:transport_result, {:error, Error.from_result_code(:network_error)})

    command =
      UnaryCommand.new!(
        name: __MODULE__,
        build_request: fn _key -> "request" end,
        parse_response: fn _body, _key -> {:ok, :unused} end
      )

    assert {{:error, %Error{code: :network_error}}, {:close, :failure}} =
             UnaryCommand.run_transport(command, TestTransport, :conn, "k1", 50, [])
  end

  test "parse errors close the worker without node-failure accounting" do
    Process.put(:transport_result, {:ok, "bad-wire"})

    command =
      UnaryCommand.new!(
        name: __MODULE__,
        build_request: fn _key -> "request" end,
        parse_response: fn "bad-wire", _key ->
          {:error, %Error{code: :parse_error, message: "bad reply"}}
        end
      )

    assert {{:error, %Error{code: :parse_error}}, :close} =
             UnaryCommand.run_transport(command, TestTransport, :conn, "k1", 50, [])
  end

  test "defaults dispatch to :read and allows explicit :write commands" do
    read_command =
      UnaryCommand.new!(
        name: __MODULE__,
        build_request: fn _key -> "request" end,
        parse_response: fn _body, _key -> {:ok, :unused} end
      )

    write_command =
      UnaryCommand.new!(
        name: __MODULE__,
        dispatch: :write,
        build_request: fn _key -> "request" end,
        parse_response: fn _body, _key -> {:ok, :unused} end
      )

    assert UnaryCommand.dispatch_kind(read_command) == :read
    assert UnaryCommand.dispatch_kind(write_command) == :write
  end
end
