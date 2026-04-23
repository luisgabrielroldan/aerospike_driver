defmodule Aerospike.Command.StreamingCommandTest do
  use ExUnit.Case, async: true

  alias Aerospike.Command.StreamingCommand
  alias Aerospike.Error

  test "exposes the configured hook set" do
    command =
      StreamingCommand.new!(
        name: __MODULE__,
        build_request: fn %{request: request} -> ["wire:", request] end,
        init: fn _input -> [:seed] end,
        consume_frame: fn body, %{suffix: suffix}, acc -> {:cont, [body <> suffix | acc]} end,
        finish: fn acc, _input -> {:ok, Enum.reverse(acc)} end,
        error_result: fn err, _input -> {:error, err} end
      )

    input = %{request: "scan", suffix: "-done"}

    assert IO.iodata_to_binary(StreamingCommand.build_request(command, input)) == "wire:scan"
    assert StreamingCommand.init(command, input) == [:seed]

    assert {:cont, ["frame-done", :seed]} =
             StreamingCommand.consume_frame(command, "frame", input, [:seed])

    assert {:ok, ["frame-done", :seed]} =
             StreamingCommand.finish(command, [:seed, "frame-done"], input)

    err = Error.from_result_code(:network_error)
    assert {:error, ^err} = StreamingCommand.error_result(command, err, input)
  end
end
