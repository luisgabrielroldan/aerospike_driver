defmodule Aerospike.StreamingCommand do
  @moduledoc """
  Internal contract for node-scoped streaming command hooks.

  Streaming scan/query work in the spike already shares its bootstrap and
  node preparation path. What still varies per command is narrower:

    * building the node-scoped wire request
    * folding each decoded stream frame into command-local state
    * finishing or shaping command-local results

  This module freezes that boundary so shared node-handle resolution,
  breaker checks, connect/open/read/close orchestration, and stream frame
  decoding can live in one executor.
  """

  alias Aerospike.Error

  @type hook_input :: map()
  @type acc :: term()
  @type command_result :: term()
  @type build_request_fun :: (hook_input() -> iodata())
  @type init_fun :: (hook_input() -> acc())

  @type consume_frame_result ::
          {:cont, acc()}
          | {:halt, command_result()}
          | {:error, Error.t()}

  @type consume_frame_fun :: (body :: binary(), hook_input(), acc() -> consume_frame_result())
  @type finish_fun :: (acc(), hook_input() -> command_result())
  @type error_result_fun :: (Error.t(), hook_input() -> command_result())

  @enforce_keys [:name, :build_request, :init, :consume_frame, :finish, :error_result]
  defstruct [:name, :build_request, :init, :consume_frame, :finish, :error_result]

  @type t :: %__MODULE__{
          name: module(),
          build_request: build_request_fun(),
          init: init_fun(),
          consume_frame: consume_frame_fun(),
          finish: finish_fun(),
          error_result: error_result_fun()
        }

  @doc """
  Builds a streaming command contract.
  """
  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    %__MODULE__{
      name: Keyword.fetch!(opts, :name),
      build_request: Keyword.fetch!(opts, :build_request),
      init: Keyword.fetch!(opts, :init),
      consume_frame: Keyword.fetch!(opts, :consume_frame),
      finish: Keyword.fetch!(opts, :finish),
      error_result: Keyword.fetch!(opts, :error_result)
    }
  end

  @doc """
  Builds the wire request for one node-scoped streaming command call.
  """
  @spec build_request(t(), hook_input()) :: iodata()
  def build_request(%__MODULE__{build_request: build_request}, input), do: build_request.(input)

  @doc """
  Initializes command-local accumulation for one node stream.
  """
  @spec init(t(), hook_input()) :: acc()
  def init(%__MODULE__{init: init}, input), do: init.(input)

  @doc """
  Applies one decoded stream frame body to the command-local accumulator.
  """
  @spec consume_frame(t(), binary(), hook_input(), acc()) :: consume_frame_result()
  def consume_frame(%__MODULE__{consume_frame: consume_frame}, body, input, acc) do
    consume_frame.(body, input, acc)
  end

  @doc """
  Finishes a command when the transport signals the stream is done.
  """
  @spec finish(t(), acc(), hook_input()) :: command_result()
  def finish(%__MODULE__{finish: finish}, acc, input), do: finish.(acc, input)

  @doc """
  Shapes an error result for the command after shared transport work fails.
  """
  @spec error_result(t(), Error.t(), hook_input()) :: command_result()
  def error_result(%__MODULE__{error_result: error_result}, err, input) do
    error_result.(err, input)
  end
end
