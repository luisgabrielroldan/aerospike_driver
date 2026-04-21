defmodule Aerospike.StreamCommand do
  @moduledoc """
  Internal contract for long-lived and bounded streaming jobs.

  The contract keeps request shaping, chunk parsing, and final result
  aggregation separate from transport callbacks and partition ownership.
  """

  alias Aerospike.Error

  defmodule Input do
    @moduledoc false

    @enforce_keys [:scannable]
    defstruct [:scannable, :cursor, :node_name, :opts]

    @type t :: %__MODULE__{
            scannable: term(),
            cursor: term() | nil,
            node_name: String.t() | nil,
            opts: keyword() | nil
          }
  end

  defmodule NodeRequest do
    @moduledoc false

    @enforce_keys [:node_name, :input]
    defstruct [:node_name, :input, :payload]

    @type t :: %__MODULE__{
            node_name: String.t(),
            input: Input.t(),
            payload: term()
          }
  end

  defmodule NodeChunk do
    @moduledoc false

    @enforce_keys [:records]
    defstruct [:records, :partition_done, done?: false]

    @type t :: %__MODULE__{
            records: [term()],
            partition_done: [term()] | nil,
            done?: boolean()
          }
  end

  defmodule NodeResult do
    @moduledoc false

    @enforce_keys [:request, :result, :attempt]
    defstruct [:request, :result, :attempt]

    @type t :: %__MODULE__{
            request: NodeRequest.t(),
            result: Aerospike.StreamCommand.command_result(),
            attempt: non_neg_integer()
          }
  end

  @enforce_keys [:name, :build_request, :parse_chunk, :merge_results, :transport_mode]
  defstruct [:name, :build_request, :parse_chunk, :merge_results, :transport_mode]

  @type input :: Input.t()
  @type command_result :: {:ok, term()} | {:error, Error.t()} | {:error, atom()}
  @type build_request_fun :: (NodeRequest.t() -> iodata())
  @type parse_chunk_fun :: (chunk :: binary(), NodeRequest.t() -> command_result() | Error.t())
  @type merge_results_fun :: ([NodeResult.t()], input() -> term())
  @type transport_mode :: :stream | :command_stream

  @type t :: %__MODULE__{
          name: module(),
          build_request: build_request_fun(),
          parse_chunk: parse_chunk_fun(),
          merge_results: merge_results_fun(),
          transport_mode: transport_mode()
        }

  @doc """
  Builds a streaming contract.
  """
  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    %__MODULE__{
      name: Keyword.fetch!(opts, :name),
      build_request: Keyword.fetch!(opts, :build_request),
      parse_chunk: Keyword.fetch!(opts, :parse_chunk),
      merge_results: Keyword.fetch!(opts, :merge_results),
      transport_mode: validate_transport_mode!(Keyword.get(opts, :transport_mode, :stream))
    }
  end

  @doc """
  Applies the contract's final merge step.
  """
  @spec merge_results(t(), [NodeResult.t()], input()) :: term()
  def merge_results(%__MODULE__{merge_results: merge_results}, node_results, input)
      when is_function(merge_results, 2) and is_list(node_results) do
    merge_results.(node_results, input)
  end

  defp validate_transport_mode!(mode) when mode in [:stream, :command_stream], do: mode

  defp validate_transport_mode!(mode) do
    raise ArgumentError,
          "expected streaming transport mode to be :stream or :command_stream, got: #{inspect(mode)}"
  end
end
