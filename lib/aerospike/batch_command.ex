defmodule Aerospike.BatchCommand do
  @moduledoc """
  Internal contract for grouped batch command hooks.

  Batch execution owns node-level dispatch, retry, and bounded fan-out.
  Command modules supply only the per-node request builder, reply parser,
  and final merge step back into caller-facing order.
  """

  alias Aerospike.Error
  alias Aerospike.RetryPolicy

  defmodule Entry do
    @moduledoc false

    @enforce_keys [:index]
    defstruct [:index, :key, :payload]

    @type t :: %__MODULE__{
            index: non_neg_integer(),
            key: term(),
            payload: term()
          }
  end

  defmodule NodeRequest do
    @moduledoc false

    @enforce_keys [:node_name, :entries]
    defstruct [:node_name, :entries, :payload]

    @type t :: %__MODULE__{
            node_name: String.t(),
            entries: [Entry.t()],
            payload: term()
          }
  end

  defmodule NodeResult do
    @moduledoc false

    @enforce_keys [:request, :result, :attempt]
    defstruct [:request, :result, :attempt]

    @type t :: %__MODULE__{
            request: NodeRequest.t(),
            result: Aerospike.BatchCommand.command_result(),
            attempt: non_neg_integer()
          }
  end

  @enforce_keys [:name, :build_request, :parse_response, :merge_results, :transport_mode]
  defstruct [:name, :build_request, :parse_response, :merge_results, :transport_mode]

  @type batch_input :: term()
  @type command_result :: {:ok, term()} | {:error, Error.t()} | {:error, atom()}
  @type build_request_fun :: (NodeRequest.t() -> iodata())
  @type parse_response_fun :: (body :: binary(), NodeRequest.t() -> command_result() | Error.t())
  @type merge_results_fun :: ([NodeResult.t()], batch_input() -> term())
  @type transport_mode :: :command | :command_stream

  @type t :: %__MODULE__{
          name: module(),
          build_request: build_request_fun(),
          parse_response: parse_response_fun(),
          merge_results: merge_results_fun(),
          transport_mode: transport_mode()
        }

  @doc """
  Builds a batch command contract.
  """
  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    %__MODULE__{
      name: Keyword.fetch!(opts, :name),
      build_request: Keyword.fetch!(opts, :build_request),
      parse_response: Keyword.fetch!(opts, :parse_response),
      merge_results: Keyword.fetch!(opts, :merge_results),
      transport_mode: Keyword.get(opts, :transport_mode, :command)
    }
  end

  @doc """
  Applies the command's final merge step to per-node outcomes.
  """
  @spec merge_results(t(), [NodeResult.t()], batch_input()) :: term()
  def merge_results(%__MODULE__{merge_results: merge_results}, node_results, batch_input)
      when is_function(merge_results, 2) and is_list(node_results) do
    merge_results.(node_results, batch_input)
  end

  @doc """
  Runs the transport-facing edge for one grouped node request.
  """
  @spec run_transport(
          t(),
          module(),
          conn :: term(),
          NodeRequest.t(),
          deadline_ms :: non_neg_integer(),
          command_opts :: keyword()
        ) :: {command_result(), Aerospike.NodePool.checkin_value()}
  def run_transport(
        %__MODULE__{
          build_request: build_request,
          parse_response: parse_response,
          transport_mode: transport_mode
        },
        transport,
        conn,
        %NodeRequest{} = node_request,
        deadline_ms,
        command_opts
      )
      when is_atom(transport) and is_function(build_request, 1) and is_function(parse_response, 2) do
    request = build_request.(node_request)

    case run_transport_request(
           transport_mode,
           transport,
           conn,
           request,
           deadline_ms,
           command_opts
         ) do
      {:ok, body} ->
        result = normalize_result(parse_response.(body, node_request))
        {result, checkin_value(result, conn)}

      {:error, %Error{}} = err ->
        {err, checkin_value(err, conn)}
    end
  end

  defp normalize_result({:ok, _} = ok), do: ok
  defp normalize_result({:error, %Error{}} = err), do: err
  defp normalize_result({:error, reason}) when is_atom(reason), do: {:error, reason}
  defp normalize_result(%Error{} = err), do: {:error, err}

  defp run_transport_request(:command, transport, conn, request, deadline_ms, command_opts) do
    transport.command(conn, request, deadline_ms, command_opts)
  end

  defp run_transport_request(:command_stream, transport, conn, request, deadline_ms, command_opts) do
    transport.command_stream(conn, request, deadline_ms, command_opts)
  end

  defp checkin_value(result, conn) do
    case RetryPolicy.classify(result) do
      %{close_connection?: true, node_failure?: true} -> {:close, :failure}
      %{close_connection?: true} -> :close
      _ -> conn
    end
  end
end
