defmodule Aerospike.UnaryCommand do
  @moduledoc """
  Internal contract for unary command-local hooks.

  Phase 4's shared executor seam is narrower than `Aerospike.Get`'s
  current end-to-end control flow suggests. The retry driver, routing,
  node-handle resolution, breaker checks, pool checkout, retry budget,
  and retry classification stay shared concerns. The command-specific
  variation exposed by the current spike is limited to:

    * choosing read or write dispatch
    * building the wire request from command input
    * parsing the reply body into the command result/error surface

  This module codifies that boundary without extracting the shared retry
  loop yet. `Aerospike.Get` can delegate its transport edge here today,
  and later Phase 4 tasks can move the surrounding retry/dispatch
  orchestration into a reusable executor without re-deciding what a
  unary command is allowed to own.
  """

  alias Aerospike.Error
  alias Aerospike.RetryPolicy

  @enforce_keys [:name, :dispatch, :build_request, :parse_response]
  defstruct [:name, :dispatch, :build_request, :parse_response]

  @type hook_input :: term()
  @type dispatch_kind :: :read | :write
  @type command_result :: {:ok, term()} | {:error, Error.t()}
  @type build_request_fun :: (hook_input() -> iodata())
  @type parse_response_fun :: (body :: binary(), hook_input() -> command_result() | Error.t())

  @type t :: %__MODULE__{
          name: module(),
          dispatch: dispatch_kind(),
          build_request: build_request_fun(),
          parse_response: parse_response_fun()
        }

  @doc """
  Builds a unary command contract.
  """
  @spec new!(keyword()) :: t()
  def new!(opts) when is_list(opts) do
    dispatch = Keyword.get(opts, :dispatch, :read)

    %__MODULE__{
      name: Keyword.fetch!(opts, :name),
      dispatch: validate_dispatch!(dispatch),
      build_request: Keyword.fetch!(opts, :build_request),
      parse_response: Keyword.fetch!(opts, :parse_response)
    }
  end

  @doc """
  Returns whether the command routes as a read or a write.
  """
  @spec dispatch_kind(t()) :: dispatch_kind()
  def dispatch_kind(%__MODULE__{dispatch: dispatch}), do: dispatch

  @doc """
  Runs the transport-facing edge for a unary command.

  The shared executor will own checkout/retry orchestration. This helper
  only applies the command-local hooks and derives the pool checkin
  value from the canonical `Aerospike.RetryPolicy.classify/1` result.
  """
  @spec run_transport(
          t(),
          module(),
          conn :: term(),
          hook_input(),
          deadline_ms :: non_neg_integer(),
          command_opts :: keyword()
        ) :: {command_result(), Aerospike.NodePool.checkin_value()}
  def run_transport(
        %__MODULE__{build_request: build_request, parse_response: parse_response},
        transport,
        conn,
        input,
        deadline_ms,
        command_opts
      )
      when is_atom(transport) and is_function(build_request, 1) and is_function(parse_response, 2) do
    request = build_request.(input)

    case transport.command(conn, request, deadline_ms, command_opts) do
      {:ok, body} ->
        result = normalize_result(parse_response.(body, input))
        {result, checkin_value(result, conn)}

      {:error, %Error{}} = err ->
        {err, checkin_value(err, conn)}
    end
  end

  defp normalize_result({:ok, _} = ok), do: ok
  defp normalize_result({:error, %Error{}} = err), do: err
  defp normalize_result(%Error{} = err), do: {:error, err}

  defp validate_dispatch!(dispatch) when dispatch in [:read, :write], do: dispatch

  defp validate_dispatch!(dispatch) do
    raise ArgumentError,
          "expected unary command dispatch to be :read or :write, got: #{inspect(dispatch)}"
  end

  defp checkin_value(result, conn) do
    case RetryPolicy.classify(result) do
      %{close_connection?: true, node_failure?: true} -> {:close, :failure}
      %{close_connection?: true} -> :close
      _ -> conn
    end
  end
end
