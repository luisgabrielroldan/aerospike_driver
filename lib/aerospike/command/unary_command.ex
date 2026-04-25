defmodule Aerospike.Command.UnaryCommand do
  @moduledoc """
  Internal contract for unary command-local hooks.

  The shared executor boundary is narrower than `Aerospike.Command.Get`'s
  current end-to-end control flow suggests. The retry driver, routing,
  node-handle resolution, breaker checks, pool checkout, retry budget, and
  retry classification stay shared concerns. The command-specific variation
  exposed by the current implementation is limited to:

    * choosing read or write dispatch
    * building the wire request from command input
    * parsing the reply body into the command result/error surface

  This module codifies that boundary without extracting the shared retry
  loop yet. `Aerospike.Command.Get` can delegate its transport edge here today,
  and future executor work can move the surrounding retry/dispatch
  orchestration into a reusable executor without re-deciding what a unary
  command is allowed to own.
  """

  alias Aerospike.Cluster.Router
  alias Aerospike.Error
  alias Aerospike.RetryPolicy
  alias Aerospike.Runtime.Executor

  @enforce_keys [:name, :dispatch, :build_request, :parse_response]
  defstruct [:name, :dispatch, :build_request, :parse_response, retry_transport?: true]

  @type hook_input :: term()
  @type dispatch_kind :: :read | :write
  @type command_result :: {:ok, term()} | {:error, Error.t()}
  @type transport_result :: command_result() | {:no_retry, command_result()}
  @type pick_node_fun ::
          (dispatch :: dispatch_kind(),
           tables :: term(),
           route_key :: term(),
           RetryPolicy.replica_policy(),
           non_neg_integer() ->
             {:ok, String.t()} | {:error, term()})
  @type resolve_handle_fun :: Executor.resolve_handle_fun()
  @type allow_dispatch_fun :: Executor.allow_dispatch_fun()
  @type checkout_fun :: Executor.checkout_fun()
  @type build_request_fun :: (hook_input() -> iodata())
  @type parse_response_fun :: (body :: binary(), hook_input() -> command_result() | Error.t())
  @type dispatch_ctx :: %{
          required(:tables) => term(),
          required(:tender) => GenServer.server(),
          required(:transport) => module(),
          required(:route_key) => term(),
          required(:command_input) => hook_input(),
          optional(:pick_node) => pick_node_fun(),
          optional(:resolve_handle) => resolve_handle_fun(),
          optional(:allow_dispatch) => allow_dispatch_fun(),
          optional(:checkout) => checkout_fun()
        }

  @type t :: %__MODULE__{
          name: module(),
          dispatch: dispatch_kind(),
          build_request: build_request_fun(),
          parse_response: parse_response_fun(),
          retry_transport?: boolean()
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
      parse_response: Keyword.fetch!(opts, :parse_response),
      retry_transport?: Keyword.get(opts, :retry_transport, true)
    }
  end

  @doc """
  Returns whether the command routes as a read or a write.
  """
  @spec dispatch_kind(t()) :: dispatch_kind()
  def dispatch_kind(%__MODULE__{dispatch: dispatch}), do: dispatch

  @doc """
  Runs one unary command through the shared executor runtime.
  """
  @spec run(t(), Executor.t(), dispatch_ctx()) :: command_result() | {:error, term()}
  def run(%__MODULE__{} = command, %Executor{} = executor, ctx) when is_map(ctx) do
    callbacks = %{
      route_unit: fn :unary, attempt ->
        pick_node = Map.get(ctx, :pick_node, &pick_node/5)

        pick_node.(
          dispatch_kind(command),
          ctx.tables,
          ctx.route_key,
          executor.policy.retry.replica_policy,
          attempt
        )
      end,
      run_transport: fn :unary, _node_name, transport, conn, remaining, command_opts ->
        run_transport(
          command,
          transport,
          conn,
          ctx.command_input,
          remaining,
          command_opts
        )
      end,
      progress_retry: fn _kind, :unary, _next_attempt, _last_result ->
        {:ok, Executor.progress([:unary])}
      end
    }

    case Executor.run_unit(executor, :unary, ctx, callbacks) do
      [%Executor.Outcome{result: result}] -> normalize_public_result(result)
    end
  end

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
        ) :: {transport_result(), Aerospike.Cluster.NodePool.checkin_value()}
  def run_transport(
        %__MODULE__{} = command,
        transport,
        conn,
        input,
        deadline_ms,
        command_opts
      )
      when is_atom(transport) do
    %__MODULE__{build_request: build_request, parse_response: parse_response} = command
    request = build_request.(input)

    case transport.command(conn, request, deadline_ms, command_opts) do
      {:ok, body} ->
        result = normalize_result(parse_response.(body, input))
        {result, checkin_value(result, conn)}

      {:error, %Error{} = error} ->
        result = transport_error_result(error, retry_transport?: retry_transport?(command))
        {result, checkin_value(error, conn)}
    end
  end

  defp retry_transport?(%__MODULE__{retry_transport?: retry_transport?}), do: retry_transport?

  defp normalize_result({:ok, _} = ok), do: ok
  defp normalize_result({:error, %Error{}} = err), do: err
  defp normalize_result(%Error{} = err), do: {:error, err}
  defp normalize_public_result({:ok, _} = ok), do: ok
  defp normalize_public_result({:error, %Error{}} = err), do: err
  defp normalize_public_result({:error, reason}) when is_atom(reason), do: {:error, reason}
  defp normalize_public_result(reason) when is_atom(reason), do: {:error, reason}

  defp validate_dispatch!(dispatch) when dispatch in [:read, :write], do: dispatch

  defp validate_dispatch!(dispatch) do
    raise ArgumentError,
          "expected unary command dispatch to be :read or :write, got: #{inspect(dispatch)}"
  end

  defp transport_error_result(%Error{} = err, retry_transport?: true), do: {:error, err}

  defp transport_error_result(%Error{} = err, retry_transport?: false),
    do: {:no_retry, {:error, err}}

  defp pick_node(:read, tables, route_key, replica_policy, attempt) do
    Router.pick_for_read(tables, route_key, replica_policy, attempt)
  end

  defp pick_node(:write, tables, route_key, _replica_policy, _attempt) do
    Router.pick_for_write(tables, route_key)
  end

  defp checkin_value(result, conn) do
    case RetryPolicy.classify(result) do
      %{close_connection?: true, node_failure?: true} -> {:close, :failure}
      %{close_connection?: true} -> :close
      _ -> conn
    end
  end
end
