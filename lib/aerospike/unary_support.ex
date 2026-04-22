defmodule Aerospike.UnarySupport do
  @moduledoc false

  alias Aerospike.Cluster
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.Response
  alias Aerospike.Tender
  alias Aerospike.UnaryCommand
  alias Aerospike.UnaryExecutor

  @type unary_policy :: Policy.UnaryRead.t() | Policy.UnaryWrite.t()

  @spec run_command(
          GenServer.server(),
          Key.t(),
          unary_policy(),
          UnaryCommand.t(),
          UnaryCommand.hook_input()
        ) :: term()
  def run_command(
        tender,
        %Key{} = route_key,
        %_{} = policy,
        %UnaryCommand{} = command,
        command_input
      ) do
    runtime = runtime_ctx(tender)

    UnaryExecutor.run_command(executor(runtime, policy), command, %{
      tender: runtime.tender,
      tables: runtime.tables,
      transport: runtime.transport,
      route_key: route_key,
      command_input: command_input
    })
  end

  @spec read_policy(GenServer.server(), keyword()) ::
          {:ok, Policy.UnaryRead.t()} | {:error, Error.t()}
  def read_policy(tender, opts) when is_list(opts) do
    tender
    |> base_retry()
    |> Policy.unary_read(opts)
  end

  @spec write_policy(GenServer.server(), keyword()) ::
          {:ok, Policy.UnaryWrite.t()} | {:error, Error.t()}
  def write_policy(tender, opts) when is_list(opts) do
    tender
    |> base_retry()
    |> Policy.unary_write(opts)
  end

  @spec operate_policy(GenServer.server(), keyword()) ::
          {:ok, Policy.UnaryWrite.t()} | {:error, Error.t()}
  def operate_policy(tender, opts) when is_list(opts) do
    tender
    |> base_retry()
    |> Policy.unary_operate(opts)
  end

  @spec parse_as_msg(binary(), (term() -> {:ok, term()} | {:error, Error.t()} | Error.t())) ::
          {:ok, term()} | {:error, Error.t()}
  def parse_as_msg(body, parser) when is_binary(body) and is_function(parser, 1) do
    case Response.decode_as_msg(body) do
      {:ok, msg} ->
        normalize_result(parser.(msg))

      {:error, %Error{}} = err ->
        err
    end
  end

  defp runtime_ctx(tender) do
    %{
      tender: tender,
      tables: Cluster.tables(tender),
      transport: Tender.transport(tender)
    }
  end

  defp executor(runtime, policy) do
    UnaryExecutor.new!(
      policy: policy,
      on_rebalance: fn -> trigger_tend_async(runtime.tender) end
    )
  end

  defp base_retry(tender) do
    Cluster.retry_policy(tender)
  end

  # Rebalance recovery is best-effort and must not take down the caller.
  defp trigger_tend_async(tender) do
    _ =
      spawn(fn ->
        try do
          Tender.tend_now(tender)
        catch
          :exit, _ -> :ok
        end
      end)

    :ok
  end

  defp normalize_result({:ok, _} = ok), do: ok
  defp normalize_result({:error, %Error{}} = err), do: err
  defp normalize_result(%Error{} = err), do: {:error, err}
end
