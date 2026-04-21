defmodule Aerospike.UnarySupport do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.Response
  alias Aerospike.RetryPolicy
  alias Aerospike.Tender
  alias Aerospike.UnaryCommand
  alias Aerospike.UnaryExecutor

  @spec run_command(
          GenServer.server(),
          Key.t(),
          keyword(),
          UnaryCommand.t(),
          UnaryCommand.hook_input()
        ) :: term()
  def run_command(tender, %Key{} = route_key, opts, %UnaryCommand{} = command, command_input)
      when is_list(opts) do
    runtime = runtime_ctx(tender)

    UnaryExecutor.run_command(executor(runtime, opts), command, %{
      tender: runtime.tender,
      tables: runtime.tables,
      transport: runtime.transport,
      route_key: route_key,
      command_input: command_input
    })
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
      tables: Tender.tables(tender),
      transport: Tender.transport(tender)
    }
  end

  defp executor(runtime, opts) do
    UnaryExecutor.new!(
      base_policy: RetryPolicy.load(runtime.tables.meta),
      command_opts: opts,
      on_rebalance: fn -> trigger_tend_async(runtime.tender) end
    )
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
