defmodule Aerospike.Get do
  @moduledoc """
  GET command adapter for the spike.

  `Aerospike.UnaryExecutor` owns the shared unary control flow:
  routing, node-handle resolution, breaker checks, pool checkout,
  transport dispatch, reply classification, and retry budgeting.

  This module stays intentionally narrow. Its responsibilities are:

    * reject spike-unsupported GET shapes such as named-bin requests
    * build the read request for a `%Aerospike.Key{}`
    * parse the reply body into the GET result surface
    * trigger an asynchronous tend when the shared retry driver sees a
      rebalance response
  """

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Record
  alias Aerospike.RetryPolicy
  alias Aerospike.Tender
  alias Aerospike.UnaryCommand
  alias Aerospike.UnaryExecutor

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:replica_policy, :master | :sequence}

  @doc """
  Reads all bins for `key` from the cluster identified by `tender`.

  `tender` is a running `Aerospike.Tender` (pid or registered name). The
  spike only supports reading every bin (`bins = :all`); other shapes
  return `{:error, %Aerospike.Error{code: :invalid_argument}}` so future
  tasks can widen the API without changing the signature.

  Options:

    * `:timeout` — total op-budget milliseconds shared across the
      initial attempt and every retry. Default `5_000`.
    * `:max_retries` — overrides the cluster-default retry cap. `0`
      disables retry entirely.
    * `:sleep_between_retries_ms` — fixed delay between attempts.
    * `:replica_policy` — `:master` or `:sequence`.

  Errors surface as typed `Aerospike.Error` structs or the router's
  `:cluster_not_ready` / `:no_master` atoms for routing failures.
  """
  @spec execute(GenServer.server(), Key.t(), :all | term(), [option()]) ::
          {:ok, Record.t()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def execute(tender, key, bins, opts \\ [])

  def execute(tender, %Key{} = key, :all, opts) do
    runtime = runtime_ctx(tender)

    UnaryExecutor.run_command(executor(runtime, opts), command(), dispatch_ctx(runtime, key))
  end

  def execute(_tender, %Key{}, _bins, _opts) do
    {:error,
     %Error{
       code: :invalid_argument,
       message: "Aerospike.Get supports only :all bins in the spike"
     }}
  end

  # Fire a tend cycle without blocking the command path. Retaining the
  # returned task pid is unnecessary — if the tend fails, the existing
  # map is still the best we have, and the next retry will either hit
  # the same rebalance (and the budget burns out) or see the updated
  # map. Spawn with `:transient` (no link) so a transient failure cannot
  # tear down the caller.
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

  defp executor(runtime, opts) do
    UnaryExecutor.new!(
      base_policy: RetryPolicy.load(runtime.tables.meta),
      command_opts: opts,
      on_rebalance: fn -> trigger_tend_async(runtime.tender) end
    )
  end

  defp command do
    UnaryCommand.new!(
      name: __MODULE__,
      build_request: &encode_read/1,
      parse_response: &parse_record_response/2
    )
  end

  defp dispatch_ctx(runtime, %Key{} = key) do
    %{
      tender: runtime.tender,
      tables: runtime.tables,
      transport: runtime.transport,
      route_key: key,
      command_input: key
    }
  end

  defp runtime_ctx(tender) do
    %{
      tender: tender,
      tables: Tender.tables(tender),
      transport: Tender.transport(tender)
    }
  end

  defp encode_read(%Key{} = key) do
    key.namespace
    |> AsmMsg.read_command(key.set, key.digest)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp decode_as_msg(body) do
    case AsmMsg.decode(body) do
      {:ok, _} = ok ->
        ok

      {:error, reason} ->
        {:error, %Error{code: :parse_error, message: "failed to decode AS_MSG reply: #{reason}"}}
    end
  end

  defp parse_record_response(body, key) do
    case decode_as_msg(body) do
      {:ok, msg} ->
        Response.parse_record_response(msg, key)

      {:error, %Error{}} = err ->
        err
    end
  end
end
