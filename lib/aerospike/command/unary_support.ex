defmodule Aerospike.Command.UnarySupport do
  @moduledoc false

  alias Aerospike.Cluster
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.Response
  alias Aerospike.Runtime.Executor
  alias Aerospike.RuntimeMetrics

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
    start_mono = System.monotonic_time()

    result =
      UnaryCommand.run(command, executor(runtime, policy), %{
        tender: runtime.tender,
        tables: runtime.tables,
        transport: runtime.transport,
        route_key: route_key,
        command_input: command_input,
        metrics_cluster: runtime.tender
      })

    RuntimeMetrics.record_command(runtime.tender, command.name, start_mono, result)
    result
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

  @spec read_header_opts(Policy.UnaryRead.t(), boolean()) :: keyword()
  def read_header_opts(%Policy.UnaryRead{} = policy, use_compression) do
    [
      timeout: policy.timeout,
      read_mode_ap: policy.read_mode_ap,
      read_mode_sc: policy.read_mode_sc,
      read_touch_ttl_percent: policy.read_touch_ttl_percent,
      send_key: policy.send_key,
      use_compression: use_compression
    ]
  end

  @spec write_header_opts(Policy.UnaryWrite.t(), boolean()) :: keyword()
  def write_header_opts(%Policy.UnaryWrite{} = policy, use_compression) do
    [
      timeout: policy.timeout,
      ttl: policy.ttl,
      generation: policy.generation,
      generation_policy: policy.generation_policy,
      exists: policy.exists,
      commit_level: policy.commit_level,
      durable_delete: policy.durable_delete,
      send_key: policy.send_key,
      use_compression: use_compression
    ]
  end

  @spec operate_header_opts(Policy.UnaryWrite.t(), boolean(), boolean(), boolean()) :: keyword()
  def operate_header_opts(
        %Policy.UnaryWrite{} = policy,
        has_write?,
        respond_all?,
        use_compression
      ) do
    base =
      [
        timeout: policy.timeout,
        generation: policy.generation,
        generation_policy: policy.generation_policy,
        exists: policy.exists,
        commit_level: policy.commit_level,
        durable_delete: has_write? and policy.durable_delete,
        respond_all_ops: respond_all? or policy.respond_per_op,
        send_key: policy.send_key,
        read_mode_ap: policy.read_mode_ap,
        read_mode_sc: policy.read_mode_sc,
        use_compression: use_compression
      ]

    if has_write? do
      Keyword.put(base, :ttl, policy.ttl)
    else
      Keyword.put(base, :read_touch_ttl_percent, policy.read_touch_ttl_percent)
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
    Executor.new!(
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
