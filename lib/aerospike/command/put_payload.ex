defmodule Aerospike.Command.PutPayload do
  @moduledoc """
  Raw single-record write/delete payload command adapter.

  The caller supplies a complete wire frame. This command only uses the key for
  write routing and response parsing; the request bytes are forwarded unchanged.
  """

  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Command.UnarySupport
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.Response
  alias Aerospike.Runtime.TxnSupport

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:ttl, non_neg_integer()}
          | {:generation, non_neg_integer()}
          | {:filter, Aerospike.Exp.t() | nil}
          | {:txn, Aerospike.Txn.t()}

  @type result ::
          :ok
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}

  @spec execute(GenServer.server(), Key.t(), binary(), [option()]) :: result()
  def execute(tender, %Key{} = key, payload, opts \\ [])
      when is_binary(payload) and is_list(opts) do
    with {:ok, _txn} <- TxnSupport.txn_from_opts(opts),
         {:ok, policy} <- UnarySupport.write_policy(tender, opts) do
      tender
      |> UnarySupport.run_command(key, policy, command(), command_input(payload, policy))
      |> public_result()
    end
  end

  defp command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :write,
      build_request: &encode_payload/1,
      parse_response: &parse_write_response/2,
      prepare_transport_opts: &Keyword.put(&1, :use_compression, false)
    )
  end

  defp command_input(payload, %Policy.UnaryWrite{} = policy) do
    %{payload: payload, timeout: policy.timeout}
  end

  defp encode_payload(%{payload: payload}), do: payload

  defp parse_write_response(body, _input) do
    UnarySupport.parse_as_msg(body, fn msg ->
      case Response.parse_record_metadata_response(msg) do
        {:ok, _metadata} -> {:ok, :ok}
        {:error, %Error{}} = err -> err
      end
    end)
  end

  defp public_result({:ok, :ok}), do: :ok
  defp public_result(other), do: other
end
