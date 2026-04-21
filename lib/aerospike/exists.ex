defmodule Aerospike.Exists do
  @moduledoc """
  EXISTS command adapter for the spike.

  This is a header-only read. It proves record existence without
  fetching bins and returns a boolean instead of a full record.
  """

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.UnaryCommand
  alias Aerospike.UnarySupport

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:replica_policy, :master | :sequence}

  @type result ::
          {:ok, boolean()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}

  @spec execute(GenServer.server(), Key.t(), [option()]) :: result()
  def execute(tender, %Key{} = key, opts \\ []) when is_list(opts) do
    UnarySupport.run_command(tender, key, opts, command(), key)
  end

  defp command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :read,
      build_request: &encode_exists/1,
      parse_response: &parse_exists_response/2
    )
  end

  defp encode_exists(%Key{} = key) do
    key
    |> AsmMsg.key_command([], read: true, read_header: true)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_exists_response(body, _input) do
    UnarySupport.parse_as_msg(body, &exists_result/1)
  end

  defp exists_result(msg) do
    case Response.parse_record_metadata_response(msg) do
      {:ok, _metadata} -> {:ok, true}
      {:error, %Error{code: :key_not_found}} -> {:ok, false}
      {:error, %Error{}} = err -> err
    end
  end
end
