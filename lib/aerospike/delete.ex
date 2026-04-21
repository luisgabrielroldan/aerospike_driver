defmodule Aerospike.Delete do
  @moduledoc """
  DELETE command adapter for the spike.

  Successful deletes return whether the record existed before removal.
  Missing-record replies stay distinguishable from successful deletes.
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
          | {:generation, non_neg_integer()}

  @type result ::
          {:ok, boolean()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}

  @spec execute(GenServer.server(), Key.t(), [option()]) :: result()
  def execute(tender, %Key{} = key, opts \\ []) when is_list(opts) do
    with {:ok, generation} <- generation_from_opts(opts) do
      UnarySupport.run_command(tender, key, opts, command(), %{key: key, generation: generation})
    end
  end

  defp command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :write,
      build_request: &encode_delete/1,
      parse_response: &parse_delete_response/2
    )
  end

  defp generation_from_opts(opts) do
    case Keyword.get(opts, :generation, 0) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      value -> invalid_generation(value)
    end
  end

  defp invalid_generation(value) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "generation must be a non-negative integer, got: #{inspect(value)}"
     )}
  end

  defp encode_delete(%{key: %Key{} = key, generation: generation}) do
    key
    |> AsmMsg.key_command([],
      write: true,
      delete: true,
      send_key: true,
      generation: generation
    )
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_delete_response(body, _input) do
    UnarySupport.parse_as_msg(body, &delete_result/1)
  end

  defp delete_result(msg) do
    case Response.parse_record_metadata_response(msg) do
      {:ok, _metadata} -> {:ok, true}
      {:error, %Error{code: :key_not_found}} -> {:ok, false}
      {:error, %Error{}} = err -> err
    end
  end
end
