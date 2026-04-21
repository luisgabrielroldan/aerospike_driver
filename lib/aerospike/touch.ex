defmodule Aerospike.Touch do
  @moduledoc """
  TOUCH command adapter for the spike.

  Touch updates record metadata without reading bins and returns the
  server-reported header metadata on success.
  """

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Record
  alias Aerospike.UnaryCommand
  alias Aerospike.UnarySupport

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:ttl, non_neg_integer()}
          | {:generation, non_neg_integer()}

  @type result ::
          {:ok, Record.metadata()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}

  @spec execute(GenServer.server(), Key.t(), [option()]) :: result()
  def execute(tender, %Key{} = key, opts \\ []) when is_list(opts) do
    with {:ok, ttl} <- non_negative_opt(opts, :ttl),
         {:ok, generation} <- non_negative_opt(opts, :generation) do
      UnarySupport.run_command(
        tender,
        key,
        opts,
        command(),
        %{key: key, ttl: ttl, generation: generation}
      )
    end
  end

  defp command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :write,
      build_request: &encode_touch/1,
      parse_response: &parse_metadata_response/2
    )
  end

  defp non_negative_opt(opts, key) do
    case Keyword.get(opts, key, 0) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      value -> invalid_non_negative_opt(key, value)
    end
  end

  defp invalid_non_negative_opt(key, value) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "#{key} must be a non-negative integer, got: #{inspect(value)}"
     )}
  end

  defp encode_touch(%{key: %Key{} = key, ttl: ttl, generation: generation}) do
    key
    |> AsmMsg.key_command([Operation.touch()],
      write: true,
      send_key: true,
      ttl: ttl,
      generation: generation
    )
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_metadata_response(body, _input) do
    UnarySupport.parse_as_msg(body, &Response.parse_record_metadata_response/1)
  end
end
