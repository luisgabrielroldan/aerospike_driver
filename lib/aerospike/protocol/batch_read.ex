defmodule Aerospike.Protocol.BatchRead do
  @moduledoc false

  alias Aerospike.Command.BatchCommand.NodeRequest
  alias Aerospike.Error
  alias Aerospike.Protocol.Batch
  alias Aerospike.Record

  defmodule RecordResult do
    @moduledoc false

    @enforce_keys [:index, :key, :result]
    defstruct [:index, :key, :result, :generation, :ttl, :bins]

    @type t :: %__MODULE__{
            index: non_neg_integer(),
            key: Aerospike.Key.t(),
            result: :ok | {:error, Error.t()},
            generation: non_neg_integer() | nil,
            ttl: non_neg_integer() | nil,
            bins: map() | nil
          }
  end

  defmodule Reply do
    @moduledoc false

    @enforce_keys [:records]
    defstruct [:records]

    @type t :: %__MODULE__{records: [RecordResult.t()]}
  end

  @spec encode_request(NodeRequest.t(), keyword()) :: iodata()
  def encode_request(%NodeRequest{} = node_request, opts \\ []) when is_list(opts) do
    Batch.encode_request(node_request,
      layout: :batch_index_with_set,
      timeout: Keyword.get(opts, :timeout, 0)
    )
  end

  @spec parse_response(binary(), NodeRequest.t(), keyword()) ::
          {:ok, Reply.t()} | {:error, Error.t()}
  def parse_response(body, %NodeRequest{} = node_request, _opts \\ []) when is_binary(body) do
    with {:ok, %Batch.Reply{results: results}} <- Batch.parse_response(body, node_request) do
      {:ok, %Reply{records: Enum.map(results, &to_record_result/1)}}
    end
  end

  defp to_record_result(%{status: :ok, record: %Record{} = record, key: key, index: index}) do
    %RecordResult{
      index: index,
      key: key,
      result: :ok,
      generation: record.generation,
      ttl: record.ttl,
      bins: record.bins
    }
  end

  defp to_record_result(%{
         status: :ok,
         record: %{generation: generation, ttl: ttl},
         key: key,
         index: index
       }) do
    %RecordResult{
      index: index,
      key: key,
      result: :ok,
      generation: generation,
      ttl: ttl,
      bins: nil
    }
  end

  defp to_record_result(%{status: :error, error: %Error{} = error, key: key, index: index}) do
    %RecordResult{
      index: index,
      key: key,
      result: {:error, error},
      generation: nil,
      ttl: nil,
      bins: nil
    }
  end
end
