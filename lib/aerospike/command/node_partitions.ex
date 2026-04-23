defmodule Aerospike.Command.NodePartitions do
  @moduledoc false

  alias Aerospike.Command.PartitionStatus

  @enforce_keys [:node]
  defstruct [
    :node,
    parts_full: [],
    parts_partial: [],
    record_count: 0,
    record_max: 0,
    disallowed_count: 0,
    parts_unavailable: 0
  ]

  @type t :: %__MODULE__{
          node: binary(),
          parts_full: [PartitionStatus.t()],
          parts_partial: [PartitionStatus.t()],
          record_count: non_neg_integer(),
          record_max: non_neg_integer(),
          disallowed_count: non_neg_integer(),
          parts_unavailable: non_neg_integer()
        }

  @doc """
  Creates an empty bucket for one node.
  """
  @spec new(binary()) :: t()
  def new(node) when is_binary(node) do
    %__MODULE__{node: node}
  end

  @doc """
  Adds a partition to the bucket, splitting full and partial partitions by digest presence.
  """
  @spec add_partition(t(), PartitionStatus.t()) :: t()
  def add_partition(%__MODULE__{} = np, %PartitionStatus{digest: nil} = ps) do
    %{np | parts_full: [ps | np.parts_full]}
  end

  def add_partition(%__MODULE__{} = np, %PartitionStatus{} = ps) do
    %{np | parts_partial: [ps | np.parts_partial]}
  end
end
