defmodule Aerospike.PartitionStatus do
  @moduledoc false

  alias Aerospike.PartitionFilter

  @enforce_keys [:id]
  defstruct [
    :id,
    digest: nil,
    bval: nil,
    sequence: 0,
    retry?: true,
    node: nil
  ]

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          digest: binary() | nil,
          bval: integer() | nil,
          sequence: non_neg_integer(),
          retry?: boolean(),
          node: binary() | nil
        }

  @doc """
  Builds a fresh partition status for `id`.
  """
  @spec new(non_neg_integer()) :: t()
  def new(id) when is_integer(id) and id >= 0 do
    %__MODULE__{id: id, retry?: true}
  end

  @doc """
  Builds a status from a serialized partition entry and resets runtime fields.
  """
  @spec from_entry(PartitionFilter.partition_entry()) :: t()
  def from_entry(%{id: id} = entry) when is_integer(id) and id >= 0 do
    %__MODULE__{
      id: id,
      digest: Map.get(entry, :digest),
      bval: Map.get(entry, :bval),
      sequence: 0,
      retry?: true,
      node: nil
    }
  end
end
