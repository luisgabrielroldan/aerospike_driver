defmodule Aerospike.Record do
  @moduledoc """
  A single Aerospike record returned by read operations.

  Bin names in `bins` are always **strings** (wire format uses string bin names).

  ## Example

      %Aerospike.Record{
        key: some_key,
        bins: %{"name" => "Alice"},
        generation: 1,
        ttl: 3600
      }

  """

  alias Aerospike.Key

  @enforce_keys [:key, :bins, :generation, :ttl]
  defstruct [:key, :bins, :generation, :ttl]

  @type t :: %__MODULE__{
          key: Key.t(),
          bins: %{String.t() => term()},
          generation: non_neg_integer(),
          ttl: non_neg_integer()
        }
end
