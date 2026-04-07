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

  @typedoc """
  Normalized record bins returned by reads.

  Bin names are strings.
  """
  @type bins :: %{String.t() => term()}

  @typedoc """
  Bin map accepted by write APIs before normalization.

  Bin names may be strings or atoms; atom names are converted to strings.
  """
  @type bins_input :: %{required(String.t() | atom()) => term()}

  @type t :: %__MODULE__{
          key: Key.t(),
          bins: bins(),
          generation: non_neg_integer(),
          ttl: non_neg_integer()
        }
end
