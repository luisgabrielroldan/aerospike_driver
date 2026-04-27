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

  @typedoc """
  Record header metadata returned by successful writes or header-only reads.
  """
  @type metadata :: %{generation: non_neg_integer(), ttl: non_neg_integer()}

  @typedoc """
  Record returned by read, operate, scan, and query APIs.

  `ttl` is the server-reported expiration/TTL value carried in the record
  header, using the same value returned by the Aerospike wire protocol.
  """
  @type t :: %__MODULE__{
          key: Key.t(),
          bins: bins(),
          generation: non_neg_integer(),
          ttl: non_neg_integer()
        }
end
