defmodule Aerospike.UDF do
  @moduledoc """
  Metadata for one registered server-side UDF package.

  Returned by `Aerospike.list_udfs/1` and `list_udfs/2`.
  """

  @enforce_keys [:filename, :hash, :language]
  defstruct [:filename, :hash, :language]

  @typedoc "Metadata for one registered UDF package."
  @type t :: %__MODULE__{
          filename: String.t(),
          hash: String.t(),
          language: String.t()
        }
end
