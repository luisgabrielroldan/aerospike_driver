defmodule Aerospike.UDF do
  @moduledoc """
  Metadata for one server-side UDF package.

  Returned by `Aerospike.list_udfs/2` once UDF inventory support is
  implemented.
  """

  @enforce_keys [:filename, :hash, :language]
  defstruct [:filename, :hash, :language]

  @type t :: %__MODULE__{
          filename: String.t(),
          hash: String.t(),
          language: String.t()
        }
end
