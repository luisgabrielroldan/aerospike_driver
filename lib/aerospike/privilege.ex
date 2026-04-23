defmodule Aerospike.Privilege do
  @moduledoc """
  Security privilege assigned to an Aerospike role.

  Global privileges use `nil` for `:namespace` and `:set`. Data privileges may
  narrow scope to a namespace or namespace/set pair.
  """

  @enforce_keys [:code]
  defstruct [:code, :raw_code, :namespace, :set]

  @typedoc "Known Aerospike privilege identifiers."
  @type code ::
          :user_admin
          | :sys_admin
          | :data_admin
          | :udf_admin
          | :sindex_admin
          | :read
          | :read_write
          | :read_write_udf
          | :write
          | :truncate
          | :masking_admin
          | :read_masked
          | :write_masked
          | :unknown

  @type t :: %__MODULE__{
          code: code(),
          raw_code: non_neg_integer() | nil,
          namespace: String.t() | nil,
          set: String.t() | nil
        }
end
