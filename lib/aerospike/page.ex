defmodule Aerospike.Page do
  @moduledoc """
  One page of scan or query results with a resumable cursor.

  `done?` is `true` when no further pages remain. `cursor` may be `nil` when the result
  set is exhausted.
  """

  alias Aerospike.Cursor
  alias Aerospike.Record

  @enforce_keys [:records, :cursor, :done?]
  defstruct [:records, :cursor, :done?]

  @type t :: %__MODULE__{
          records: [Record.t()],
          cursor: Cursor.t() | nil,
          done?: boolean()
        }
end
