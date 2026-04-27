defmodule Aerospike.Page do
  @moduledoc """
  One page of collected query results with a resumable partition cursor.
  """

  alias Aerospike.Cursor
  alias Aerospike.Record

  @enforce_keys [:records, :cursor, :done?]
  defstruct [:records, :cursor, :done?]

  @typedoc """
  One collected page of records.

  `cursor` is `nil` when no resume state is needed. `done?` indicates whether
  the server-side partition walk has completed.
  """
  @type t :: %__MODULE__{
          records: [Record.t()],
          cursor: Cursor.t() | nil,
          done?: boolean()
        }
end
