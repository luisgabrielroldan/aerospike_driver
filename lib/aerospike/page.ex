defmodule Aerospike.Page do
  @moduledoc """
  One page of collected query results with a resumable partition cursor.

  Returned by `Aerospike.scan_page/3` and `Aerospike.query_page/3`. The page
  contains the records collected for this request plus cursor state for the
  next request when more partitions remain.
  """

  alias Aerospike.Cursor
  alias Aerospike.Record

  @enforce_keys [:records, :cursor, :done?]
  defstruct [:records, :cursor, :done?]

  @typedoc """
  One collected page of records.

  `cursor` is `nil` when no resume state is needed. `done?` indicates whether
  the server-side partition walk has completed. A page is not guaranteed to
  contain exactly the requested maximum number of records.
  """
  @type t :: %__MODULE__{
          records: [Record.t()],
          cursor: Cursor.t() | nil,
          done?: boolean()
        }
end
