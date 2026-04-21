defmodule Aerospike.Page do
  @moduledoc """
  One page of scan or query results with a resumable cursor.
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
