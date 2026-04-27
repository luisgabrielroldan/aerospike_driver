defmodule Aerospike.User do
  @moduledoc """
  Security user metadata returned by Aerospike user queries.

  `read_info` and `write_info` preserve the server's ordered counters so later
  server versions can add metrics without changing the result shape.
  """

  defstruct [:name, roles: [], read_info: [], write_info: [], connections_in_use: 0]

  @typedoc "Opaque ordered statistics vector returned by the server."
  @type info_counters :: [non_neg_integer()]

  @typedoc "Security user metadata returned by user queries."
  @type t :: %__MODULE__{
          name: String.t(),
          roles: [String.t()],
          read_info: info_counters(),
          write_info: info_counters(),
          connections_in_use: non_neg_integer()
        }
end
