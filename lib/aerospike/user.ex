defmodule Aerospike.User do
  @moduledoc """
  Security user metadata returned by Aerospike user queries.

  `read_info` and `write_info` preserve the server's ordered counters so later
  server versions can add metrics without changing the result shape.
  """

  defstruct [:name, roles: [], read_info: [], write_info: [], connections_in_use: 0]

  @typedoc """
  Opaque ordered statistics vector returned by the server.

  The client preserves the server order rather than naming individual counters
  because available counters vary by server version and configuration.
  """
  @type info_counters :: [non_neg_integer()]

  @typedoc """
  Security user metadata returned by user queries.

  `roles` contains role names assigned to the user. `read_info`,
  `write_info`, and `connections_in_use` are server-reported security
  statistics when present in the info response.
  """
  @type t :: %__MODULE__{
          name: String.t(),
          roles: [String.t()],
          read_info: info_counters(),
          write_info: info_counters(),
          connections_in_use: non_neg_integer()
        }
end
