defmodule Aerospike.Role do
  @moduledoc """
  Security role as returned by Aerospike role metadata queries.
  """

  alias Aerospike.Privilege

  @enforce_keys [:name]
  defstruct [:name, privileges: [], whitelist: [], read_quota: 0, write_quota: 0]

  @typedoc "List of whitelisted client addresses for a role."
  @type whitelist :: [String.t()]

  @typedoc "Maximum operations per second for a role. `0` means unlimited."
  @type quota :: non_neg_integer()

  @typedoc "Security role metadata returned by role queries."
  @type t :: %__MODULE__{
          name: String.t(),
          privileges: [Privilege.t()],
          whitelist: whitelist(),
          read_quota: quota(),
          write_quota: quota()
        }
end
