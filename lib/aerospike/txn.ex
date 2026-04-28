defmodule Aerospike.Txn do
  @moduledoc """
  Transaction handle for multi-record transactions.

  `Aerospike.Txn` is an immutable handle that identifies a transaction in
  progress. Create one with `new/0` or `new/1`, then pass it as the `:txn`
  option to transaction-aware single-record commands.

  Creating this struct does not start a transaction by itself. Runtime state
  lives in the started cluster's ETS tables and is initialized when
  `Aerospike.transaction/2` or `Aerospike.transaction/3` enters the callback.
  This struct only carries the transaction identifier and timeout.

  The handle is only valid against the cluster that initialized it. A fresh
  `%Aerospike.Txn{}` or a handle reused against another cluster has no runtime
  state behind it, so transaction commands reject it.

  Passing the same `%Aerospike.Txn{}` to multiple concurrent processes is
  undefined behavior. The tracking row is shared mutable state, and the driver
  does not serialize concurrent updates around one transaction handle.
  """

  @enforce_keys [:id, :timeout]
  defstruct [:id, :timeout]

  @typedoc """
  Multi-record transaction handle.

  `id` is the signed 64-bit transaction identifier sent to the server.
  `timeout` is the multi-record transaction timeout in milliseconds, where `0`
  asks the server to use its configured default duration.
  """
  @type t :: %__MODULE__{
          id: integer(),
          timeout: non_neg_integer()
        }

  @typedoc "Option accepted by `new/1`."
  @type option :: {:timeout, non_neg_integer()}

  @typedoc "Keyword options accepted by `new/1`."
  @type opts :: [option()]

  @doc """
  Creates a new transaction handle with a random signed int64 ID.
  """
  @spec new() :: t()
  def new do
    %__MODULE__{id: random_id(), timeout: 0}
  end

  @doc """
  Creates a new transaction handle with the given options.

  Options:

    * `:timeout` — transaction timeout in milliseconds. `0` asks the server to
      use its configured multi-record transaction duration.
  """
  @spec new(opts()) :: t()
  def new(opts) when is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 0)
    %__MODULE__{id: random_id(), timeout: timeout}
  end

  defp random_id do
    <<id::64-signed-big>> = :crypto.strong_rand_bytes(8)
    id
  end
end
