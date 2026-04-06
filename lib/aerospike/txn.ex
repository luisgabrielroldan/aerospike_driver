defmodule Aerospike.Txn do
  @moduledoc """
  Transaction handle for multi-record transactions (MRT).

  An `Aerospike.Txn` is a lightweight, immutable data structure that identifies a
  transaction in progress. Create one with `new/0` or `new/1`, then pass it as the
  `txn:` option to any CRUD operation that participates in the transaction.

  ## Creating a transaction

      txn = Aerospike.Txn.new()
      Aerospike.put(:conn, key1, %{"score" => 10}, txn: txn)
      Aerospike.put(:conn, key2, %{"score" => 20}, txn: txn)
      Aerospike.commit(:conn, txn)

  You can set a timeout (in milliseconds). A timeout of `0` means the server applies
  its default transaction timeout:

      txn = Aerospike.Txn.new(timeout: 5_000)

  ## State management

  Transaction state — tracked reads, tracked writes, commit/abort status — lives in an
  ETS table managed by the connection, **not** in this struct. The struct is a read-only
  handle that identifies the transaction by its `id`.

  Passing the same `%Txn{}` to multiple concurrent processes is undefined behavior:
  the ETS state is shared, and concurrent mutation is not protected by any lock.

  ## Constraints

  - All operations within a single transaction must target the **same namespace**.
  - A transaction supports at most 4096 write keys.
  - Scans and queries cannot participate in a transaction.
  - Transactions require Aerospike Enterprise Edition with strong-consistency namespaces.
  """

  # Full signed int64 range: -(2^63) to 2^63-1
  @int64_min -9_223_372_036_854_775_808

  # 2^64 — the number of distinct values in the signed int64 range
  @int64_range 18_446_744_073_709_551_616

  @enforce_keys [:id, :timeout]
  defstruct [:id, :timeout]

  @type t :: %__MODULE__{
          id: integer(),
          timeout: non_neg_integer()
        }

  @doc """
  Creates a new transaction handle with a random ID and zero timeout.

  The `id` is a uniformly random signed 64-bit integer. A `timeout` of `0`
  instructs the server to apply its configured default transaction timeout.

  ## Examples

      iex> txn = Aerospike.Txn.new()
      iex> is_integer(txn.id)
      true
      iex> txn.timeout
      0

  """
  @spec new() :: t()
  def new do
    %__MODULE__{id: random_id(), timeout: 0}
  end

  @doc """
  Creates a new transaction handle with the given options.

  ## Options

  - `:timeout` — transaction timeout in milliseconds (default: `0`, meaning server default)

  ## Examples

      iex> txn = Aerospike.Txn.new(timeout: 5_000)
      iex> txn.timeout
      5_000

  """
  @spec new(keyword()) :: t()
  def new(opts) when is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 0)
    %__MODULE__{id: random_id(), timeout: timeout}
  end

  # Generates a uniformly random signed int64.
  # :rand.uniform(N) returns an integer in 1..N; subtracting 1 shifts it to 0..N-1.
  # Adding @int64_min maps [0, 2^64-1] → [-2^63, 2^63-1].
  defp random_id do
    @int64_min + :rand.uniform(@int64_range) - 1
  end
end
