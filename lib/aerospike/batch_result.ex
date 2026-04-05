defmodule Aerospike.BatchResult do
  @moduledoc """
  Per-key outcome from `Aerospike.batch_operate/3`.

  Unlike `Aerospike.batch_get/3` (which uses `nil` for missing keys), batch operate
  returns one `%BatchResult{}` per input operation so you can inspect `status`,
  `error`, and `in_doubt` per key.

  ## Field invariants

  - When `status` is `:ok` — `error` is `nil`, `in_doubt` is `false`. `record` may be
    present (reads) or `nil` (writes/deletes that succeeded without returned bins).
  - When `status` is `:error` — `record` is `nil`, `error` is set. `in_doubt` may be
    `true` if the server indicated an ambiguous write outcome.

  ## Pattern matching

      {:ok, results} = Aerospike.batch_operate(:aero, ops)

      Enum.each(results, fn
        %BatchResult{status: :ok, record: %Aerospike.Record{} = rec} ->
          IO.inspect(rec.bins)

        %BatchResult{status: :ok, record: nil} ->
          IO.puts("write/delete succeeded")

        %BatchResult{status: :error, error: err, in_doubt: true} ->
          Logger.warning("ambiguous write: \#{err.message}")

        %BatchResult{status: :error, error: err} ->
          Logger.error("failed: \#{err.code}")
      end)

  ## Related

  - `Aerospike.Batch` — constructors for batch operations
  - `Aerospike.batch_operate/3` — executes the batch and returns these results
  """

  alias Aerospike.Error
  alias Aerospike.Record

  @enforce_keys [:status]
  defstruct status: nil, record: nil, error: nil, in_doubt: false

  @type t :: %__MODULE__{
          status: :ok | :error,
          record: Record.t() | nil,
          error: Error.t() | nil,
          in_doubt: boolean()
        }

  @doc """
  Builds a successful result. `record` defaults to `nil` for write/delete outcomes.

  ## Examples

      BatchResult.ok()                # write/delete success
      BatchResult.ok(record)          # read success with record data

  """
  @spec ok(Record.t() | nil) :: t()
  def ok(record \\ nil) do
    %__MODULE__{status: :ok, record: record, error: nil, in_doubt: false}
  end

  @doc """
  Builds a failed result. `in_doubt` defaults to `false`.

  ## Examples

      err = Aerospike.Error.from_result_code(:key_not_found)
      BatchResult.error(err)              # definite failure
      BatchResult.error(err, true)        # ambiguous write outcome

  """
  @spec error(Error.t(), boolean()) :: t()
  def error(%Error{} = err, in_doubt \\ false) do
    %__MODULE__{status: :error, record: nil, error: err, in_doubt: in_doubt}
  end
end
