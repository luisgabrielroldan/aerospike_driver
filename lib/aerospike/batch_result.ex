defmodule Aerospike.BatchResult do
  @moduledoc """
  Per-key outcome returned by batch helpers that need record-level metadata.

  Each result keeps the original key and a status so callers can handle partial
  batch outcomes without losing caller-order context.

  ## Field invariants

  - When `status` is `:ok`, `error` is `nil`. `record` may contain returned
    data or be `nil` for successful operations that do not return bins.
  - When `status` is `:error`, `record` is `nil`, `error` is set, and
    `in_doubt` reports whether a write outcome may be ambiguous.
  """

  alias Aerospike.Command.BatchCommand.Result
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Record

  @enforce_keys [:key, :status]
  defstruct [:key, :status, :record, :error, in_doubt: false]

  @type status :: :ok | :error
  @type error_reason :: Error.t() | atom()

  @type t :: %__MODULE__{
          key: Key.t(),
          status: status(),
          record: Record.t() | map() | nil,
          error: error_reason() | nil,
          in_doubt: boolean()
        }

  @doc false
  @spec from_command_result(term()) :: t()
  def from_command_result(%Result{
        key: key,
        status: :ok,
        record: record,
        error: nil,
        in_doubt: in_doubt
      }) do
    %__MODULE__{
      key: key,
      status: :ok,
      record: record,
      error: nil,
      in_doubt: in_doubt
    }
  end

  def from_command_result(%Result{
        key: key,
        status: :error,
        record: nil,
        error: error,
        in_doubt: in_doubt
      })
      when not is_nil(error) do
    %__MODULE__{
      key: key,
      status: :error,
      record: nil,
      error: error,
      in_doubt: in_doubt
    }
  end
end
