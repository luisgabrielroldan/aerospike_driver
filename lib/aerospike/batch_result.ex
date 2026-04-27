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

  @typedoc "Per-key batch status."
  @type status :: :ok | :error

  @typedoc "Per-key batch error returned for failed entries."
  @type error_reason :: Error.t() | atom()

  @typedoc """
  Per-key result returned by heterogeneous batch helpers.

  `record` contains returned bins or metadata for successful entries that
  produce data. `error` is set only when `status` is `:error`. `in_doubt`
  indicates that a write-style entry may have reached the server even though
  the client received an error.
  """
  @type t :: %__MODULE__{
          key: Key.t(),
          status: status(),
          record: Record.t() | map() | nil,
          error: error_reason() | nil,
          in_doubt: boolean()
        }

  @doc """
  Converts command-layer batch results into public batch result structs.
  """
  @spec from_command_results([term()]) :: [t()]
  def from_command_results(results) when is_list(results) do
    from_command_results(results, [])
  end

  defp from_command_results([], acc), do: Enum.reverse(acc)

  defp from_command_results(
         [
           %Result{
             key: key,
             status: :ok,
             record: record,
             error: nil,
             in_doubt: in_doubt
           }
           | rest
         ],
         acc
       ) do
    result = %__MODULE__{
      key: key,
      status: :ok,
      record: record,
      error: nil,
      in_doubt: in_doubt
    }

    from_command_results(rest, [result | acc])
  end

  defp from_command_results(
         [
           %Result{
             key: key,
             status: :error,
             record: nil,
             error: error,
             in_doubt: in_doubt
           }
           | rest
         ],
         acc
       )
       when not is_nil(error) do
    result = %__MODULE__{
      key: key,
      status: :error,
      record: nil,
      error: error,
      in_doubt: in_doubt
    }

    from_command_results(rest, [result | acc])
  end

  @doc """
  Converts one command-layer batch result into a public batch result struct.
  """
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
