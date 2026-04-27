defmodule Aerospike.PartitionFilter do
  @moduledoc """
  Describes which partitions participate in a scan or query.

  This is a pure data container. It does not own routing, retries, or
  execution state.
  """

  @partitions 4_096

  @doc """
  Returns the fixed Aerospike partition count.
  """
  @spec partition_count() :: pos_integer()
  def partition_count, do: @partitions

  @enforce_keys [:begin, :count]
  defstruct begin: 0,
            count: @partitions,
            digest: nil,
            partitions: [],
            done?: false,
            retry?: true

  @typedoc """
  Per-partition resume state used by scan/query pagination.

  `:digest` and `:bval` represent the last server cursor position for the
  partition, not user-key order.
  """
  @type partition_entry :: %{
          required(:id) => non_neg_integer(),
          optional(:digest) => binary() | nil,
          optional(:bval) => integer() | nil
        }

  @typedoc "Partition selection and resume state for scans and queries."
  @type t :: %__MODULE__{
          begin: non_neg_integer(),
          count: pos_integer(),
          digest: binary() | nil,
          partitions: [partition_entry()],
          done?: boolean(),
          retry?: boolean()
        }

  @doc """
  All partitions (`begin: 0`, `count: 4096`).
  """
  @spec all() :: t()
  def all do
    %__MODULE__{begin: 0, count: @partitions, done?: false, retry?: true}
  end

  @doc """
  One partition by id.
  """
  @spec by_id(non_neg_integer()) :: t()
  def by_id(partition_id) when is_integer(partition_id) do
    validate_partition_id!(partition_id)
    %__MODULE__{begin: partition_id, count: 1, done?: false, retry?: true}
  end

  @doc """
  A contiguous partition range.
  """
  @spec by_range(non_neg_integer(), pos_integer()) :: t()
  def by_range(begin_part, count) when is_integer(begin_part) and is_integer(count) do
    validate_partition_id!(begin_part)

    if count < 1 do
      raise ArgumentError, "count must be a positive integer, got: #{count}"
    end

    if begin_part + count > @partitions do
      raise ArgumentError,
            "begin + count exceeds partition count (#{@partitions}), got #{begin_part} + #{count}"
    end

    %__MODULE__{begin: begin_part, count: count, done?: false, retry?: true}
  end

  @doc """
  Resume from a specific record digest.
  """
  @spec by_digest(<<_::160>>) :: t()
  def by_digest(digest) when is_binary(digest) do
    if byte_size(digest) != 20 do
      raise ArgumentError, "digest must be 20 bytes, got: #{byte_size(digest)}"
    end

    %__MODULE__{begin: 0, count: @partitions, digest: digest, done?: false, retry?: true}
  end

  defp validate_partition_id!(partition_id) do
    if partition_id < 0 or partition_id > @partitions - 1 do
      raise ArgumentError,
            "partition_id must be in 0..#{@partitions - 1}, got: #{partition_id}"
    end
  end
end
