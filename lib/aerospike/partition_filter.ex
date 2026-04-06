defmodule Aerospike.PartitionFilter do
  @moduledoc """
  Describes which partitions participate in a scan or query (pure data).

  Aerospike uses 4_096 partitions (ids `0..4095`). Use `all/0` for a full cluster scan,
  `by_id/1` or `by_range/2` for subsets, and `by_digest/1` to resume after a record cursor.
  """

  @partitions 4_096

  @doc false
  @spec partition_count() :: pos_integer()
  def partition_count, do: @partitions

  @enforce_keys [:begin, :count]
  defstruct begin: 0,
            count: 4096,
            digest: nil,
            partitions: []

  @type partition_entry :: %{
          required(:id) => integer(),
          optional(:digest) => binary() | nil,
          optional(:bval) => integer() | nil
        }

  @type t :: %__MODULE__{
          begin: non_neg_integer(),
          count: pos_integer(),
          digest: binary() | nil,
          partitions: [partition_entry()]
        }

  @doc """
  All partitions (`begin: 0`, `count: 4096`).
  """
  @spec all() :: t()
  def all do
    %__MODULE__{begin: 0, count: @partitions}
  end

  @doc """
  A single partition by id (`0..4095`).
  """
  @spec by_id(non_neg_integer()) :: t()
  def by_id(partition_id) when is_integer(partition_id) do
    if partition_id < 0 or partition_id > @partitions - 1 do
      raise ArgumentError,
            "partition_id must be in 0..#{@partitions - 1}, got: #{partition_id}"
    end

    %__MODULE__{begin: partition_id, count: 1}
  end

  @doc """
  A contiguous range of partitions starting at `begin` (0..4095) with length `count`.
  """
  @spec by_range(non_neg_integer(), pos_integer()) :: t()
  def by_range(begin_part, count) when is_integer(begin_part) and is_integer(count) do
    if begin_part < 0 or begin_part > @partitions - 1 do
      raise ArgumentError,
            "begin must be in 0..#{@partitions - 1}, got: #{begin_part}"
    end

    if count < 1 do
      raise ArgumentError, "count must be a positive integer, got: #{count}"
    end

    if begin_part + count > @partitions do
      raise ArgumentError,
            "begin + count exceeds partition count (#{@partitions}), got #{begin_part} + #{count}"
    end

    %__MODULE__{begin: begin_part, count: count}
  end

  @doc """
  Resume from a specific 20-byte record digest (typically returned with pagination cursors).
  """
  @spec by_digest(<<_::160>>) :: t()
  def by_digest(digest) when is_binary(digest) do
    if byte_size(digest) != 20 do
      raise ArgumentError, "digest must be 20 bytes, got: #{byte_size(digest)}"
    end

    %__MODULE__{begin: 0, count: @partitions, digest: digest}
  end
end
