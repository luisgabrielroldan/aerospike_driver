defmodule Aerospike.Op.HLL do
  @moduledoc """
  HyperLogLog (HLL) operations for `Aerospike.operate/4`.

  Requires Aerospike server 4.9+. The server does not support HLL bins nested inside lists or
  maps, so these operations do not take `ctx:`.

  For `init/4` and `add/5`, pass `-1` for both bit counts (defaults) to let the server choose
  sizes. When set explicitly, index bits are typically 4–16, min-hash bits 4–58, and their
  sum must not exceed 64.

  Multi-HLL read operations take `hlls` as a list of `{:bytes, blob}` HLL values. Pass `flags:`
  on modify operations for write policy (default `0`).
  """

  alias Aerospike.Protocol.CDT

  @opaque t :: Aerospike.Op.t()

  @init 0
  @add 1
  @set_union 2
  @set_count 3
  @fold 4
  @count 50
  @union 51
  @union_count 52
  @intersect_count 53
  @similarity 54
  @describe 55

  defp flags(opts), do: Keyword.get(opts, :flags, 0)

  @doc """
  Creates a new HLL bin or resets an existing one.

  Server does not return a value. Use `-1` for both bit counts for server defaults.
  """
  @spec init(String.t(), integer(), integer(), keyword()) :: t()
  def init(bin_name, index_bit_count \\ -1, min_hash_bit_count \\ -1, opts \\ [])
      when is_binary(bin_name) and is_integer(index_bit_count) and is_integer(min_hash_bit_count) do
    CDT.hll_modify_op(bin_name, @init, [index_bit_count, min_hash_bit_count, flags(opts)])
  end

  @doc """
  Adds elements to the HLL (`elements` is UTF-8 strings and/or `{:bytes, binary}` entries).

  If the bin is missing, it is created using the given bit counts (`-1` for defaults).
  Server returns how many inputs caused a register update.
  """
  @spec add(String.t(), list(), integer(), integer(), keyword()) :: t()
  def add(bin_name, elements, index_bit_count \\ -1, min_hash_bit_count \\ -1, opts \\ [])
      when is_binary(bin_name) and is_list(elements) do
    CDT.hll_modify_op(bin_name, @add, [elements, index_bit_count, min_hash_bit_count, flags(opts)])
  end

  @doc """
  Merges each HLL in `hlls` into the bin (`hlls` is a list of `{:bytes, blob}`).

  Server does not return a value.
  """
  @spec set_union(String.t(), list(), keyword()) :: t()
  def set_union(bin_name, hlls, opts \\ []) when is_binary(bin_name) and is_list(hlls) do
    CDT.hll_modify_op(bin_name, @set_union, [hlls, flags(opts)])
  end

  @doc """
  Refreshes the cached cardinality if stale and returns the estimated count.
  """
  @spec refresh_count(String.t()) :: t()
  def refresh_count(bin_name) when is_binary(bin_name) do
    CDT.hll_modify_op(bin_name, @set_count, [])
  end

  @doc """
  Folds the HLL to a smaller `index_bit_count` (only when min-hash bit count on the bin is 0).

  Server does not return a value.
  """
  @spec fold(String.t(), integer()) :: t()
  def fold(bin_name, index_bit_count) when is_binary(bin_name) and is_integer(index_bit_count) do
    CDT.hll_modify_op(bin_name, @fold, [index_bit_count])
  end

  @doc """
  Returns the estimated cardinality of the HLL bin.
  """
  @spec get_count(String.t()) :: t()
  def get_count(bin_name) when is_binary(bin_name), do: CDT.hll_read_op(bin_name, @count, [])

  @doc """
  Returns an HLL blob that is the union of the bin with every HLL in `hlls`.

  `hlls` is a list of `{:bytes, blob}`. `opts` is reserved for future use.
  """
  @spec get_union(String.t(), list(), keyword()) :: t()
  def get_union(bin_name, hlls, opts \\ []) when is_binary(bin_name) and is_list(hlls) do
    _ = opts
    CDT.hll_read_op(bin_name, @union, [hlls])
  end

  @doc """
  Returns the estimated cardinality of the union of the bin with all HLLs in `hlls`.

  `opts` is reserved for future use.
  """
  @spec get_union_count(String.t(), list(), keyword()) :: t()
  def get_union_count(bin_name, hlls, opts \\ []) when is_binary(bin_name) and is_list(hlls) do
    _ = opts
    CDT.hll_read_op(bin_name, @union_count, [hlls])
  end

  @doc """
  Returns the estimated cardinality of the intersection of the bin with all HLLs in `hlls`.

  `opts` is reserved for future use.
  """
  @spec get_intersect_count(String.t(), list(), keyword()) :: t()
  def get_intersect_count(bin_name, hlls, opts \\ [])
      when is_binary(bin_name) and is_list(hlls) do
    _ = opts
    CDT.hll_read_op(bin_name, @intersect_count, [hlls])
  end

  @doc """
  Returns an estimated similarity score (double) for the bin and all HLLs in `hlls`.

  `opts` is reserved for future use.
  """
  @spec get_similarity(String.t(), list(), keyword()) :: t()
  def get_similarity(bin_name, hlls, opts \\ []) when is_binary(bin_name) and is_list(hlls) do
    _ = opts
    CDT.hll_read_op(bin_name, @similarity, [hlls])
  end

  @doc """
  Returns `[index_bit_count, min_hash_bit_count]` used to create the HLL (two integers).
  """
  @spec describe(String.t()) :: t()
  def describe(bin_name) when is_binary(bin_name), do: CDT.hll_read_op(bin_name, @describe, [])
end
