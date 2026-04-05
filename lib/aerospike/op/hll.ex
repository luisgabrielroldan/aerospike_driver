defmodule Aerospike.Op.HLL do
  @moduledoc """
  HyperLogLog (HLL) operations for `Aerospike.operate/4`.

  Requires Aerospike server 4.9+. HLL bins cannot be nested inside lists/maps on the server;
  there is no `ctx:` option.

  Use `index_bit_count: -1` and `min_hash_bit_count: -1` (defaults) to let the server pick sizes.
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

  @doc "Create or reset an HLL bin."
  @spec init(String.t(), integer(), integer(), keyword()) :: t()
  def init(bin_name, index_bit_count \\ -1, min_hash_bit_count \\ -1, opts \\ [])
      when is_binary(bin_name) and is_integer(index_bit_count) and is_integer(min_hash_bit_count) do
    CDT.hll_modify_op(bin_name, @init, [index_bit_count, min_hash_bit_count, flags(opts)])
  end

  @doc "Add string elements to the HLL (pass a list of UTF-8 strings or `{:bytes, binary}` entries)."
  @spec add(String.t(), list(), integer(), integer(), keyword()) :: t()
  def add(bin_name, elements, index_bit_count \\ -1, min_hash_bit_count \\ -1, opts \\ [])
      when is_binary(bin_name) and is_list(elements) do
    CDT.hll_modify_op(bin_name, @add, [elements, index_bit_count, min_hash_bit_count, flags(opts)])
  end

  @doc "Union additional HLL blobs into the bin (`hlls` is a list of `{:bytes, blob}`)."
  @spec set_union(String.t(), list(), keyword()) :: t()
  def set_union(bin_name, hlls, opts \\ []) when is_binary(bin_name) and is_list(hlls) do
    CDT.hll_modify_op(bin_name, @set_union, [hlls, flags(opts)])
  end

  @doc "Refresh cached count on the server (returns count)."
  @spec refresh_count(String.t()) :: t()
  def refresh_count(bin_name) when is_binary(bin_name) do
    CDT.hll_modify_op(bin_name, @set_count, [])
  end

  @doc "Fold index bit width to `index_bit_count`."
  @spec fold(String.t(), integer()) :: t()
  def fold(bin_name, index_bit_count) when is_binary(bin_name) and is_integer(index_bit_count) do
    CDT.hll_modify_op(bin_name, @fold, [index_bit_count])
  end

  @spec get_count(String.t()) :: t()
  def get_count(bin_name) when is_binary(bin_name), do: CDT.hll_read_op(bin_name, @count, [])

  @spec get_union(String.t(), list(), keyword()) :: t()
  def get_union(bin_name, hlls, opts \\ []) when is_binary(bin_name) and is_list(hlls) do
    _ = opts
    CDT.hll_read_op(bin_name, @union, [hlls])
  end

  @spec get_union_count(String.t(), list(), keyword()) :: t()
  def get_union_count(bin_name, hlls, opts \\ []) when is_binary(bin_name) and is_list(hlls) do
    _ = opts
    CDT.hll_read_op(bin_name, @union_count, [hlls])
  end

  @spec get_intersect_count(String.t(), list(), keyword()) :: t()
  def get_intersect_count(bin_name, hlls, opts \\ [])
      when is_binary(bin_name) and is_list(hlls) do
    _ = opts
    CDT.hll_read_op(bin_name, @intersect_count, [hlls])
  end

  @spec get_similarity(String.t(), list(), keyword()) :: t()
  def get_similarity(bin_name, hlls, opts \\ []) when is_binary(bin_name) and is_list(hlls) do
    _ = opts
    CDT.hll_read_op(bin_name, @similarity, [hlls])
  end

  @spec describe(String.t()) :: t()
  def describe(bin_name) when is_binary(bin_name), do: CDT.hll_read_op(bin_name, @describe, [])
end
