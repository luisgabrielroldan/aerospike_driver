defmodule Aerospike.Op.HLL do
  @moduledoc """
  HyperLogLog operations for `Aerospike.operate/4`.

  HyperLogLog bins require Aerospike server 4.9 or later. The server does not
  support HyperLogLog bins nested inside lists or maps, so these operations do
  not accept `ctx:`.

  `init/4` and `add/5` default both bit counts to `-1`, which asks the server
  to choose its defaults. When set explicitly, index bits are typically 4 to
  16, min-hash bits 4 to 58, and their sum must not exceed 64.

  Multi-HLL read operations accept `hlls` as a list of `{:bytes, blob}` HLL
  values. Modify operations accept raw integer `flags:` values for the server
  write policy.
  """

  alias Aerospike.Protocol.CDT

  @typedoc """
  Opaque HyperLogLog operation for `Aerospike.operate/4`.
  """
  @opaque t :: Aerospike.Op.t()

  @typedoc """
  HyperLogLog write flags integer sent to the server.

  Use the `write_*` helpers in this module and combine multiple flags with
  `Bitwise.bor/2`.
  """
  @type flags :: non_neg_integer()

  @typedoc """
  Options accepted by HyperLogLog modify operations.

  Supported key:

  * `:flags` - HyperLogLog write flags from the `write_*` helpers. Defaults to `0`.
  """
  @type write_opts :: [flags: flags()]

  @typedoc """
  Options accepted by multi-HLL read helpers.

  No option keys are currently encoded for these read operations; the argument
  is accepted for API consistency.
  """
  @type read_opts :: []

  @init 0
  @add 1
  @set_union 2
  @refresh_count 3
  @fold 4
  @count 50
  @union 51
  @union_count 52
  @intersect_count 53
  @similarity 54
  @describe 55

  @doc "Use default HyperLogLog write behavior."
  @spec write_default() :: 0
  def write_default, do: 0

  @doc "Only create the HyperLogLog bin when it does not already exist."
  @spec write_create_only() :: 1
  def write_create_only, do: 1

  @doc "Only update the HyperLogLog bin when it already exists."
  @spec write_update_only() :: 2
  def write_update_only, do: 2

  @doc "Do not fail the command when a HyperLogLog operation is denied by write flags."
  @spec write_no_fail() :: 4
  def write_no_fail, do: 4

  @doc "Allow folding to less precise HyperLogLog settings when needed."
  @spec write_allow_fold() :: 8
  def write_allow_fold, do: 8

  @doc """
  Creates a new HyperLogLog bin or resets an existing one.
  """
  @spec init(String.t(), integer(), integer(), write_opts()) :: t()
  def init(bin_name, index_bit_count \\ -1, min_hash_bit_count \\ -1, opts \\ [])
      when is_binary(bin_name) and is_integer(index_bit_count) and
             is_integer(min_hash_bit_count) and is_list(opts) do
    CDT.hll_modify_op(bin_name, @init, [index_bit_count, min_hash_bit_count, flags(opts)])
  end

  @doc """
  Adds elements to a HyperLogLog bin.

  Elements may be strings or `{:bytes, binary}` values. If the bin is missing,
  the server creates it using the supplied bit counts.
  """
  @spec add(String.t(), list(), integer(), integer(), write_opts()) :: t()
  def add(bin_name, elements, index_bit_count \\ -1, min_hash_bit_count \\ -1, opts \\ [])
      when is_binary(bin_name) and is_list(elements) and is_integer(index_bit_count) and
             is_integer(min_hash_bit_count) and is_list(opts) do
    CDT.hll_modify_op(bin_name, @add, [elements, index_bit_count, min_hash_bit_count, flags(opts)])
  end

  @doc """
  Merges every HyperLogLog value in `hlls` into the bin.
  """
  @spec set_union(String.t(), list(), write_opts()) :: t()
  def set_union(bin_name, hlls, opts \\ [])
      when is_binary(bin_name) and is_list(hlls) and is_list(opts) do
    CDT.hll_modify_op(bin_name, @set_union, [hlls, flags(opts)])
  end

  @doc """
  Refreshes stale cached cardinality and returns the estimated count.
  """
  @spec refresh_count(String.t()) :: t()
  def refresh_count(bin_name) when is_binary(bin_name) do
    CDT.hll_modify_op(bin_name, @refresh_count, [])
  end

  @doc """
  Folds the HyperLogLog bin to a smaller index bit count.
  """
  @spec fold(String.t(), integer()) :: t()
  def fold(bin_name, index_bit_count) when is_binary(bin_name) and is_integer(index_bit_count) do
    CDT.hll_modify_op(bin_name, @fold, [index_bit_count])
  end

  @doc """
  Returns the estimated cardinality of the HyperLogLog bin.
  """
  @spec get_count(String.t()) :: t()
  def get_count(bin_name) when is_binary(bin_name) do
    CDT.hll_read_op(bin_name, @count, [])
  end

  @doc """
  Returns a HyperLogLog blob containing the union of the bin and `hlls`.
  """
  @spec get_union(String.t(), list(), read_opts()) :: t()
  def get_union(bin_name, hlls, opts \\ [])
      when is_binary(bin_name) and is_list(hlls) and is_list(opts) do
    CDT.hll_read_op(bin_name, @union, [hlls])
  end

  @doc """
  Returns the estimated cardinality of the union of the bin and `hlls`.
  """
  @spec get_union_count(String.t(), list(), read_opts()) :: t()
  def get_union_count(bin_name, hlls, opts \\ [])
      when is_binary(bin_name) and is_list(hlls) and is_list(opts) do
    CDT.hll_read_op(bin_name, @union_count, [hlls])
  end

  @doc """
  Returns the estimated cardinality of the intersection of the bin and `hlls`.
  """
  @spec get_intersect_count(String.t(), list(), read_opts()) :: t()
  def get_intersect_count(bin_name, hlls, opts \\ [])
      when is_binary(bin_name) and is_list(hlls) and is_list(opts) do
    CDT.hll_read_op(bin_name, @intersect_count, [hlls])
  end

  @doc """
  Returns an estimated similarity score for the bin and `hlls`.
  """
  @spec get_similarity(String.t(), list(), read_opts()) :: t()
  def get_similarity(bin_name, hlls, opts \\ [])
      when is_binary(bin_name) and is_list(hlls) and is_list(opts) do
    CDT.hll_read_op(bin_name, @similarity, [hlls])
  end

  @doc """
  Returns `[index_bit_count, min_hash_bit_count]` for the HyperLogLog bin.
  """
  @spec describe(String.t()) :: t()
  def describe(bin_name) when is_binary(bin_name) do
    CDT.hll_read_op(bin_name, @describe, [])
  end

  defp flags(opts), do: Keyword.get(opts, :flags, 0)
end
