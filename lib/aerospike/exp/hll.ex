defmodule Aerospike.Exp.HLL do
  @moduledoc """
  HyperLogLog expression helpers.
  """

  alias Aerospike.Exp
  alias Aerospike.Exp.Module

  @typedoc "Opaque server-side expression."
  @type t :: Exp.t()

  @typedoc """
  HyperLogLog expression modify options.

  Supported key:

  * `:flags` - raw server HLL write flags. Defaults to `0`.
  """
  @type opts :: [flags: non_neg_integer()]

  @init 0
  @add 1
  @count 50
  @union 51
  @union_count 52
  @intersect_count 53
  @similarity 54
  @describe 55
  @may_contain 56

  @doc """
  Creates or resets an HLL expression value.

  `index_bit_count` and `min_hash_bit_count` default to `Exp.int(-1)`, asking
  the server to use its defaults. Supports `flags:`.
  """
  @spec init(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def init(%Exp{} = bin, %Exp{} = index_bit_count, min_hash_bit_count \\ Exp.int(-1), opts \\ []) do
    modify(bin, @init, [index_bit_count, min_hash_bit_count, flags(opts)])
  end

  @doc """
  Adds element expressions to an HLL expression value.

  `elements` must evaluate to a list. Supports `flags:`.
  """
  @spec add(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def add(
        %Exp{} = bin,
        %Exp{} = elements,
        index_bit_count \\ Exp.int(-1),
        min_hash_bit_count \\ Exp.int(-1),
        opts \\ []
      ) do
    modify(bin, @add, [elements, index_bit_count, min_hash_bit_count, flags(opts)])
  end

  @doc "Returns the estimated cardinality."
  @spec get_count(Exp.t()) :: t()
  def get_count(%Exp{} = bin), do: read(bin, :int, @count, [])

  @doc "Returns an HLL expression containing the union of `bin` and `hlls`."
  @spec get_union(Exp.t(), Exp.t()) :: t()
  def get_union(%Exp{} = bin, %Exp{} = hlls), do: read(bin, :hll, @union, [hlls])

  @doc "Returns the estimated cardinality of the union of `bin` and `hlls`."
  @spec get_union_count(Exp.t(), Exp.t()) :: t()
  def get_union_count(%Exp{} = bin, %Exp{} = hlls), do: read(bin, :int, @union_count, [hlls])

  @doc "Returns the estimated cardinality of the intersection of `bin` and `hlls`."
  @spec get_intersect_count(Exp.t(), Exp.t()) :: t()
  def get_intersect_count(%Exp{} = bin, %Exp{} = hlls),
    do: read(bin, :int, @intersect_count, [hlls])

  @doc "Returns an estimated similarity score for `bin` and `hlls`."
  @spec get_similarity(Exp.t(), Exp.t()) :: t()
  def get_similarity(%Exp{} = bin, %Exp{} = hlls), do: read(bin, :float, @similarity, [hlls])

  @doc "Returns `[index_bit_count, min_hash_bit_count]` for the HLL value."
  @spec describe(Exp.t()) :: t()
  def describe(%Exp{} = bin), do: read(bin, :list, @describe, [])

  @doc "Returns whether the HLL value may contain every element in `elements`."
  @spec may_contain(Exp.t(), Exp.t()) :: t()
  def may_contain(%Exp{} = bin, %Exp{} = elements), do: read(bin, :int, @may_contain, [elements])

  defp read(bin, type, op_code, args), do: Module.hll_read(bin, type, op_code, args)
  defp modify(bin, op_code, args), do: Module.hll_modify(bin, op_code, args)
  defp flags(opts), do: Keyword.get(opts, :flags, 0)
end
