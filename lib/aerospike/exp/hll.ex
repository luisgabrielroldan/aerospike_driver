defmodule Aerospike.Exp.HLL do
  @moduledoc """
  HyperLogLog expression helpers.
  """

  alias Aerospike.Exp
  alias Aerospike.Exp.Module

  @typedoc "Opaque server-side expression."
  @type t :: Exp.t()

  @init 0
  @add 1
  @count 50
  @union 51
  @union_count 52
  @intersect_count 53
  @similarity 54
  @describe 55
  @may_contain 56

  @spec init(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def init(%Exp{} = bin, %Exp{} = index_bit_count, min_hash_bit_count \\ Exp.int(-1), opts \\ []) do
    modify(bin, @init, [index_bit_count, min_hash_bit_count, flags(opts)])
  end

  @spec add(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def add(
        %Exp{} = bin,
        %Exp{} = elements,
        index_bit_count \\ Exp.int(-1),
        min_hash_bit_count \\ Exp.int(-1),
        opts \\ []
      ) do
    modify(bin, @add, [elements, index_bit_count, min_hash_bit_count, flags(opts)])
  end

  @spec get_count(Exp.t()) :: t()
  def get_count(%Exp{} = bin), do: read(bin, :int, @count, [])

  @spec get_union(Exp.t(), Exp.t()) :: t()
  def get_union(%Exp{} = bin, %Exp{} = hlls), do: read(bin, :hll, @union, [hlls])

  @spec get_union_count(Exp.t(), Exp.t()) :: t()
  def get_union_count(%Exp{} = bin, %Exp{} = hlls), do: read(bin, :int, @union_count, [hlls])

  @spec get_intersect_count(Exp.t(), Exp.t()) :: t()
  def get_intersect_count(%Exp{} = bin, %Exp{} = hlls),
    do: read(bin, :int, @intersect_count, [hlls])

  @spec get_similarity(Exp.t(), Exp.t()) :: t()
  def get_similarity(%Exp{} = bin, %Exp{} = hlls), do: read(bin, :float, @similarity, [hlls])

  @spec describe(Exp.t()) :: t()
  def describe(%Exp{} = bin), do: read(bin, :list, @describe, [])

  @spec may_contain(Exp.t(), Exp.t()) :: t()
  def may_contain(%Exp{} = bin, %Exp{} = elements), do: read(bin, :int, @may_contain, [elements])

  defp read(bin, type, op_code, args), do: Module.hll_read(bin, type, op_code, args)
  defp modify(bin, op_code, args), do: Module.hll_modify(bin, op_code, args)
  defp flags(opts), do: Keyword.get(opts, :flags, 0)
end
