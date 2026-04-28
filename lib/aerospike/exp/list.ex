defmodule Aerospike.Exp.List do
  @moduledoc """
  List CDT expression helpers.

  These builders create expressions that evaluate list operations against a
  list expression, usually `Aerospike.Exp.list_bin/1`.
  """

  import Bitwise

  alias Aerospike.Exp
  alias Aerospike.Exp.Module
  alias Aerospike.Op.List, as: ListOp

  @typedoc "Opaque server-side expression."
  @type t :: Exp.t()

  @typedoc """
  List selector return type integer.

  Use the `return_*` helpers in this module. `return_inverted/0` may be OR-ed
  into another return type to invert a selector.
  """
  @type return_type :: non_neg_integer()

  @typedoc """
  List expression write policy map accepted in `opts[:policy]`.

  Append/increment operations expect both `:order` and `:flags`; insert and set
  operations encode only `:flags`.
  """
  @type policy :: %{optional(:order) => non_neg_integer(), required(:flags) => non_neg_integer()}

  @typedoc """
  Common list expression options.

  Supported keys:

  * `:policy` - list write policy for modify expressions that document it.
  * `:return_type` - selector return type from the `return_*` helpers.
  """
  @type opts :: [policy: policy(), return_type: return_type()]

  @append 1
  @append_items 2
  @insert 3
  @insert_items 4
  @set 9
  @clear 11
  @increment 12
  @sort 13
  @size 16
  @get_by_index 19
  @get_by_rank 21
  @get_by_value 22
  @get_by_value_list 23
  @get_by_index_range 24
  @get_by_value_interval 25
  @get_by_rank_range 26
  @get_by_value_rel_rank_range 27
  @remove_by_index 32
  @remove_by_rank 34
  @remove_by_value 35
  @remove_by_value_list 36
  @remove_by_index_range 37
  @remove_by_value_interval 38
  @remove_by_rank_range 39
  @remove_by_value_rel_rank_range 40

  @inverted 0x10000

  @doc "Return no result for a selector expression."
  @spec return_none() :: 0
  def return_none, do: ListOp.return_none()

  @doc "Return selected item indexes."
  @spec return_index() :: 1
  def return_index, do: ListOp.return_index()

  @doc "Return selected reverse indexes, counted from the list end."
  @spec return_reverse_index() :: 2
  def return_reverse_index, do: ListOp.return_reverse_index()

  @doc "Return selected value ranks."
  @spec return_rank() :: 3
  def return_rank, do: ListOp.return_rank()

  @doc "Return selected reverse ranks, counted from the highest value."
  @spec return_reverse_rank() :: 4
  def return_reverse_rank, do: ListOp.return_reverse_rank()

  @doc "Return the selected item count."
  @spec return_count() :: 5
  def return_count, do: ListOp.return_count()

  @doc "Return selected value or values."
  @spec return_value() :: 7
  def return_value, do: ListOp.return_value()

  @doc "Return whether matching values exist."
  @spec return_exists() :: 13
  def return_exists, do: ListOp.return_exists()

  @doc "Invert the selector so it applies outside the matched range."
  @spec return_inverted() :: 0x10000
  def return_inverted, do: @inverted

  @doc "Appends `value` to a list expression. Supports `policy:`."
  @spec append(Exp.t(), Exp.t(), opts()) :: t()
  def append(%Exp{} = bin, %Exp{} = value, opts \\ []) do
    modify(bin, @append, [value | list_policy(opts)])
  end

  @doc "Appends list expression `values` to a list expression. Supports `policy:`."
  @spec append_items(Exp.t(), Exp.t(), opts()) :: t()
  def append_items(%Exp{} = bin, %Exp{} = values, opts \\ []) do
    modify(bin, @append_items, [values | list_policy(opts)])
  end

  @doc "Inserts `value` at `index`. Supports `policy: %{flags: flags}`."
  @spec insert(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def insert(%Exp{} = bin, %Exp{} = index, %Exp{} = value, opts \\ []) do
    modify(bin, @insert, [index, value | flags_policy(opts)])
  end

  @doc "Inserts list expression `values` at `index`. Supports `policy: %{flags: flags}`."
  @spec insert_items(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def insert_items(%Exp{} = bin, %Exp{} = index, %Exp{} = values, opts \\ []) do
    modify(bin, @insert_items, [index, values | flags_policy(opts)])
  end

  @doc "Sets the value at `index`. Supports `policy: %{flags: flags}`."
  @spec set(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def set(%Exp{} = bin, %Exp{} = index, %Exp{} = value, opts \\ []) do
    modify(bin, @set, [index, value | flags_policy(opts)])
  end

  @doc "Removes all values from the list expression."
  @spec clear(Exp.t()) :: t()
  def clear(%Exp{} = bin), do: modify(bin, @clear, [])

  @doc "Increments the numeric value at `index`. Supports `policy:`."
  @spec increment(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def increment(%Exp{} = bin, %Exp{} = index, %Exp{} = value, opts \\ []) do
    modify(bin, @increment, [index, value | list_policy(opts)])
  end

  @doc "Sorts the list expression using `sort_flags`."
  @spec sort(Exp.t(), integer()) :: t()
  def sort(%Exp{} = bin, sort_flags \\ 0) when is_integer(sort_flags) do
    modify(bin, @sort, [sort_flags])
  end

  @doc "Removes values equal to `value`, returning data selected by `return_type:`."
  @spec remove_by_value(Exp.t(), Exp.t(), opts()) :: t()
  def remove_by_value(%Exp{} = bin, %Exp{} = value, opts \\ []) do
    modify(bin, @remove_by_value, [rt(opts), value])
  end

  @doc "Removes values matching any expression in `values`, returning selected data."
  @spec remove_by_value_list(Exp.t(), Exp.t(), opts()) :: t()
  def remove_by_value_list(%Exp{} = bin, %Exp{} = values, opts \\ []) do
    modify(bin, @remove_by_value_list, [rt(opts), values])
  end

  @doc "Removes values in `[begin_value, end_value)`, returning selected data."
  @spec remove_by_value_range(Exp.t(), Exp.t() | nil, Exp.t() | nil, opts()) :: t()
  def remove_by_value_range(%Exp{} = bin, begin_value, end_value, opts \\ []) do
    modify(bin, @remove_by_value_interval, range_args(rt(opts), begin_value, end_value))
  end

  @doc "Removes values nearest to `value` and greater by relative `rank`."
  @spec remove_by_value_rel_rank_range(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def remove_by_value_rel_rank_range(%Exp{} = bin, %Exp{} = value, %Exp{} = rank, opts \\ []) do
    modify(bin, @remove_by_value_rel_rank_range, [rt(opts), value, rank])
  end

  @doc "Removes `count` values nearest to `value` and greater by relative `rank`."
  @spec remove_by_value_rel_rank_range_count(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def remove_by_value_rel_rank_range_count(
        %Exp{} = bin,
        %Exp{} = value,
        %Exp{} = rank,
        %Exp{} = count,
        opts \\ []
      ) do
    modify(bin, @remove_by_value_rel_rank_range, [rt(opts), value, rank, count])
  end

  @doc "Removes the value at `index`."
  @spec remove_by_index(Exp.t(), Exp.t()) :: t()
  def remove_by_index(%Exp{} = bin, %Exp{} = index) do
    modify(bin, @remove_by_index, [return_none(), index])
  end

  @doc "Removes values from `index` through the end, returning selected data."
  @spec remove_by_index_range_from(Exp.t(), Exp.t(), opts()) :: t()
  def remove_by_index_range_from(%Exp{} = bin, %Exp{} = index, opts \\ []) do
    modify(bin, @remove_by_index_range, [rt(opts), index])
  end

  @doc "Removes `count` values from `index`, returning selected data."
  @spec remove_by_index_range(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def remove_by_index_range(%Exp{} = bin, %Exp{} = index, %Exp{} = count, opts \\ []) do
    modify(bin, @remove_by_index_range, [rt(opts), index, count])
  end

  @doc "Removes the value at `rank`."
  @spec remove_by_rank(Exp.t(), Exp.t()) :: t()
  def remove_by_rank(%Exp{} = bin, %Exp{} = rank) do
    modify(bin, @remove_by_rank, [return_none(), rank])
  end

  @doc "Removes values from `rank` through the highest rank, returning selected data."
  @spec remove_by_rank_range_from(Exp.t(), Exp.t(), opts()) :: t()
  def remove_by_rank_range_from(%Exp{} = bin, %Exp{} = rank, opts \\ []) do
    modify(bin, @remove_by_rank_range, [rt(opts), rank])
  end

  @doc "Removes `count` values from `rank`, returning selected data."
  @spec remove_by_rank_range(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def remove_by_rank_range(%Exp{} = bin, %Exp{} = rank, %Exp{} = count, opts \\ []) do
    modify(bin, @remove_by_rank_range, [rt(opts), rank, count])
  end

  @doc "Returns the number of values in the list expression."
  @spec size(Exp.t()) :: t()
  def size(%Exp{} = bin), do: read(bin, :int, @size, [])

  @doc "Returns values equal to `value`, selected by `return_type:`."
  @spec get_by_value(Exp.t(), Exp.t(), opts()) :: t()
  def get_by_value(%Exp{} = bin, %Exp{} = value, opts \\ []) do
    read(bin, list_return_type(opts), @get_by_value, [rt(opts), value])
  end

  @doc "Returns values matching any expression in `values`, selected by `return_type:`."
  @spec get_by_value_list(Exp.t(), Exp.t(), opts()) :: t()
  def get_by_value_list(%Exp{} = bin, %Exp{} = values, opts \\ []) do
    read(bin, list_return_type(opts), @get_by_value_list, [rt(opts), values])
  end

  @doc "Returns values in `[begin_value, end_value)`, selected by `return_type:`."
  @spec get_by_value_range(Exp.t(), Exp.t() | nil, Exp.t() | nil, opts()) :: t()
  def get_by_value_range(%Exp{} = bin, begin_value, end_value, opts \\ []) do
    read(
      bin,
      list_return_type(opts),
      @get_by_value_interval,
      range_args(rt(opts), begin_value, end_value)
    )
  end

  @doc "Returns values nearest to `value` and greater by relative `rank`."
  @spec get_by_value_rel_rank_range(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def get_by_value_rel_rank_range(%Exp{} = bin, %Exp{} = value, %Exp{} = rank, opts \\ []) do
    read(bin, list_return_type(opts), @get_by_value_rel_rank_range, [rt(opts), value, rank])
  end

  @doc "Returns `count` values nearest to `value` and greater by relative `rank`."
  @spec get_by_value_rel_rank_range_count(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def get_by_value_rel_rank_range_count(
        %Exp{} = bin,
        %Exp{} = value,
        %Exp{} = rank,
        %Exp{} = count,
        opts \\ []
      ) do
    read(bin, list_return_type(opts), @get_by_value_rel_rank_range, [rt(opts), value, rank, count])
  end

  @doc "Returns the value at `index`, selected by `return_type:`."
  @spec get_by_index(Exp.t(), Exp.t(), Exp.exp_type(), opts()) :: t()
  def get_by_index(%Exp{} = bin, %Exp{} = index, value_type, opts \\ []) do
    read(bin, single_return_type(opts, value_type), @get_by_index, [rt(opts), index])
  end

  @doc "Returns values from `index` through the end, selected by `return_type:`."
  @spec get_by_index_range_from(Exp.t(), Exp.t(), opts()) :: t()
  def get_by_index_range_from(%Exp{} = bin, %Exp{} = index, opts \\ []) do
    read(bin, list_return_type(opts), @get_by_index_range, [rt(opts), index])
  end

  @doc "Returns `count` values from `index`, selected by `return_type:`."
  @spec get_by_index_range(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def get_by_index_range(%Exp{} = bin, %Exp{} = index, %Exp{} = count, opts \\ []) do
    read(bin, list_return_type(opts), @get_by_index_range, [rt(opts), index, count])
  end

  @doc "Returns the value at `rank`, selected by `return_type:`."
  @spec get_by_rank(Exp.t(), Exp.t(), Exp.exp_type(), opts()) :: t()
  def get_by_rank(%Exp{} = bin, %Exp{} = rank, value_type, opts \\ []) do
    read(bin, single_return_type(opts, value_type), @get_by_rank, [rt(opts), rank])
  end

  @doc "Returns values from `rank` through the highest rank, selected by `return_type:`."
  @spec get_by_rank_range_from(Exp.t(), Exp.t(), opts()) :: t()
  def get_by_rank_range_from(%Exp{} = bin, %Exp{} = rank, opts \\ []) do
    read(bin, list_return_type(opts), @get_by_rank_range, [rt(opts), rank])
  end

  @doc "Returns `count` values from `rank`, selected by `return_type:`."
  @spec get_by_rank_range(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def get_by_rank_range(%Exp{} = bin, %Exp{} = rank, %Exp{} = count, opts \\ []) do
    read(bin, list_return_type(opts), @get_by_rank_range, [rt(opts), rank, count])
  end

  defp read(bin, type, op_code, args), do: Module.cdt_read(bin, type, op_code, args)
  defp modify(bin, op_code, args), do: Module.cdt_modify(bin, :list, op_code, args, :list)
  defp rt(opts), do: Keyword.get(opts, :return_type, return_value())

  defp list_return_type(opts), do: list_return_type_from_rt(rt(opts))

  defp single_return_type(opts, value_type), do: single_return_type_from_rt(rt(opts), value_type)

  defp list_return_type_from_rt(return_type) do
    case return_type &&& bnot(@inverted) do
      value when value in [1, 2, 3, 4, 7] -> :list
      5 -> :int
      13 -> :bool
    end
  end

  defp single_return_type_from_rt(return_type, value_type) do
    case return_type &&& bnot(@inverted) do
      value when value in [1, 2, 3, 4, 5] -> :int
      7 -> value_type
      13 -> :bool
    end
  end

  defp list_policy(opts) do
    case Keyword.get(opts, :policy) do
      nil -> []
      %{order: order, flags: flags} -> [order, flags]
    end
  end

  defp flags_policy(opts) do
    case Keyword.get(opts, :policy) do
      nil -> []
      %{flags: flags} -> [flags]
    end
  end

  defp range_args(return_type, begin_value, nil), do: [return_type, begin_value]
  defp range_args(return_type, begin_value, end_value), do: [return_type, begin_value, end_value]
end
