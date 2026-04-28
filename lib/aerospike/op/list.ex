defmodule Aerospike.Op.List do
  @moduledoc """
  List CDT operations for `Aerospike.operate/4`.

  These builders create server-side list operations. Pass `ctx:` for nested CDT
  paths and `return_type:` for selector operations that can return indexes,
  ranks, counts, values, or existence flags.
  """

  alias Aerospike.Protocol.CDT

  @typedoc """
  Opaque list CDT operation for `Aerospike.operate/4`.
  """
  @opaque t :: Aerospike.Op.t()

  @typedoc """
  List order attribute integer.

  Use `order_unordered/0` or `order_ordered/0` to build this value. Some
  server operations also accept this value OR-ed with persist-index flags.
  """
  @type order :: non_neg_integer()

  @typedoc """
  List write policy flags integer.

  Use `write_default/0`, `write_add_unique/0`, `write_insert_bounded/0`,
  `write_no_fail/0`, and `write_partial/0`, combined with `Bitwise.bor/2`
  when more than one flag is needed.
  """
  @type write_flags :: non_neg_integer()

  @typedoc """
  List selector return type integer.

  Use the `return_*` helpers in this module. `return_inverted/0` may be OR-ed
  into another return type to invert a selector.
  """
  @type return_type :: non_neg_integer()

  @typedoc """
  List write policy map accepted in `opts[:policy]`.

  Append, insert, and set operations expect both `:order` and `:flags`;
  increment only reads `:flags`.
  """
  @type policy :: %{optional(:order) => order(), required(:flags) => write_flags()}

  @typedoc """
  Common list operation options.

  Supported keys:

  * `:ctx` - nested CDT context path from `Aerospike.Ctx`.
  * `:return_type` - selector return type from the `return_*` helpers.
  * `:policy` - list write policy map for write operations that document it.
  * `:persist_index` - persist the index for top-level ordered lists.
  * `:pad` - allow nested list creation past the current boundary.
  """
  @type opts :: [
          ctx: Aerospike.Ctx.t(),
          return_type: return_type(),
          policy: policy(),
          persist_index: boolean(),
          pad: boolean()
        ]

  @set_type 0
  @append 1
  @append_items 2
  @insert 3
  @insert_items 4
  @pop 5
  @pop_range 6
  @remove 7
  @remove_range 8
  @set 9
  @trim 10
  @clear 11
  @increment 12
  @sort 13
  @size 16
  @get 17
  @get_range 18
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
  @persist_index 0x10

  @doc "Create unordered lists by default."
  @spec order_unordered() :: 0
  def order_unordered, do: 0

  @doc "Create ordered lists."
  @spec order_ordered() :: 1
  def order_ordered, do: 1

  @doc "Use default list write behavior."
  @spec write_default() :: 0
  def write_default, do: 0

  @doc "Only add unique list values."
  @spec write_add_unique() :: 1
  def write_add_unique, do: 1

  @doc "Require list insert indexes to be inside list boundaries."
  @spec write_insert_bounded() :: 2
  def write_insert_bounded, do: 2

  @doc "Do not fail the command when a list item is rejected by write flags."
  @spec write_no_fail() :: 4
  def write_no_fail, do: 4

  @doc "Commit valid list items even when another item is rejected by write flags."
  @spec write_partial() :: 8
  def write_partial, do: 8

  @doc "Return no result for a selector operation."
  @spec return_none() :: 0
  def return_none, do: 0

  @doc "Return selected item indexes."
  @spec return_index() :: 1
  def return_index, do: 1

  @doc "Return selected reverse indexes, counted from the list end."
  @spec return_reverse_index() :: 2
  def return_reverse_index, do: 2

  @doc "Return selected value ranks."
  @spec return_rank() :: 3
  def return_rank, do: 3

  @doc "Return selected reverse ranks, counted from the highest value."
  @spec return_reverse_rank() :: 4
  def return_reverse_rank, do: 4

  @doc "Return the selected item count."
  @spec return_count() :: 5
  def return_count, do: 5

  @doc "Return selected value or values."
  @spec return_value() :: 7
  def return_value, do: 7

  @doc "Return whether matching items exist."
  @spec return_exists() :: 13
  def return_exists, do: 13

  @doc "Invert the selector so it applies outside the matched range."
  @spec return_inverted() :: 0x10000
  def return_inverted, do: @inverted

  @doc """
  Sets the list order and write flags for a list bin.
  """
  @spec set_type(String.t(), integer(), integer(), opts()) :: t()
  def set_type(bin_name, order, flags, opts \\ [])
      when is_binary(bin_name) and is_integer(order) and is_integer(flags) do
    CDT.list_modify_op(bin_name, @set_type, [order, flags], ctx(opts))
  end

  @doc """
  Creates a list at the selected context level.

  When `ctx:` is omitted, this sets the top-level bin list order. For nested
  lists, pass `pad: true` to allow create indexes beyond the current boundary.
  """
  @spec create(String.t(), integer(), opts()) :: t()
  def create(bin_name, order, opts \\ [])
      when is_binary(bin_name) and is_integer(order) do
    case ctx(opts) do
      steps when is_list(steps) and steps != [] ->
        CDT.list_create_op(
          bin_name,
          list_order_flag(order, Keyword.get(opts, :pad, false)),
          [
            @set_type,
            order
          ],
          steps
        )

      _ ->
        set_order(bin_name, order, opts)
    end
  end

  @doc """
  Sets the list order.

  Pass `persist_index: true` to persist an index for a top-level ordered list.
  """
  @spec set_order(String.t(), integer(), opts()) :: t()
  def set_order(bin_name, order, opts \\ [])
      when is_binary(bin_name) and is_integer(order) do
    CDT.list_modify_op(bin_name, @set_type, [order_attr(order, opts)], ctx(opts))
  end

  @doc """
  Appends a value to a list bin.

  Pass `policy: %{order: order, flags: flags}` to include list order and write
  flags in the operation payload.
  """
  @spec append(String.t(), term(), opts()) :: t()
  def append(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.list_modify_op(bin_name, @append, [value | list_policy(opts)], ctx(opts))
  end

  @doc """
  Appends multiple values to a list bin.

  Pass `policy: %{order: order, flags: flags}` to include list order and write
  flags in the operation payload.
  """
  @spec append_items(String.t(), list(), opts()) :: t()
  def append_items(bin_name, values, opts \\ []) when is_binary(bin_name) and is_list(values) do
    CDT.list_modify_op(bin_name, @append_items, [values | list_policy(opts)], ctx(opts))
  end

  @doc """
  Inserts a value at `index`.
  """
  @spec insert(String.t(), integer(), term(), opts()) :: t()
  def insert(bin_name, index, value, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @insert, [index, value | list_policy(opts)], ctx(opts))
  end

  @doc """
  Inserts multiple values at `index`.
  """
  @spec insert_items(String.t(), integer(), list(), opts()) :: t()
  def insert_items(bin_name, index, values, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_list(values) do
    CDT.list_modify_op(bin_name, @insert_items, [index, values | list_policy(opts)], ctx(opts))
  end

  @doc """
  Removes and returns the value at `index`.
  """
  @spec pop(String.t(), integer(), opts()) :: t()
  def pop(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @pop, [index], ctx(opts))
  end

  @doc """
  Removes and returns `count` values starting at `index`.
  """
  @spec pop_range(String.t(), integer(), integer(), opts()) :: t()
  def pop_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_modify_op(bin_name, @pop_range, [index, count], ctx(opts))
  end

  @doc """
  Removes and returns values from `index` through the end of the list.
  """
  @spec pop_range_from(String.t(), integer(), opts()) :: t()
  def pop_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @pop_range, [index], ctx(opts))
  end

  @doc """
  Removes the value at `index` and returns the removed count.
  """
  @spec remove(String.t(), integer(), opts()) :: t()
  def remove(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @remove, [index], ctx(opts))
  end

  @doc """
  Removes `count` values starting at `index` and returns the removed count.
  """
  @spec remove_range(String.t(), integer(), integer(), opts()) :: t()
  def remove_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_modify_op(bin_name, @remove_range, [index, count], ctx(opts))
  end

  @doc """
  Removes values from `index` through the end and returns the removed count.
  """
  @spec remove_range_from(String.t(), integer(), opts()) :: t()
  def remove_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @remove_range, [index], ctx(opts))
  end

  @doc """
  Sets the value at `index`.

  Pass `policy: %{order: order, flags: flags}` to include list order and write
  flags in the operation payload.
  """
  @spec set(String.t(), integer(), term(), opts()) :: t()
  def set(bin_name, index, value, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @set, [index, value | list_policy(opts)], ctx(opts))
  end

  @doc """
  Keeps `count` values starting at `index` and removes all others.
  """
  @spec trim(String.t(), integer(), integer(), opts()) :: t()
  def trim(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_modify_op(bin_name, @trim, [index, count], ctx(opts))
  end

  @doc """
  Removes all values from the list.
  """
  @spec clear(String.t(), opts()) :: t()
  def clear(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.list_modify_op(bin_name, @clear, [], ctx(opts))
  end

  @doc """
  Increments the numeric value at `index`.

  Pass `policy: %{flags: flags}` to include write flags in the operation
  payload.
  """
  @spec increment(String.t(), integer(), term(), opts()) :: t()
  def increment(bin_name, index, value, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @increment, [index, value | increment_policy(opts)], ctx(opts))
  end

  @doc """
  Sorts the list with `sort_flags`.
  """
  @spec sort(String.t(), integer(), opts()) :: t()
  def sort(bin_name, sort_flags \\ 0, opts \\ [])
      when is_binary(bin_name) and is_integer(sort_flags) do
    CDT.list_modify_op(bin_name, @sort, [sort_flags], ctx(opts))
  end

  @doc """
  Returns the number of values in the list.
  """
  @spec size(String.t(), opts()) :: t()
  def size(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.list_read_op(bin_name, @size, [], ctx(opts))
  end

  @doc """
  Returns the value at `index`.
  """
  @spec get(String.t(), integer(), opts()) :: t()
  def get(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get, [index], ctx(opts))
  end

  @doc """
  Returns `count` values starting at `index`.
  """
  @spec get_range(String.t(), integer(), integer(), opts()) :: t()
  def get_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_read_op(bin_name, @get_range, [index, count], ctx(opts))
  end

  @doc """
  Returns values from `index` through the end of the list.
  """
  @spec get_range_from(String.t(), integer(), opts()) :: t()
  def get_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get_range, [index], ctx(opts))
  end

  @doc """
  Removes the value at `index`, returning data selected by `return_type:`.
  """
  @spec remove_by_index(String.t(), integer(), opts()) :: t()
  def remove_by_index(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @remove_by_index, [rt(opts), index], ctx(opts))
  end

  @doc """
  Removes values from `index` through the end, returning selected data.
  """
  @spec remove_by_index_range_from(String.t(), integer(), opts()) :: t()
  def remove_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @remove_by_index_range, [rt(opts), index], ctx(opts))
  end

  @doc """
  Removes `count` values from `index`, returning selected data.
  """
  @spec remove_by_index_range(String.t(), integer(), integer(), opts()) :: t()
  def remove_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_modify_op(bin_name, @remove_by_index_range, [rt(opts), index, count], ctx(opts))
  end

  @doc """
  Removes the value at `rank`, returning data selected by `return_type:`.
  """
  @spec remove_by_rank(String.t(), integer(), opts()) :: t()
  def remove_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    CDT.list_modify_op(bin_name, @remove_by_rank, [rt(opts), rank], ctx(opts))
  end

  @doc """
  Removes values from `rank` through the highest rank, returning selected data.
  """
  @spec remove_by_rank_range_from(String.t(), integer(), opts()) :: t()
  def remove_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.list_modify_op(bin_name, @remove_by_rank_range, [rt(opts), rank], ctx(opts))
  end

  @doc """
  Removes `count` values from `rank`, returning selected data.
  """
  @spec remove_by_rank_range(String.t(), integer(), integer(), opts()) :: t()
  def remove_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.list_modify_op(bin_name, @remove_by_rank_range, [rt(opts), rank, count], ctx(opts))
  end

  @doc """
  Removes values equal to `value`, returning data selected by `return_type:`.
  """
  @spec remove_by_value(String.t(), term(), opts()) :: t()
  def remove_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.list_modify_op(bin_name, @remove_by_value, [rt(opts), value], ctx(opts))
  end

  @doc """
  Removes values matching any entry in `values`, returning selected data.
  """
  @spec remove_by_value_list(String.t(), list(), opts()) :: t()
  def remove_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    CDT.list_modify_op(bin_name, @remove_by_value_list, [rt(opts), values], ctx(opts))
  end

  @doc """
  Removes values in `[begin_value, end_value)`, returning selected data.

  Pass `nil` for an open range boundary.
  """
  @spec remove_by_value_range(String.t(), term(), term(), opts()) :: t()
  def remove_by_value_range(bin_name, begin_value, end_value, opts \\ [])
      when is_binary(bin_name) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_value_interval,
      [rt(opts), begin_value, end_value],
      ctx(opts)
    )
  end

  @doc """
  Removes values nearest to `value` and greater by relative `rank`.
  """
  @spec remove_by_value_rel_rank_range(String.t(), term(), integer(), opts()) :: t()
  def remove_by_value_rel_rank_range(bin_name, value, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_value_rel_rank_range,
      [rt(opts), value, rank],
      ctx(opts)
    )
  end

  @doc """
  Removes `count` values nearest to `value` and greater by relative `rank`.
  """
  @spec remove_by_value_rel_rank_range_count(String.t(), term(), integer(), integer(), opts()) ::
          t()
  def remove_by_value_rel_rank_range_count(bin_name, value, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_value_rel_rank_range,
      [rt(opts), value, rank, count],
      ctx(opts)
    )
  end

  @doc """
  Returns the value at `index`, selected by `return_type:`.
  """
  @spec get_by_index(String.t(), integer(), opts()) :: t()
  def get_by_index(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get_by_index, [rt(opts), index], ctx(opts))
  end

  @doc """
  Returns values from `index` through the end, selected by `return_type:`.
  """
  @spec get_by_index_range_from(String.t(), integer(), opts()) :: t()
  def get_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get_by_index_range, [rt(opts), index], ctx(opts))
  end

  @doc """
  Returns `count` values from `index`, selected by `return_type:`.
  """
  @spec get_by_index_range(String.t(), integer(), integer(), opts()) :: t()
  def get_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_read_op(bin_name, @get_by_index_range, [rt(opts), index, count], ctx(opts))
  end

  @doc """
  Returns the value at `rank`, selected by `return_type:`.
  """
  @spec get_by_rank(String.t(), integer(), opts()) :: t()
  def get_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    CDT.list_read_op(bin_name, @get_by_rank, [rt(opts), rank], ctx(opts))
  end

  @doc """
  Returns values from `rank` through the highest rank, selected by `return_type:`.
  """
  @spec get_by_rank_range_from(String.t(), integer(), opts()) :: t()
  def get_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.list_read_op(bin_name, @get_by_rank_range, [rt(opts), rank], ctx(opts))
  end

  @doc """
  Returns `count` values from `rank`, selected by `return_type:`.
  """
  @spec get_by_rank_range(String.t(), integer(), integer(), opts()) :: t()
  def get_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.list_read_op(bin_name, @get_by_rank_range, [rt(opts), rank, count], ctx(opts))
  end

  @doc """
  Returns values equal to `value`, selected by `return_type:`.
  """
  @spec get_by_value(String.t(), term(), opts()) :: t()
  def get_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.list_read_op(bin_name, @get_by_value, [rt(opts), value], ctx(opts))
  end

  @doc """
  Returns values matching any entry in `values`, selected by `return_type:`.
  """
  @spec get_by_value_list(String.t(), list(), opts()) :: t()
  def get_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    CDT.list_read_op(bin_name, @get_by_value_list, [rt(opts), values], ctx(opts))
  end

  @doc """
  Returns values in `[begin_value, end_value)`, selected by `return_type:`.

  Pass `nil` for an open range boundary.
  """
  @spec get_by_value_range(String.t(), term(), term(), opts()) :: t()
  def get_by_value_range(bin_name, begin_value, end_value, opts \\ [])
      when is_binary(bin_name) do
    CDT.list_read_op(
      bin_name,
      @get_by_value_interval,
      [rt(opts), begin_value, end_value],
      ctx(opts)
    )
  end

  @doc """
  Returns values nearest to `value` and greater by relative `rank`.
  """
  @spec get_by_value_rel_rank_range(String.t(), term(), integer(), opts()) :: t()
  def get_by_value_rel_rank_range(bin_name, value, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.list_read_op(
      bin_name,
      @get_by_value_rel_rank_range,
      [rt(opts), value, rank],
      ctx(opts)
    )
  end

  @doc """
  Returns `count` values nearest to `value` and greater by relative `rank`.
  """
  @spec get_by_value_rel_rank_range_count(String.t(), term(), integer(), integer(), opts()) ::
          t()
  def get_by_value_rel_rank_range_count(bin_name, value, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.list_read_op(
      bin_name,
      @get_by_value_rel_rank_range,
      [rt(opts), value, rank, count],
      ctx(opts)
    )
  end

  defp ctx(opts), do: Keyword.get(opts, :ctx)
  defp rt(opts), do: Keyword.get(opts, :return_type, return_value())

  defp order_attr(order, opts) do
    case Keyword.get(opts, :persist_index, false) do
      true -> Bitwise.bor(order, @persist_index)
      false -> order
    end
  end

  defp list_order_flag(1, _pad), do: 0xC0
  defp list_order_flag(_order, true), do: 0x80
  defp list_order_flag(_order, false), do: 0x40

  defp list_policy(opts) do
    case Keyword.get(opts, :policy) do
      nil -> []
      %{order: order, flags: flags} -> [order, flags]
    end
  end

  defp increment_policy(opts) do
    case Keyword.get(opts, :policy) do
      nil -> []
      %{flags: flags} -> [flags]
    end
  end
end
