defmodule Aerospike.Op.List do
  @moduledoc """
  List Collection Data Type (CDT) operations for `Aerospike.operate/4`.

  Each function builds one server-side list operation. Pass optional `ctx:` for nested
  lists (see `Aerospike.Ctx`) and `return_type:` where the protocol requires a
  `ListReturnType` (see `return_none/0`, `return_value/0`, etc.).

  ## Index and range semantics

  List operations support negative indexing. If the index is negative, the resolved
  index starts backwards from the end of the list. If an index is out of bounds,
  a parameter error will be returned. If a range is partially out of bounds, the
  valid part of the range will be returned.

      Index  0         -> first item
      Index  4         -> fifth item
      Index -1         -> last item
      Index -3         -> third to last item
      Index  1, Count 2 -> second and third items
      Index -3, Count 3 -> last three items

  ## Rank semantics

  Rank is the value-sorted position within the list:

      Rank  0  -> smallest value
      Rank  N  -> Nth smallest value
      Rank -1  -> largest value
      Rank -N  -> Nth largest value

  ## List ordering

  Lists default to unordered (insertion order). Set `policy: %{order: 1, flags: 0}`
  to create an ordered list that maintains sorted order on every write.

  ## Nested CDT operations

  Nested operations are supported via the optional `ctx:` keyword argument.

      bin = [[7,9,5], [1,2,3], [6,5,4,1]]

      # Append 11 to the last nested list
      List.append("bin", 11, ctx: [Aerospike.Ctx.list_index(-1)])
      # bin result = [[7,9,5], [1,2,3], [6,5,4,1,11]]

  ## Return types

  Functions that accept `return_type:` let you control what the server returns:

  - `return_none/0` — no returned value (fastest for write-only operations)
  - `return_index/0` — index offset order
  - `return_reverse_index/0` — reverse index offset from list end
  - `return_rank/0` — value rank order
  - `return_reverse_rank/0` — reverse value rank from highest
  - `return_count/0` — count of items selected
  - `return_value/0` — value(s) for single reads and value lists for range reads
  - `return_exists/0` — existence flag(s) for matched items

  ## Examples

      alias Aerospike.Op.List

      # Tag management
      MyApp.Repo.operate(key, [
        List.append("tags", "vip"),
        List.remove_by_value("tags", "trial", return_type: List.return_none()),
        List.size("tags")
      ])

      # Queue: atomic enqueue + dequeue
      MyApp.Repo.operate(key, [
        List.append("queue", %{"job" => "send_email"}),
        List.pop("queue", 0)
      ])

      # Bounded list: append then keep last 100 entries
      MyApp.Repo.operate(key, [
        List.append("history", new_entry),
        List.trim("history", -100, 100)
      ])

      # Top-3 values by rank
      MyApp.Repo.operate(key, [
        List.get_by_rank_range("scores", -3, 3)
      ])

      # Time-range query on [timestamp, value] pairs
      MyApp.Repo.operate(key, [
        List.get_by_value_range("readings",
          [1_523_474_231_000, nil],
          [1_523_474_234_000, nil])
      ])

  See the [List Patterns guide](list-patterns.md) for real-world usage patterns
  like queues, time series, and bounded lists.
  """

  alias Aerospike.Protocol.CDT

  @typedoc """
  Opaque list-CDT operation for `Aerospike.operate/4`.

  Construct via `Aerospike.Op.List.*` builder functions and pass in an operation list.
  """
  @opaque t :: Aerospike.Op.t()

  # Operation type integers — `official_libs/aerospike-client-go/cdt_list.go`
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

  @doc "ListReturnType: no returned value."
  @spec return_none() :: 0
  def return_none, do: 0

  @doc "ListReturnType: return value(s)."
  @spec return_value() :: 7
  def return_value, do: 7

  @doc "ListReturnType: return count."
  @spec return_count() :: 5
  def return_count, do: 5

  @doc "ListReturnType: return index."
  @spec return_index() :: 1
  def return_index, do: 1

  @doc "ListReturnType: return reverse index."
  @spec return_reverse_index() :: 2
  def return_reverse_index, do: 2

  @doc "ListReturnType: return rank."
  @spec return_rank() :: 3
  def return_rank, do: 3

  @doc "ListReturnType: return reverse rank."
  @spec return_reverse_rank() :: 4
  def return_reverse_rank, do: 4

  @doc "ListReturnType: return exists."
  @spec return_exists() :: 13
  def return_exists, do: 13

  defp ctx(opts), do: Keyword.get(opts, :ctx)
  defp rt(opts, default), do: Keyword.get(opts, :return_type, default)

  @doc false
  @spec set_type(String.t(), integer(), integer(), keyword()) :: t()
  def set_type(bin_name, order, flags, opts \\ [])
      when is_binary(bin_name) and is_integer(order) and is_integer(flags) do
    CDT.list_modify_op(bin_name, @set_type, [order, flags], ctx(opts))
  end

  @doc """
  Appends a single value to the end of the list bin.

  Server returns list size after the append. Pass `policy:` with
  `%{order: order, flags: flags}` to set list ordering and write flags.

  ## Options

  - `ctx:` — nested CDT context (see `Aerospike.Ctx`)
  - `policy:` — `%{order: order, flags: flags}` for list creation attributes
  """
  @spec append(String.t(), term(), keyword()) :: t()
  def append(bin_name, value, opts \\ []) when is_binary(bin_name) do
    case Keyword.get(opts, :policy) do
      nil ->
        CDT.list_modify_op(bin_name, @append, [value], ctx(opts))

      %{order: order, flags: flags} ->
        CDT.list_modify_op(bin_name, @append, [value, order, flags], ctx(opts))
    end
  end

  @doc """
  Appends multiple values to the end of the list bin.

  Server returns list size after the append. Pass `policy:` with
  `%{order: order, flags: flags}` to set list ordering and write flags.

  ## Options

  - `ctx:` — nested CDT context (see `Aerospike.Ctx`)
  - `policy:` — `%{order: order, flags: flags}` for list creation attributes
  """
  @spec append_items(String.t(), list(), keyword()) :: t()
  def append_items(bin_name, values, opts \\ []) when is_binary(bin_name) and is_list(values) do
    case Keyword.get(opts, :policy) do
      nil ->
        CDT.list_modify_op(bin_name, @append_items, [values], ctx(opts))

      %{order: order, flags: flags} ->
        CDT.list_modify_op(bin_name, @append_items, [values, order, flags], ctx(opts))
    end
  end

  @doc """
  Inserts a value at the specified index in the list bin.

  Server returns list size after the insert.

  ## Options

  - `ctx:` — nested CDT context (see `Aerospike.Ctx`)
  - `policy:` — `%{order: order, flags: flags}` for list creation attributes
  """
  @spec insert(String.t(), integer(), term(), keyword()) :: t()
  def insert(bin_name, index, value, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    case Keyword.get(opts, :policy) do
      nil ->
        CDT.list_modify_op(bin_name, @insert, [index, value], ctx(opts))

      %{order: order, flags: flags} ->
        CDT.list_modify_op(bin_name, @insert, [index, value, order, flags], ctx(opts))
    end
  end

  @doc """
  Inserts multiple values at the specified index in the list bin.

  Server returns list size after the insert.

  ## Options

  - `ctx:` — nested CDT context (see `Aerospike.Ctx`)
  - `policy:` — `%{order: order, flags: flags}` for list creation attributes
  """
  @spec insert_items(String.t(), integer(), list(), keyword()) :: t()
  def insert_items(bin_name, index, values, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_list(values) do
    case Keyword.get(opts, :policy) do
      nil ->
        CDT.list_modify_op(bin_name, @insert_items, [index, values], ctx(opts))

      %{order: order, flags: flags} ->
        CDT.list_modify_op(bin_name, @insert_items, [index, values, order, flags], ctx(opts))
    end
  end

  @doc """
  Returns the item at `index` and removes it from the list.

  Supports negative indexing: `0` = first, `-1` = last.
  """
  @spec pop(String.t(), integer(), keyword()) :: t()
  def pop(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @pop, [index], ctx(opts))
  end

  @doc """
  Returns `count` items starting at `index` and removes them from the list.
  """
  @spec pop_range(String.t(), integer(), integer(), keyword()) :: t()
  def pop_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_modify_op(bin_name, @pop_range, [index, count], ctx(opts))
  end

  @doc """
  Removes the item at `index` from the list.

  Server returns the number of items removed. Supports negative indexing.
  """
  @spec remove(String.t(), integer(), keyword()) :: t()
  def remove(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @remove, [index], ctx(opts))
  end

  @doc """
  Removes `count` items starting at `index` from the list.

  Server returns the number of items removed.
  """
  @spec remove_range(String.t(), integer(), integer(), keyword()) :: t()
  def remove_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_modify_op(bin_name, @remove_range, [index, count], ctx(opts))
  end

  @doc """
  Sets the item value at the specified `index` in the list.

  Server does not return a result by default. Pass `policy:` with
  `%{order: order, flags: flags}` to control write behavior.
  """
  @spec set(String.t(), integer(), term(), keyword()) :: t()
  def set(bin_name, index, value, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    case Keyword.get(opts, :policy) do
      nil ->
        CDT.list_modify_op(bin_name, @set, [index, value], ctx(opts))

      %{order: order, flags: flags} ->
        CDT.list_modify_op(bin_name, @set, [index, value, order, flags], ctx(opts))
    end
  end

  @doc """
  Keeps only items in the range `[index, index + count)`, removing all others.

  If the range is out of bounds, all items will be removed.
  Server returns the number of items removed.
  """
  @spec trim(String.t(), integer(), integer(), keyword()) :: t()
  def trim(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_modify_op(bin_name, @trim, [index, count], ctx(opts))
  end

  @doc """
  Removes all items from the list.

  Server does not return a result by default.
  """
  @spec clear(String.t(), keyword()) :: t()
  def clear(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.list_modify_op(bin_name, @clear, [], ctx(opts))
  end

  @doc """
  Increments `list[index]` by `value` and returns the result.

  The value must be integer or float. Pass `policy:` with `%{flags: flags}`
  to control write behavior on the incremented element.
  """
  @spec increment(String.t(), integer(), term(), keyword()) :: t()
  def increment(bin_name, index, value, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    case Keyword.get(opts, :policy) do
      nil ->
        CDT.list_modify_op(bin_name, @increment, [index, value], ctx(opts))

      %{flags: flags} ->
        CDT.list_modify_op(bin_name, @increment, [index, value, flags], ctx(opts))
    end
  end

  @doc """
  Sorts the list according to `sort_flags`.

  Server does not return a result by default. Use `0` for ascending order,
  `1` for descending, `2` to drop duplicates (flags may be OR'd).
  """
  @spec sort(String.t(), integer(), keyword()) :: t()
  def sort(bin_name, sort_flags \\ 0, opts \\ [])
      when is_binary(bin_name) and is_integer(sort_flags) do
    CDT.list_modify_op(bin_name, @sort, [sort_flags], ctx(opts))
  end

  @doc """
  Returns the number of items in the list.
  """
  @spec size(String.t(), keyword()) :: t()
  def size(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.list_read_op(bin_name, @size, [], ctx(opts))
  end

  @doc """
  Returns the item at the specified `index`.

  Supports negative indexing: `0` = first item, `-1` = last item.
  """
  @spec get(String.t(), integer(), keyword()) :: t()
  def get(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get, [index], ctx(opts))
  end

  @doc """
  Returns `count` items starting at the specified `index`.
  """
  @spec get_range(String.t(), integer(), integer(), keyword()) :: t()
  def get_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_read_op(bin_name, @get_range, [index, count], ctx(opts))
  end

  @doc """
  Returns all items starting at the specified `index` through the end of the list.
  """
  @spec get_range_from(String.t(), integer(), keyword()) :: t()
  def get_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get_range, [index], ctx(opts))
  end

  @doc """
  Removes the item at `index` and returns data specified by `return_type:`.

  Defaults to `return_value/0`.
  """
  @spec remove_by_index(String.t(), integer(), keyword()) :: t()
  def remove_by_index(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @remove_by_index, [rt(opts, return_value()), index], ctx(opts))
  end

  @doc """
  Removes all items from `index` to the end of the list.

  Returns data specified by `return_type:` (defaults to `return_value/0`).
  """
  @spec remove_by_index_range_from(String.t(), integer(), keyword()) :: t()
  def remove_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_index_range,
      [rt(opts, return_value()), index],
      ctx(opts)
    )
  end

  @doc """
  Removes `count` items starting at `index`.

  Returns data specified by `return_type:` (defaults to `return_value/0`).
  """
  @spec remove_by_index_range(String.t(), integer(), integer(), keyword()) :: t()
  def remove_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_index_range,
      [rt(opts, return_value()), index, count],
      ctx(opts)
    )
  end

  @doc """
  Removes the item at the given value-sorted `rank`.

  Rank `0` = smallest value, `-1` = largest value.
  Returns data specified by `return_type:` (defaults to `return_value/0`).
  """
  @spec remove_by_rank(String.t(), integer(), keyword()) :: t()
  def remove_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    CDT.list_modify_op(bin_name, @remove_by_rank, [rt(opts, return_value()), rank], ctx(opts))
  end

  @doc """
  Removes all items from the given `rank` through the highest-ranked item.

  Returns data specified by `return_type:` (defaults to `return_value/0`).
  """
  @spec remove_by_rank_range_from(String.t(), integer(), keyword()) :: t()
  def remove_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_rank_range,
      [rt(opts, return_value()), rank],
      ctx(opts)
    )
  end

  @doc """
  Removes `count` items starting at the given value-sorted `rank`.

  Returns data specified by `return_type:` (defaults to `return_value/0`).
  """
  @spec remove_by_rank_range(String.t(), integer(), integer(), keyword()) :: t()
  def remove_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_rank_range,
      [rt(opts, return_value()), rank, count],
      ctx(opts)
    )
  end

  @doc """
  Removes items matching `value` and returns data specified by `return_type:`.

  Defaults to `return_value/0`.
  """
  @spec remove_by_value(String.t(), term(), keyword()) :: t()
  def remove_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.list_modify_op(bin_name, @remove_by_value, [rt(opts, return_value()), value], ctx(opts))
  end

  @doc """
  Removes items matching any value in `values` and returns data specified by `return_type:`.

  Defaults to `return_value/0`.
  """
  @spec remove_by_value_list(String.t(), list(), keyword()) :: t()
  def remove_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_value_list,
      [rt(opts, return_value()), values],
      ctx(opts)
    )
  end

  @doc """
  Removes items in the value range `[begin_val, end_val)` (begin inclusive, end exclusive).

  Pass `nil` for `begin_val` to start from the smallest value, or `nil` for `end_val`
  to include all values greater than or equal to `begin_val`.
  Returns data specified by `return_type:` (defaults to `return_value/0`).
  """
  @spec remove_by_value_range(String.t(), term(), term(), keyword()) :: t()
  def remove_by_value_range(bin_name, begin_val, end_val, opts \\ []) when is_binary(bin_name) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_value_interval,
      [rt(opts, return_value()), begin_val, end_val],
      ctx(opts)
    )
  end

  @doc """
  Removes items nearest to `value` and greater by relative `rank`, with no count limit.

  Returns data specified by `return_type:` (defaults to `return_value/0`).

  ## Examples (ordered list `[0, 4, 5, 9, 11, 15]`)

      {value, rank} -> removed items
      {5,  0}  -> [5, 9, 11, 15]
      {5,  1}  -> [9, 11, 15]
      {5, -1}  -> [4, 5, 9, 11, 15]
      {3,  0}  -> [4, 5, 9, 11, 15]
      {3,  3}  -> [11, 15]
      {3, -3}  -> [0, 4, 5, 9, 11, 15]
  """
  @spec remove_by_value_rel_rank_range(String.t(), term(), integer(), keyword()) :: t()
  def remove_by_value_rel_rank_range(bin_name, value, rank, opts \\ [])
      when is_binary(bin_name) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_value_rel_rank_range,
      [rt(opts, return_value()), value, rank],
      ctx(opts)
    )
  end

  @doc """
  Removes `count` items nearest to `value` and greater by relative `rank`.

  Returns data specified by `return_type:` (defaults to `return_value/0`).

  ## Examples (ordered list `[0, 4, 5, 9, 11, 15]`)

      {value, rank, count} -> removed items
      {5,  0, 2}  -> [5, 9]
      {5,  1, 1}  -> [9]
      {5, -1, 2}  -> [4, 5]
      {3,  0, 1}  -> [4]
      {3,  3, 7}  -> [11, 15]
      {3, -3, 2}  -> []
  """
  @spec remove_by_value_rel_rank_range_count(String.t(), term(), integer(), integer(), keyword()) ::
          t()
  def remove_by_value_rel_rank_range_count(bin_name, value, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_value_rel_rank_range,
      [rt(opts, return_value()), value, rank, count],
      ctx(opts)
    )
  end

  @doc """
  Returns the item at `index`, selected by `return_type:`.

  Supports negative indexing. Defaults to `return_value/0`.
  """
  @spec get_by_index(String.t(), integer(), keyword()) :: t()
  def get_by_index(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get_by_index, [rt(opts, return_value()), index], ctx(opts))
  end

  @doc """
  Returns all items from `index` to the end of the list, selected by `return_type:`.

  Defaults to `return_value/0`.
  """
  @spec get_by_index_range_from(String.t(), integer(), keyword()) :: t()
  def get_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get_by_index_range, [rt(opts, return_value()), index], ctx(opts))
  end

  @doc """
  Returns `count` items starting at `index`, selected by `return_type:`.

  Defaults to `return_value/0`.
  """
  @spec get_by_index_range(String.t(), integer(), integer(), keyword()) :: t()
  def get_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_read_op(
      bin_name,
      @get_by_index_range,
      [rt(opts, return_value()), index, count],
      ctx(opts)
    )
  end

  @doc """
  Returns the item at the given value-sorted `rank`, selected by `return_type:`.

  Rank `0` = smallest value, `-1` = largest value. Defaults to `return_value/0`.
  """
  @spec get_by_rank(String.t(), integer(), keyword()) :: t()
  def get_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    CDT.list_read_op(bin_name, @get_by_rank, [rt(opts, return_value()), rank], ctx(opts))
  end

  @doc """
  Returns all items from `rank` through the highest-ranked item, selected by `return_type:`.

  Defaults to `return_value/0`.
  """
  @spec get_by_rank_range_from(String.t(), integer(), keyword()) :: t()
  def get_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.list_read_op(bin_name, @get_by_rank_range, [rt(opts, return_value()), rank], ctx(opts))
  end

  @doc """
  Returns `count` items starting at the given value-sorted `rank`, selected by `return_type:`.

  Defaults to `return_value/0`.
  """
  @spec get_by_rank_range(String.t(), integer(), integer(), keyword()) :: t()
  def get_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.list_read_op(
      bin_name,
      @get_by_rank_range,
      [rt(opts, return_value()), rank, count],
      ctx(opts)
    )
  end

  @doc """
  Returns items matching `value`, selected by `return_type:`.

  Defaults to `return_value/0`.
  """
  @spec get_by_value(String.t(), term(), keyword()) :: t()
  def get_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.list_read_op(bin_name, @get_by_value, [rt(opts, return_value()), value], ctx(opts))
  end

  @doc """
  Returns items matching any value in `values`, selected by `return_type:`.

  Defaults to `return_value/0`.
  """
  @spec get_by_value_list(String.t(), list(), keyword()) :: t()
  def get_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    CDT.list_read_op(bin_name, @get_by_value_list, [rt(opts, return_value()), values], ctx(opts))
  end

  @doc """
  Returns items in the value range `[begin_val, end_val)` (begin inclusive, end exclusive).

  Pass `nil` for `begin_val` to start from the smallest value, or `nil` for `end_val`
  to include all values greater than or equal to `begin_val`.
  Selected by `return_type:` (defaults to `return_value/0`).
  """
  @spec get_by_value_range(String.t(), term(), term(), keyword()) :: t()
  def get_by_value_range(bin_name, begin_val, end_val, opts \\ []) when is_binary(bin_name) do
    CDT.list_read_op(
      bin_name,
      @get_by_value_interval,
      [rt(opts, return_value()), begin_val, end_val],
      ctx(opts)
    )
  end

  @doc """
  Returns items nearest to `value` and greater by relative `rank`, with no count limit.

  Selected by `return_type:` (defaults to `return_value/0`).

  ## Examples (ordered list `[0, 4, 5, 9, 11, 15]`)

      {value, rank} -> selected items
      {5,  0}  -> [5, 9, 11, 15]
      {5,  1}  -> [9, 11, 15]
      {5, -1}  -> [4, 5, 9, 11, 15]
      {3,  0}  -> [4, 5, 9, 11, 15]
      {3,  3}  -> [11, 15]
      {3, -3}  -> [0, 4, 5, 9, 11, 15]
  """
  @spec get_by_value_rel_rank_range(String.t(), term(), integer(), keyword()) :: t()
  def get_by_value_rel_rank_range(bin_name, value, rank, opts \\ []) when is_binary(bin_name) do
    CDT.list_read_op(
      bin_name,
      @get_by_value_rel_rank_range,
      [rt(opts, return_value()), value, rank],
      ctx(opts)
    )
  end

  @doc """
  Returns `count` items nearest to `value` and greater by relative `rank`.

  Selected by `return_type:` (defaults to `return_value/0`).

  ## Examples (ordered list `[0, 4, 5, 9, 11, 15]`)

      {value, rank, count} -> selected items
      {5,  0, 2}  -> [5, 9]
      {5,  1, 1}  -> [9]
      {5, -1, 2}  -> [4, 5]
      {3,  0, 1}  -> [4]
      {3,  3, 7}  -> [11, 15]
      {3, -3, 2}  -> []
  """
  @spec get_by_value_rel_rank_range_count(String.t(), term(), integer(), integer(), keyword()) ::
          t()
  def get_by_value_rel_rank_range_count(bin_name, value, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.list_read_op(
      bin_name,
      @get_by_value_rel_rank_range,
      [rt(opts, return_value()), value, rank, count],
      ctx(opts)
    )
  end
end
