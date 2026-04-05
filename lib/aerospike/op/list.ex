defmodule Aerospike.Op.List do
  @moduledoc """
  List Collection Data Type (CDT) operations for `Aerospike.operate/4`.

  Each function builds one server-side list operation. Pass optional `ctx:` for nested
  lists (see `Aerospike.Ctx`) and `return_type:` where the protocol requires a
  `ListReturnType` (see `return_none/0`, `return_value/0`, etc.).

  ## Example

      alias Aerospike.Op.List

      Aerospike.operate(conn, key, [
        List.append("tags", "vip"),
        List.remove_by_value("tags", "trial", return_type: List.return_value()),
        List.size("tags")
      ])
  """

  alias Aerospike.Protocol.CDT

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

  defp ctx(opts), do: Keyword.get(opts, :ctx)
  defp rt(opts, default), do: Keyword.get(opts, :return_type, default)

  @doc false
  @spec set_type(String.t(), integer(), integer(), keyword()) :: t()
  def set_type(bin_name, order, flags, opts \\ [])
      when is_binary(bin_name) and is_integer(order) and is_integer(flags) do
    CDT.list_modify_op(bin_name, @set_type, [order, flags], ctx(opts))
  end

  @spec append(String.t(), term(), keyword()) :: t()
  def append(bin_name, value, opts \\ []) when is_binary(bin_name) do
    case Keyword.get(opts, :policy) do
      nil ->
        CDT.list_modify_op(bin_name, @append, [value], ctx(opts))

      %{order: order, flags: flags} ->
        CDT.list_modify_op(bin_name, @append, [value, order, flags], ctx(opts))
    end
  end

  @spec append_items(String.t(), list(), keyword()) :: t()
  def append_items(bin_name, values, opts \\ []) when is_binary(bin_name) and is_list(values) do
    case Keyword.get(opts, :policy) do
      nil ->
        CDT.list_modify_op(bin_name, @append_items, [values], ctx(opts))

      %{order: order, flags: flags} ->
        CDT.list_modify_op(bin_name, @append_items, [values, order, flags], ctx(opts))
    end
  end

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

  @spec pop(String.t(), integer(), keyword()) :: t()
  def pop(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @pop, [index], ctx(opts))
  end

  @spec pop_range(String.t(), integer(), integer(), keyword()) :: t()
  def pop_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_modify_op(bin_name, @pop_range, [index, count], ctx(opts))
  end

  @spec remove(String.t(), integer(), keyword()) :: t()
  def remove(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @remove, [index], ctx(opts))
  end

  @spec remove_range(String.t(), integer(), integer(), keyword()) :: t()
  def remove_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_modify_op(bin_name, @remove_range, [index, count], ctx(opts))
  end

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

  @spec trim(String.t(), integer(), integer(), keyword()) :: t()
  def trim(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_modify_op(bin_name, @trim, [index, count], ctx(opts))
  end

  @spec clear(String.t(), keyword()) :: t()
  def clear(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.list_modify_op(bin_name, @clear, [], ctx(opts))
  end

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

  @spec sort(String.t(), integer(), keyword()) :: t()
  def sort(bin_name, sort_flags \\ 0, opts \\ [])
      when is_binary(bin_name) and is_integer(sort_flags) do
    CDT.list_modify_op(bin_name, @sort, [sort_flags], ctx(opts))
  end

  @spec size(String.t(), keyword()) :: t()
  def size(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.list_read_op(bin_name, @size, [], ctx(opts))
  end

  @spec get(String.t(), integer(), keyword()) :: t()
  def get(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get, [index], ctx(opts))
  end

  @spec get_range(String.t(), integer(), integer(), keyword()) :: t()
  def get_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.list_read_op(bin_name, @get_range, [index, count], ctx(opts))
  end

  @spec get_range_from(String.t(), integer(), keyword()) :: t()
  def get_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get_range, [index], ctx(opts))
  end

  @spec remove_by_index(String.t(), integer(), keyword()) :: t()
  def remove_by_index(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_modify_op(bin_name, @remove_by_index, [rt(opts, return_value()), index], ctx(opts))
  end

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

  @spec remove_by_rank(String.t(), integer(), keyword()) :: t()
  def remove_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    CDT.list_modify_op(bin_name, @remove_by_rank, [rt(opts, return_value()), rank], ctx(opts))
  end

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

  @spec remove_by_value(String.t(), term(), keyword()) :: t()
  def remove_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.list_modify_op(bin_name, @remove_by_value, [rt(opts, return_value()), value], ctx(opts))
  end

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

  @spec remove_by_value_range(String.t(), term(), term(), keyword()) :: t()
  def remove_by_value_range(bin_name, begin_val, end_val, opts \\ []) when is_binary(bin_name) do
    CDT.list_modify_op(
      bin_name,
      @remove_by_value_interval,
      [rt(opts, return_value()), begin_val, end_val],
      ctx(opts)
    )
  end

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

  @spec get_by_index(String.t(), integer(), keyword()) :: t()
  def get_by_index(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get_by_index, [rt(opts, return_value()), index], ctx(opts))
  end

  @spec get_by_index_range_from(String.t(), integer(), keyword()) :: t()
  def get_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get_by_index_range, [rt(opts, return_value()), index], ctx(opts))
  end

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

  @spec get_by_rank(String.t(), integer(), keyword()) :: t()
  def get_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    CDT.list_read_op(bin_name, @get_by_rank, [rt(opts, return_value()), rank], ctx(opts))
  end

  @spec get_by_rank_range_from(String.t(), integer(), keyword()) :: t()
  def get_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.list_read_op(bin_name, @get_by_rank_range, [rt(opts, return_value()), rank], ctx(opts))
  end

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

  @spec get_by_value(String.t(), term(), keyword()) :: t()
  def get_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.list_read_op(bin_name, @get_by_value, [rt(opts, return_value()), value], ctx(opts))
  end

  @spec get_by_value_list(String.t(), list(), keyword()) :: t()
  def get_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    CDT.list_read_op(bin_name, @get_by_value_list, [rt(opts, return_value()), values], ctx(opts))
  end

  @spec get_by_value_range(String.t(), term(), term(), keyword()) :: t()
  def get_by_value_range(bin_name, begin_val, end_val, opts \\ []) when is_binary(bin_name) do
    CDT.list_read_op(
      bin_name,
      @get_by_value_interval,
      [rt(opts, return_value()), begin_val, end_val],
      ctx(opts)
    )
  end

  @spec get_by_value_rel_rank_range(String.t(), term(), integer(), keyword()) :: t()
  def get_by_value_rel_rank_range(bin_name, value, rank, opts \\ []) when is_binary(bin_name) do
    CDT.list_read_op(
      bin_name,
      @get_by_value_rel_rank_range,
      [rt(opts, return_value()), value, rank],
      ctx(opts)
    )
  end

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
