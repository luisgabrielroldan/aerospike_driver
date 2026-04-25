defmodule Aerospike.Op.Map do
  @moduledoc """
  Map CDT operations for `Aerospike.operate/4`.

  These builders create server-side map operations. Pass `ctx:` for nested CDT
  paths and `return_type:` for selector operations that can return keys,
  values, key/value pairs, counts, ranks, indexes, or existence flags.
  """

  alias Aerospike.Protocol.CDT

  @typedoc """
  Opaque map CDT operation for `Aerospike.operate/4`.
  """
  @opaque t :: Aerospike.Op.t()

  @set_policy 64
  @put 67
  @put_items 68
  @increment 73
  @decrement 74
  @clear 75
  @remove_by_key 76
  @remove_by_index 77
  @remove_by_rank 79
  @remove_by_key_list 81
  @remove_by_value 82
  @remove_by_value_list 83
  @remove_by_key_range 84
  @remove_by_index_range 85
  @remove_by_value_range 86
  @remove_by_rank_range 87
  @size 96
  @get_by_key 97
  @get_by_index 98
  @get_by_rank 100
  @get_by_value 102
  @get_by_key_range 103
  @get_by_index_range 104
  @get_by_value_range 105
  @get_by_rank_range 106
  @get_by_key_list 107
  @get_by_value_list 108

  @doc "Return no result for a selector operation."
  @spec return_none() :: 0
  def return_none, do: 0

  @doc "Return selected item indexes."
  @spec return_index() :: 1
  def return_index, do: 1

  @doc "Return selected reverse indexes, counted from the map end."
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

  @doc "Return selected keys."
  @spec return_key() :: 6
  def return_key, do: 6

  @doc "Return selected values."
  @spec return_value() :: 7
  def return_value, do: 7

  @doc "Return selected key/value pairs."
  @spec return_key_value() :: 8
  def return_key_value, do: 8

  @doc "Return whether matching entries exist."
  @spec return_exists() :: 13
  def return_exists, do: 13

  @doc """
  Sets the map order attributes.
  """
  @spec set_policy(String.t(), integer(), keyword()) :: t()
  def set_policy(bin_name, attributes, opts \\ [])
      when is_binary(bin_name) and is_integer(attributes) do
    CDT.map_modify_op(bin_name, @set_policy, [attributes], ctx(opts))
  end

  @doc """
  Writes one key/value pair and returns the map size.

  Pass `policy: %{attr: attr, flags: flags}` to include map order attributes
  and write flags in the operation payload.
  """
  @spec put(String.t(), term(), term(), keyword()) :: t()
  def put(bin_name, map_key, value, opts \\ []) when is_binary(bin_name) do
    %{attr: attr, flags: flags} = map_policy(opts)
    CDT.map_modify_op(bin_name, @put, [map_key, value, attr | write_flags(flags)], ctx(opts))
  end

  @doc """
  Writes multiple key/value pairs and returns the map size.

  Pass `policy: %{attr: attr, flags: flags}` to include map order attributes
  and write flags in the operation payload.
  """
  @spec put_items(String.t(), map(), keyword()) :: t()
  def put_items(bin_name, values, opts \\ []) when is_binary(bin_name) and is_map(values) do
    %{attr: attr, flags: flags} = map_policy(opts)
    CDT.map_modify_op(bin_name, @put_items, [values, attr | write_flags(flags)], ctx(opts))
  end

  @doc """
  Increments the numeric value at `map_key` and returns the final value.
  """
  @spec increment(String.t(), term(), term(), keyword()) :: t()
  def increment(bin_name, map_key, delta, opts \\ [])
      when is_binary(bin_name) do
    %{attr: attr} = map_policy(opts)
    CDT.map_modify_op(bin_name, @increment, [map_key, delta, attr], ctx(opts))
  end

  @doc """
  Decrements the numeric value at `map_key` and returns the final value.
  """
  @spec decrement(String.t(), term(), term(), keyword()) :: t()
  def decrement(bin_name, map_key, delta, opts \\ [])
      when is_binary(bin_name) do
    %{attr: attr} = map_policy(opts)
    CDT.map_modify_op(bin_name, @decrement, [map_key, delta, attr], ctx(opts))
  end

  @doc """
  Removes all entries from the map.
  """
  @spec clear(String.t(), keyword()) :: t()
  def clear(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.map_modify_op(bin_name, @clear, [], ctx(opts))
  end

  @doc """
  Removes the entry for `map_key`, returning data selected by `return_type:`.
  """
  @spec remove_by_key(String.t(), term(), keyword()) :: t()
  def remove_by_key(bin_name, map_key, opts \\ []) when is_binary(bin_name) do
    CDT.map_modify_op(bin_name, @remove_by_key, [key_rt(opts), map_key], ctx(opts))
  end

  @doc """
  Removes entries matching any key in `keys`, returning selected data.
  """
  @spec remove_by_key_list(String.t(), list(), keyword()) :: t()
  def remove_by_key_list(bin_name, keys, opts \\ []) when is_binary(bin_name) and is_list(keys) do
    CDT.map_modify_op(bin_name, @remove_by_key_list, [key_rt(opts), keys], ctx(opts))
  end

  @doc """
  Removes entries with keys in `[begin_key, end_key)`, returning selected data.

  Pass `nil` for an open range boundary.
  """
  @spec remove_by_key_range(String.t(), term(), term(), keyword()) :: t()
  def remove_by_key_range(bin_name, begin_key, end_key, opts \\ []) when is_binary(bin_name) do
    CDT.map_modify_op(
      bin_name,
      @remove_by_key_range,
      [key_rt(opts), begin_key, end_key],
      ctx(opts)
    )
  end

  @doc """
  Removes entries equal to `value`, returning data selected by `return_type:`.
  """
  @spec remove_by_value(String.t(), term(), keyword()) :: t()
  def remove_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.map_modify_op(bin_name, @remove_by_value, [value_rt(opts), value], ctx(opts))
  end

  @doc """
  Removes entries matching any value in `values`, returning selected data.
  """
  @spec remove_by_value_list(String.t(), list(), keyword()) :: t()
  def remove_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    CDT.map_modify_op(bin_name, @remove_by_value_list, [value_rt(opts), values], ctx(opts))
  end

  @doc """
  Removes entries with values in `[begin_value, end_value)`, returning selected data.

  Pass `nil` for an open range boundary.
  """
  @spec remove_by_value_range(String.t(), term(), term(), keyword()) :: t()
  def remove_by_value_range(bin_name, begin_value, end_value, opts \\ [])
      when is_binary(bin_name) do
    CDT.map_modify_op(
      bin_name,
      @remove_by_value_range,
      [value_rt(opts), begin_value, end_value],
      ctx(opts)
    )
  end

  @doc """
  Removes the entry at `index`, returning data selected by `return_type:`.
  """
  @spec remove_by_index(String.t(), integer(), keyword()) :: t()
  def remove_by_index(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.map_modify_op(bin_name, @remove_by_index, [key_rt(opts), index], ctx(opts))
  end

  @doc """
  Removes entries from `index` through the end, returning selected data.
  """
  @spec remove_by_index_range_from(String.t(), integer(), keyword()) :: t()
  def remove_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.map_modify_op(bin_name, @remove_by_index_range, [key_rt(opts), index], ctx(opts))
  end

  @doc """
  Removes `count` entries from `index`, returning selected data.
  """
  @spec remove_by_index_range(String.t(), integer(), integer(), keyword()) :: t()
  def remove_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.map_modify_op(bin_name, @remove_by_index_range, [key_rt(opts), index, count], ctx(opts))
  end

  @doc """
  Removes the entry at `rank`, returning data selected by `return_type:`.
  """
  @spec remove_by_rank(String.t(), integer(), keyword()) :: t()
  def remove_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    CDT.map_modify_op(bin_name, @remove_by_rank, [value_rt(opts), rank], ctx(opts))
  end

  @doc """
  Removes entries from `rank` through the highest rank, returning selected data.
  """
  @spec remove_by_rank_range_from(String.t(), integer(), keyword()) :: t()
  def remove_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.map_modify_op(bin_name, @remove_by_rank_range, [value_rt(opts), rank], ctx(opts))
  end

  @doc """
  Removes `count` entries from `rank`, returning selected data.
  """
  @spec remove_by_rank_range(String.t(), integer(), integer(), keyword()) :: t()
  def remove_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.map_modify_op(bin_name, @remove_by_rank_range, [value_rt(opts), rank, count], ctx(opts))
  end

  @doc """
  Returns the number of entries in the map.
  """
  @spec size(String.t(), keyword()) :: t()
  def size(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.map_read_op(bin_name, @size, [], ctx(opts))
  end

  @doc """
  Returns the entry for `map_key`, selected by `return_type:`.
  """
  @spec get_by_key(String.t(), term(), keyword()) :: t()
  def get_by_key(bin_name, map_key, opts \\ []) when is_binary(bin_name) do
    CDT.map_read_op(bin_name, @get_by_key, [value_rt(opts), map_key], ctx(opts))
  end

  @doc """
  Returns entries matching any key in `keys`, selected by `return_type:`.
  """
  @spec get_by_key_list(String.t(), list(), keyword()) :: t()
  def get_by_key_list(bin_name, keys, opts \\ []) when is_binary(bin_name) and is_list(keys) do
    CDT.map_read_op(bin_name, @get_by_key_list, [key_value_rt(opts), keys], ctx(opts))
  end

  @doc """
  Returns entries with keys in `[begin_key, end_key)`, selected by `return_type:`.

  Pass `nil` for an open range boundary.
  """
  @spec get_by_key_range(String.t(), term(), term(), keyword()) :: t()
  def get_by_key_range(bin_name, begin_key, end_key, opts \\ []) when is_binary(bin_name) do
    CDT.map_read_op(
      bin_name,
      @get_by_key_range,
      [key_value_rt(opts), begin_key, end_key],
      ctx(opts)
    )
  end

  @doc """
  Returns entries equal to `value`, selected by `return_type:`.
  """
  @spec get_by_value(String.t(), term(), keyword()) :: t()
  def get_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.map_read_op(bin_name, @get_by_value, [key_value_rt(opts), value], ctx(opts))
  end

  @doc """
  Returns entries matching any value in `values`, selected by `return_type:`.
  """
  @spec get_by_value_list(String.t(), list(), keyword()) :: t()
  def get_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    CDT.map_read_op(bin_name, @get_by_value_list, [key_value_rt(opts), values], ctx(opts))
  end

  @doc """
  Returns entries with values in `[begin_value, end_value)`, selected by `return_type:`.

  Pass `nil` for an open range boundary.
  """
  @spec get_by_value_range(String.t(), term(), term(), keyword()) :: t()
  def get_by_value_range(bin_name, begin_value, end_value, opts \\ [])
      when is_binary(bin_name) do
    CDT.map_read_op(
      bin_name,
      @get_by_value_range,
      [key_value_rt(opts), begin_value, end_value],
      ctx(opts)
    )
  end

  @doc """
  Returns the entry at `index`, selected by `return_type:`.
  """
  @spec get_by_index(String.t(), integer(), keyword()) :: t()
  def get_by_index(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.map_read_op(bin_name, @get_by_index, [key_value_rt(opts), index], ctx(opts))
  end

  @doc """
  Returns entries from `index` through the end, selected by `return_type:`.
  """
  @spec get_by_index_range_from(String.t(), integer(), keyword()) :: t()
  def get_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.map_read_op(bin_name, @get_by_index_range, [key_value_rt(opts), index], ctx(opts))
  end

  @doc """
  Returns `count` entries from `index`, selected by `return_type:`.
  """
  @spec get_by_index_range(String.t(), integer(), integer(), keyword()) :: t()
  def get_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.map_read_op(bin_name, @get_by_index_range, [key_value_rt(opts), index, count], ctx(opts))
  end

  @doc """
  Returns the entry at `rank`, selected by `return_type:`.
  """
  @spec get_by_rank(String.t(), integer(), keyword()) :: t()
  def get_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    CDT.map_read_op(bin_name, @get_by_rank, [key_value_rt(opts), rank], ctx(opts))
  end

  @doc """
  Returns entries from `rank` through the highest rank, selected by `return_type:`.
  """
  @spec get_by_rank_range_from(String.t(), integer(), keyword()) :: t()
  def get_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.map_read_op(bin_name, @get_by_rank_range, [key_value_rt(opts), rank], ctx(opts))
  end

  @doc """
  Returns `count` entries from `rank`, selected by `return_type:`.
  """
  @spec get_by_rank_range(String.t(), integer(), integer(), keyword()) :: t()
  def get_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.map_read_op(bin_name, @get_by_rank_range, [key_value_rt(opts), rank, count], ctx(opts))
  end

  defp ctx(opts), do: Keyword.get(opts, :ctx)

  defp key_rt(opts), do: Keyword.get(opts, :return_type, return_key())
  defp key_value_rt(opts), do: Keyword.get(opts, :return_type, return_key_value())
  defp value_rt(opts), do: Keyword.get(opts, :return_type, return_value())

  defp map_policy(opts) do
    Map.merge(%{attr: 0, flags: 0}, Keyword.get(opts, :policy, %{}))
  end

  defp write_flags(0), do: []
  defp write_flags(flags), do: [flags]
end
