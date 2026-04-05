defmodule Aerospike.Op.Map do
  @moduledoc """
  Map Collection Data Type (CDT) operations for `Aerospike.operate/4`.

  Optional `ctx:` targets nested maps (`Aerospike.Ctx`). Use `policy:` for write
  operations — default is `%{attr: 0, flags: 0}` (unordered map, default write flags).

  ## Example

      alias Aerospike.Op.Map

      Aerospike.operate(conn, key, [
        Map.put("prefs", "theme", "dark"),
        Map.get_by_key("prefs", "theme"),
        Map.remove_by_key("prefs", "legacy", return_type: Map.return_key())
      ])
  """

  alias Aerospike.Protocol.CDT

  @opaque t :: Aerospike.Op.t()

  @set_type 64
  @put 67
  @put_items 68
  @increment 73
  @decrement 74
  @clear 75
  @remove_by_key 76
  @remove_by_index 77
  @remove_by_rank 79
  @remove_key_list 81
  @remove_by_value 82
  @remove_value_list 83
  @remove_by_key_interval 84
  @remove_by_index_range 85
  @remove_by_value_interval 86
  @remove_by_rank_range 87
  @size 96
  @get_by_key 97
  @get_by_index 98
  @get_by_rank 100
  @get_by_value 102
  @get_by_key_interval 103
  @get_by_index_range 104
  @get_by_value_interval 105
  @get_by_rank_range 106
  @get_by_key_list 107
  @get_by_value_list 108

  @doc "MapReturnType: none."
  @spec return_none() :: 0
  def return_none, do: 0

  @doc "MapReturnType: key."
  @spec return_key() :: 6
  def return_key, do: 6

  @doc "MapReturnType: value."
  @spec return_value() :: 7
  def return_value, do: 7

  @doc "MapReturnType: key/value pairs."
  @spec return_key_value() :: 8
  def return_key_value, do: 8

  defp ctx(opts), do: Keyword.get(opts, :ctx)

  defp default_policy do
    %{attr: 0, flags: 0}
  end

  defp policy_attr(opts) do
    Map.merge(default_policy(), Keyword.get(opts, :policy, %{}))
  end

  @doc "Set map order / policy attributes on the bin (or nested map via `ctx:`)."
  @spec set_policy(String.t(), integer(), keyword()) :: t()
  def set_policy(bin_name, attributes, opts \\ [])
      when is_binary(bin_name) and is_integer(attributes) do
    CDT.map_modify_op(bin_name, @set_type, [attributes], ctx(opts))
  end

  @doc "Put one key/value into the map (creates the map if needed using `policy.attr` / `policy.flags`)."
  @spec put(String.t(), term(), term(), keyword()) :: t()
  def put(bin_name, map_key, value, opts \\ []) when is_binary(bin_name) do
    %{attr: attr, flags: flags} = policy_attr(opts)
    c = ctx(opts)

    args =
      if flags != 0 do
        [map_key, value, attr, flags]
      else
        [map_key, value, attr]
      end

    CDT.map_modify_op(bin_name, @put, args, c)
  end

  @doc "Put multiple entries from an Elixir map."
  @spec put_items(String.t(), map(), keyword()) :: t()
  def put_items(bin_name, map, opts \\ []) when is_binary(bin_name) and is_map(map) do
    %{attr: attr, flags: flags} = policy_attr(opts)
    c = ctx(opts)

    args =
      if flags != 0 do
        [map, attr, flags]
      else
        [map, attr]
      end

    CDT.map_modify_op(bin_name, @put_items, args, c)
  end

  @spec increment(String.t(), term(), term(), keyword()) :: t()
  def increment(bin_name, map_key, incr, opts \\ []) when is_binary(bin_name) do
    %{attr: attr} = policy_attr(opts)
    CDT.map_modify_op(bin_name, @increment, [map_key, incr, attr], ctx(opts))
  end

  @spec decrement(String.t(), term(), term(), keyword()) :: t()
  def decrement(bin_name, map_key, decr, opts \\ []) when is_binary(bin_name) do
    %{attr: attr} = policy_attr(opts)
    CDT.map_modify_op(bin_name, @decrement, [map_key, decr, attr], ctx(opts))
  end

  @spec clear(String.t(), keyword()) :: t()
  def clear(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.map_modify_op(bin_name, @clear, [], ctx(opts))
  end

  @spec remove_by_key(String.t(), term(), keyword()) :: t()
  def remove_by_key(bin_name, map_key, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_by_key, [rt, map_key], ctx(opts))
  end

  @spec remove_by_key_list(String.t(), list(), keyword()) :: t()
  def remove_by_key_list(bin_name, keys, opts \\ []) when is_binary(bin_name) and is_list(keys) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_key_list, [rt, keys], ctx(opts))
  end

  @spec remove_by_key_range(String.t(), term(), term(), keyword()) :: t()
  def remove_by_key_range(bin_name, begin_key, end_key, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_by_key_interval, [rt, begin_key, end_key], ctx(opts))
  end

  @spec remove_by_value(String.t(), term(), keyword()) :: t()
  def remove_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_by_value, [rt, value], ctx(opts))
  end

  @spec remove_by_value_list(String.t(), list(), keyword()) :: t()
  def remove_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_value_list, [rt, values], ctx(opts))
  end

  @spec remove_by_value_range(String.t(), term(), term(), keyword()) :: t()
  def remove_by_value_range(bin_name, begin_val, end_val, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_by_value_interval, [rt, begin_val, end_val], ctx(opts))
  end

  @spec remove_by_index(String.t(), integer(), keyword()) :: t()
  def remove_by_index(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_by_index, [rt, index], ctx(opts))
  end

  @spec remove_by_index_range_from(String.t(), integer(), keyword()) :: t()
  def remove_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_by_index_range, [rt, index], ctx(opts))
  end

  @spec remove_by_index_range(String.t(), integer(), integer(), keyword()) :: t()
  def remove_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_by_index_range, [rt, index, count], ctx(opts))
  end

  @spec remove_by_rank(String.t(), integer(), keyword()) :: t()
  def remove_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_by_rank, [rt, rank], ctx(opts))
  end

  @spec remove_by_rank_range_from(String.t(), integer(), keyword()) :: t()
  def remove_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_by_rank_range, [rt, rank], ctx(opts))
  end

  @spec remove_by_rank_range(String.t(), integer(), integer(), keyword()) :: t()
  def remove_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_by_rank_range, [rt, rank, count], ctx(opts))
  end

  @spec size(String.t(), keyword()) :: t()
  def size(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.map_read_op(bin_name, @size, [], ctx(opts))
  end

  @spec get_by_key(String.t(), term(), keyword()) :: t()
  def get_by_key(bin_name, map_key, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_read_op(bin_name, @get_by_key, [rt, map_key], ctx(opts))
  end

  @spec get_by_key_list(String.t(), list(), keyword()) :: t()
  def get_by_key_list(bin_name, keys, opts \\ []) when is_binary(bin_name) and is_list(keys) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_key_list, [rt, keys], ctx(opts))
  end

  @spec get_by_key_range(String.t(), term(), term(), keyword()) :: t()
  def get_by_key_range(bin_name, begin_key, end_key, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_key_interval, [rt, begin_key, end_key], ctx(opts))
  end

  @spec get_by_value(String.t(), term(), keyword()) :: t()
  def get_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_value, [rt, value], ctx(opts))
  end

  @spec get_by_value_list(String.t(), list(), keyword()) :: t()
  def get_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_value_list, [rt, values], ctx(opts))
  end

  @spec get_by_value_range(String.t(), term(), term(), keyword()) :: t()
  def get_by_value_range(bin_name, begin_val, end_val, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_value_interval, [rt, begin_val, end_val], ctx(opts))
  end

  @spec get_by_index(String.t(), integer(), keyword()) :: t()
  def get_by_index(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_index, [rt, index], ctx(opts))
  end

  @spec get_by_index_range_from(String.t(), integer(), keyword()) :: t()
  def get_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_index_range, [rt, index], ctx(opts))
  end

  @spec get_by_index_range(String.t(), integer(), integer(), keyword()) :: t()
  def get_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_index_range, [rt, index, count], ctx(opts))
  end

  @spec get_by_rank(String.t(), integer(), keyword()) :: t()
  def get_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_rank, [rt, rank], ctx(opts))
  end

  @spec get_by_rank_range_from(String.t(), integer(), keyword()) :: t()
  def get_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_rank_range, [rt, rank], ctx(opts))
  end

  @spec get_by_rank_range(String.t(), integer(), integer(), keyword()) :: t()
  def get_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_rank_range, [rt, rank, count], ctx(opts))
  end
end
