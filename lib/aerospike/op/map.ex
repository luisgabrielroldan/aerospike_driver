defmodule Aerospike.Op.Map do
  @moduledoc """
  Map Collection Data Type (CDT) operations for `Aerospike.operate/4`.

  Unique-key maps are the default; the default storage order is unordered. Typical map
  keys are strings, integers, or binaries.

  Every map maintains an **index** (item offset from the start) and a **rank** (sorted
  position by value). Both support negative indexing.

  ## Index examples

      Index  0         -> first item
      Index  4         -> fifth item
      Index -1         -> last item
      Index -3         -> third to last item
      Index  1, Count 2 -> second and third items

  ## Rank examples

      Rank  0  -> lowest-valued item
      Rank  4  -> fifth lowest by value
      Rank -1  -> highest-valued item
      Rank -3  -> third highest by value

  ## Map ordering

  Maps can be unordered (default), key-ordered, or key-value-ordered. Key-ordered maps
  (`attr: 1`) give O(log N) lookups on in-memory namespaces and are recommended.

  ## Policy and context

  Pass `policy: %{attr: order_attr, flags: write_flags}` on writes. Default is
  `%{attr: 0, flags: 0}` (unordered map, default write flags). Use `ctx:` for nested
  maps (see `Aerospike.Ctx`).

  ### Write flags

  Combine with `Bitwise.|||/2`:

  - `0` — default upsert (create or update)
  - `1` — `CREATE_ONLY` — fail if key exists
  - `2` — `UPDATE_ONLY` — fail if key missing
  - `4` — `NO_FAIL` — no-op instead of error on policy violation
  - `8` — `DO_PARTIAL` — with `NO_FAIL`, apply entries that don't violate the policy

  ## Return types

  Functions that accept `return_type:` control what the server returns. Helpers:

  - `return_none/0` — nothing (fastest for write-only operations)
  - `return_index/0` — index(es) of affected entries
  - `return_reverse_index/0` — reverse index(es) from map end
  - `return_rank/0` — rank(s) of affected entries
  - `return_reverse_rank/0` — reverse rank(s) from highest rank
  - `return_count/0` — count of affected entries
  - `return_key/0` — key(s) of affected entries
  - `return_value/0` — value(s) of affected entries
  - `return_key_value/0` — key/value pairs
  - `return_exists/0` — existence flag(s) for matched entries

  Each function documents its default when `return_type:` is omitted.

  ## Examples

      alias Aerospike.Op.Map

      # Basic CRUD on a map bin
      Aerospike.operate(conn, key, [
        Map.put("prefs", "theme", "dark"),
        Map.get_by_key("prefs", "theme"),
        Map.remove_by_key("prefs", "legacy", return_type: Map.return_key())
      ])

      # Leaderboard: get top 3 scores by rank
      Aerospike.operate(conn, key, [
        Map.get_by_rank_range("scores", -3, 3,
          return_type: Map.return_key_value())
      ])

      # Atomic counter increment per category
      Aerospike.operate(conn, key, [
        Map.increment("stats", "views", 1),
        Map.increment("stats", "clicks", 1)
      ])

      # Trim old events by key range (timestamps as keys)
      cutoff = System.system_time(:millisecond) - 86_400_000
      Aerospike.operate(conn, key, [
        Map.remove_by_key_range("events", nil, cutoff,
          return_type: Map.return_none())
      ])

  See the [Map Patterns guide](map-patterns.md) for real-world usage patterns
  like event containers, document stores, and leaderboards.
  """

  alias Aerospike.Protocol.CDT

  @typedoc """
  Opaque map-CDT operation for `Aerospike.operate/4`.

  Construct via `Aerospike.Op.Map.*` builder functions and pass in an operation list.
  """
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

  @doc "MapReturnType: index."
  @spec return_index() :: 1
  def return_index, do: 1

  @doc "MapReturnType: reverse index."
  @spec return_reverse_index() :: 2
  def return_reverse_index, do: 2

  @doc "MapReturnType: rank."
  @spec return_rank() :: 3
  def return_rank, do: 3

  @doc "MapReturnType: reverse rank."
  @spec return_reverse_rank() :: 4
  def return_reverse_rank, do: 4

  @doc "MapReturnType: count."
  @spec return_count() :: 5
  def return_count, do: 5

  @doc "MapReturnType: key."
  @spec return_key() :: 6
  def return_key, do: 6

  @doc "MapReturnType: value."
  @spec return_value() :: 7
  def return_value, do: 7

  @doc "MapReturnType: key/value pairs."
  @spec return_key_value() :: 8
  def return_key_value, do: 8

  @doc "MapReturnType: exists."
  @spec return_exists() :: 13
  def return_exists, do: 13

  defp ctx(opts), do: Keyword.get(opts, :ctx)

  defp default_policy do
    %{attr: 0, flags: 0}
  end

  defp policy_attr(opts) do
    Map.merge(default_policy(), Keyword.get(opts, :policy, %{}))
  end

  @doc """
  Sets map order and policy attributes on the bin (or nested map via `ctx:`).

  Server returns no value. Attributes may be changed after the map is created.
  """
  @spec set_policy(String.t(), integer(), keyword()) :: t()
  def set_policy(bin_name, attributes, opts \\ [])
      when is_binary(bin_name) and is_integer(attributes) do
    CDT.map_modify_op(bin_name, @set_type, [attributes], ctx(opts))
  end

  @doc """
  Writes one key/value to the map and returns the map size.

  Creates the map when missing using `policy.attr` / `policy.flags`.
  """
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

  @doc """
  Writes each entry from an Elixir map into the bin map and returns the map size.

  Creates the map when missing using `policy.attr` / `policy.flags`.
  """
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

  @doc """
  Atomically increments the numeric value at `map_key` and returns the final value.

  Valid only for numeric values. Uses `policy.attr` when creating the map if needed.
  """
  @spec increment(String.t(), term(), term(), keyword()) :: t()
  def increment(bin_name, map_key, incr, opts \\ []) when is_binary(bin_name) do
    %{attr: attr} = policy_attr(opts)
    CDT.map_modify_op(bin_name, @increment, [map_key, incr, attr], ctx(opts))
  end

  @doc """
  Atomically decrements the numeric value at `map_key` and returns the final value.

  Valid only for numeric values. Uses `policy.attr` when creating the map if needed.
  """
  @spec decrement(String.t(), term(), term(), keyword()) :: t()
  def decrement(bin_name, map_key, decr, opts \\ []) when is_binary(bin_name) do
    %{attr: attr} = policy_attr(opts)
    CDT.map_modify_op(bin_name, @decrement, [map_key, decr, attr], ctx(opts))
  end

  @doc """
  Removes all entries from the map.

  Server returns no value.
  """
  @spec clear(String.t(), keyword()) :: t()
  def clear(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.map_modify_op(bin_name, @clear, [], ctx(opts))
  end

  @doc """
  Removes the entry for `map_key` and returns data per `return_type:`.

  Default `return_type:` is `return_key/0`.
  """
  @spec remove_by_key(String.t(), term(), keyword()) :: t()
  def remove_by_key(bin_name, map_key, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_by_key, [rt, map_key], ctx(opts))
  end

  @doc """
  Removes entries for all keys in `keys` and returns data per `return_type:`.

  Default `return_type:` is `return_key/0`.
  """
  @spec remove_by_key_list(String.t(), list(), keyword()) :: t()
  def remove_by_key_list(bin_name, keys, opts \\ []) when is_binary(bin_name) and is_list(keys) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_key_list, [rt, keys], ctx(opts))
  end

  @doc """
  Removes entries whose keys fall in `[begin_key, end_key)` (begin inclusive, end exclusive).

  Pass `nil` for `begin_key` for keys less than `end_key`, or `nil` for `end_key` for keys
  greater than or equal to `begin_key`. Returns data per `return_type:` (default
  `return_key/0`).
  """
  @spec remove_by_key_range(String.t(), term(), term(), keyword()) :: t()
  def remove_by_key_range(bin_name, begin_key, end_key, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_by_key_interval, [rt, begin_key, end_key], ctx(opts))
  end

  @doc """
  Removes entries whose value equals `value` and returns data per `return_type:`.

  Default `return_type:` is `return_value/0`.
  """
  @spec remove_by_value(String.t(), term(), keyword()) :: t()
  def remove_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_by_value, [rt, value], ctx(opts))
  end

  @doc """
  Removes entries whose value is in `values` and returns data per `return_type:`.

  Default `return_type:` is `return_value/0`.
  """
  @spec remove_by_value_list(String.t(), list(), keyword()) :: t()
  def remove_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_value_list, [rt, values], ctx(opts))
  end

  @doc """
  Removes entries whose values fall in `[begin_val, end_val)` (begin inclusive, end exclusive).

  Open-ended ranges use `nil` for `begin_val` or `end_val` as for key ranges.
  Default `return_type:` is `return_value/0`.
  """
  @spec remove_by_value_range(String.t(), term(), term(), keyword()) :: t()
  def remove_by_value_range(bin_name, begin_val, end_val, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_by_value_interval, [rt, begin_val, end_val], ctx(opts))
  end

  @doc """
  Removes the entry at map index `index` and returns data per `return_type:`.

  Supports negative index. Default `return_type:` is `return_key/0`.
  """
  @spec remove_by_index(String.t(), integer(), keyword()) :: t()
  def remove_by_index(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_by_index, [rt, index], ctx(opts))
  end

  @doc """
  Removes all entries from `index` through the end of the map.

  Default `return_type:` is `return_key/0`.
  """
  @spec remove_by_index_range_from(String.t(), integer(), keyword()) :: t()
  def remove_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_by_index_range, [rt, index], ctx(opts))
  end

  @doc """
  Removes `count` entries starting at `index`.

  Default `return_type:` is `return_key/0`.
  """
  @spec remove_by_index_range(String.t(), integer(), integer(), keyword()) :: t()
  def remove_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    rt = Keyword.get(opts, :return_type, return_key())
    CDT.map_modify_op(bin_name, @remove_by_index_range, [rt, index, count], ctx(opts))
  end

  @doc """
  Removes the entry at value-sorted rank `rank` and returns data per `return_type:`.

  Rank `0` = smallest value, `-1` = largest. Default `return_type:` is `return_value/0`.
  """
  @spec remove_by_rank(String.t(), integer(), keyword()) :: t()
  def remove_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_by_rank, [rt, rank], ctx(opts))
  end

  @doc """
  Removes all entries from rank `rank` through the highest rank.

  Default `return_type:` is `return_value/0`.
  """
  @spec remove_by_rank_range_from(String.t(), integer(), keyword()) :: t()
  def remove_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_by_rank_range, [rt, rank], ctx(opts))
  end

  @doc """
  Removes `count` entries starting at value-sorted rank `rank`.

  Default `return_type:` is `return_value/0`. Often used with `rank: -1` and a positive
  `count` to take the top *N* values by rank.
  """
  @spec remove_by_rank_range(String.t(), integer(), integer(), keyword()) :: t()
  def remove_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_modify_op(bin_name, @remove_by_rank_range, [rt, rank, count], ctx(opts))
  end

  @doc """
  Returns the number of entries in the map.
  """
  @spec size(String.t(), keyword()) :: t()
  def size(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.map_read_op(bin_name, @size, [], ctx(opts))
  end

  @doc """
  Returns the entry for `map_key`, shaped by `return_type:`.

  Default `return_type:` is `return_value/0`. When multiple operations target the same bin,
  the client may return a list of partial results instead of a single value.
  """
  @spec get_by_key(String.t(), term(), keyword()) :: t()
  def get_by_key(bin_name, map_key, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_value())
    CDT.map_read_op(bin_name, @get_by_key, [rt, map_key], ctx(opts))
  end

  @doc """
  Returns entries for all keys in `keys`, shaped by `return_type:`.

  Default `return_type:` is `return_key_value/0`.
  """
  @spec get_by_key_list(String.t(), list(), keyword()) :: t()
  def get_by_key_list(bin_name, keys, opts \\ []) when is_binary(bin_name) and is_list(keys) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_key_list, [rt, keys], ctx(opts))
  end

  @doc """
  Returns entries whose keys fall in `[begin_key, end_key)` (begin inclusive, end exclusive).

  Open-ended ranges use `nil` for either bound. Works best on ordered maps.
  Default `return_type:` is `return_key_value/0`.
  """
  @spec get_by_key_range(String.t(), term(), term(), keyword()) :: t()
  def get_by_key_range(bin_name, begin_key, end_key, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_key_interval, [rt, begin_key, end_key], ctx(opts))
  end

  @doc """
  Returns entries whose value equals `value`, shaped by `return_type:`.

  Default `return_type:` is `return_key_value/0`.
  """
  @spec get_by_value(String.t(), term(), keyword()) :: t()
  def get_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_value, [rt, value], ctx(opts))
  end

  @doc """
  Returns entries whose value is in `values`, shaped by `return_type:`.

  Default `return_type:` is `return_key_value/0`.
  """
  @spec get_by_value_list(String.t(), list(), keyword()) :: t()
  def get_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_value_list, [rt, values], ctx(opts))
  end

  @doc """
  Returns entries whose values fall in `[begin_val, end_val)` (begin inclusive, end exclusive).

  Default `return_type:` is `return_key_value/0`.
  """
  @spec get_by_value_range(String.t(), term(), term(), keyword()) :: t()
  def get_by_value_range(bin_name, begin_val, end_val, opts \\ []) when is_binary(bin_name) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_value_interval, [rt, begin_val, end_val], ctx(opts))
  end

  @doc """
  Returns the entry at map index `index`, shaped by `return_type:`.

  Default `return_type:` is `return_key_value/0`.
  """
  @spec get_by_index(String.t(), integer(), keyword()) :: t()
  def get_by_index(bin_name, index, opts \\ []) when is_binary(bin_name) and is_integer(index) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_index, [rt, index], ctx(opts))
  end

  @doc """
  Returns all entries from `index` through the end of the map.

  Default `return_type:` is `return_key_value/0`.
  """
  @spec get_by_index_range_from(String.t(), integer(), keyword()) :: t()
  def get_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_index_range, [rt, index], ctx(opts))
  end

  @doc """
  Returns `count` entries starting at map index `index`.

  Default `return_type:` is `return_key_value/0`.
  """
  @spec get_by_index_range(String.t(), integer(), integer(), keyword()) :: t()
  def get_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_index_range, [rt, index, count], ctx(opts))
  end

  @doc """
  Returns the entry at value-sorted rank `rank`, shaped by `return_type:`.

  Rank `0` = lowest value, `-1` = highest (common for leaderboards).
  Default `return_type:` is `return_key_value/0`.
  """
  @spec get_by_rank(String.t(), integer(), keyword()) :: t()
  def get_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_rank, [rt, rank], ctx(opts))
  end

  @doc """
  Returns all entries from rank `rank` through the highest rank.

  Default `return_type:` is `return_key_value/0`.
  """
  @spec get_by_rank_range_from(String.t(), integer(), keyword()) :: t()
  def get_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_rank_range, [rt, rank], ctx(opts))
  end

  @doc """
  Returns `count` entries starting at value-sorted rank `rank`.

  Default `return_type:` is `return_key_value/0`. Using `rank: -1` with a positive `count`
  returns the top *N* items by value rank.
  """
  @spec get_by_rank_range(String.t(), integer(), integer(), keyword()) :: t()
  def get_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    rt = Keyword.get(opts, :return_type, return_key_value())
    CDT.map_read_op(bin_name, @get_by_rank_range, [rt, rank, count], ctx(opts))
  end
end
