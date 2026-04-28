defmodule Aerospike.Op.Map do
  @moduledoc """
  Map CDT operations for `Aerospike.operate/4`.

  These builders create server-side map operations. Pass `ctx:` for nested CDT
  paths and `return_type:` for selector operations that can return keys,
  values, key/value pairs, counts, ranks, indexes, or existence flags.
  """

  alias Aerospike.PolicyInteger
  alias Aerospike.Protocol.CDT

  @typedoc """
  Opaque map CDT operation for `Aerospike.operate/4`.
  """
  @opaque t :: Aerospike.Op.t()

  @typedoc """
  Raw compatibility value for server integer policy constants.
  """
  @type raw_integer :: non_neg_integer() | {:raw, non_neg_integer()}

  @typedoc """
  Map order attribute.
  """
  @type order ::
          :unordered
          | :key_ordered
          | :key_value_ordered
          | raw_integer()

  @typedoc """
  Map write policy flag or flags.
  """
  @type write_flag :: :default | :create_only | :update_only | :no_fail | :partial
  @type write_flags :: write_flag() | [write_flag()] | raw_integer()

  @typedoc """
  Map selector return type.
  """
  @type return_type ::
          :none
          | :index
          | :reverse_index
          | :rank
          | :reverse_rank
          | :count
          | :key
          | :value
          | :key_value
          | :exists
          | :unordered_map
          | :ordered_map
          | :inverted
          | [
              :inverted
              | :index
              | :reverse_index
              | :rank
              | :reverse_rank
              | :count
              | :key
              | :value
              | :key_value
              | :exists
              | :unordered_map
              | :ordered_map
            ]
          | raw_integer()

  @typedoc """
  Map write policy accepted in `opts[:policy]`.

  Prefer `:order` for the map order. `:attr` remains accepted for
  compatibility. Omitted keys default to `0`.
  """
  @type policy ::
          %{
            optional(:order) => order(),
            optional(:attr) => order(),
            optional(:flags) => write_flags()
          }
          | [{:order, order()} | {:attr, order()} | {:flags, write_flags()}]

  @typedoc """
  Common map operation options.

  Supported keys:

  * `:ctx` - nested CDT context path from `Aerospike.Ctx`.
  * `:return_type` - selector return type from the `return_*` helpers.
  * `:policy` - map write policy for put/increment/decrement operations.
  * `:persist_index` - persist the index for top-level ordered maps.
  """
  @type opts :: [
          ctx: Aerospike.Ctx.t(),
          return_type: return_type(),
          policy: policy(),
          persist_index: boolean()
        ]

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
  @remove_by_key_rel_index_range 88
  @remove_by_value_rel_rank_range 89
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
  @get_by_key_rel_index_range 109
  @get_by_value_rel_rank_range 110

  @inverted 0x10000
  @doc "Create unordered maps by default."
  @spec order_unordered() :: 0
  def order_unordered, do: 0

  @doc "Create maps ordered by key."
  @spec order_key_ordered() :: 1
  def order_key_ordered, do: 1

  @doc "Create maps ordered by key and value."
  @spec order_key_value_ordered() :: 3
  def order_key_value_ordered, do: 3

  @doc "Use default map write behavior."
  @spec write_default() :: 0
  def write_default, do: 0

  @doc "Only create map items that do not already exist."
  @spec write_create_only() :: 1
  def write_create_only, do: 1

  @doc "Only update map items that already exist."
  @spec write_update_only() :: 2
  def write_update_only, do: 2

  @doc "Do not fail the command when a map item is rejected by write flags."
  @spec write_no_fail() :: 4
  def write_no_fail, do: 4

  @doc "Commit valid map items even when another item is rejected by write flags."
  @spec write_partial() :: 8
  def write_partial, do: 8

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

  @doc "Return selected entries as an unordered map."
  @spec return_unordered_map() :: 16
  def return_unordered_map, do: 16

  @doc "Return selected entries as an ordered map."
  @spec return_ordered_map() :: 17
  def return_ordered_map, do: 17

  @doc "Invert the selector so it applies outside the matched range."
  @spec return_inverted() :: 0x10000
  def return_inverted, do: @inverted

  @doc """
  Creates a map at the selected context level.

  When `ctx:` is omitted, this sets the top-level bin map order. Nested map
  creation ignores `persist_index:` because server indexes are top-level only.
  """
  @spec create(String.t(), order(), opts()) :: t()
  def create(bin_name, order, opts \\ [])
      when is_binary(bin_name) do
    normalized_order = PolicyInteger.map_order(order)

    case ctx(opts) do
      steps when is_list(steps) and steps != [] ->
        CDT.map_create_op(
          bin_name,
          PolicyInteger.map_create_flag(order),
          [@set_policy, normalized_order],
          steps
        )

      _ ->
        set_policy(bin_name, normalized_order, opts)
    end
  end

  @doc """
  Sets the map order attributes.
  """
  @spec set_policy(String.t(), order(), opts()) :: t()
  def set_policy(bin_name, attributes, opts \\ [])
      when is_binary(bin_name) do
    steps = ctx(opts)
    attributes = attributes |> order_attr(opts) |> policy_attr(steps)

    CDT.map_modify_op(bin_name, @set_policy, [attributes], steps)
  end

  @doc """
  Writes one key/value pair and returns the map size.

  Pass `policy: [order: order, flags: flags]` to include map order attributes
  and write flags in the operation payload.
  """
  @spec put(String.t(), term(), term(), opts()) :: t()
  def put(bin_name, map_key, value, opts \\ []) when is_binary(bin_name) do
    %{attr: attr, flags: flags} = map_policy(opts)
    CDT.map_modify_op(bin_name, @put, [map_key, value, attr | write_flags(flags)], ctx(opts))
  end

  @doc """
  Writes multiple key/value pairs and returns the map size.

  Pass `policy: [order: order, flags: flags]` to include map order attributes
  and write flags in the operation payload.
  """
  @spec put_items(String.t(), map(), opts()) :: t()
  def put_items(bin_name, values, opts \\ []) when is_binary(bin_name) and is_map(values) do
    %{attr: attr, flags: flags} = map_policy(opts)
    CDT.map_modify_op(bin_name, @put_items, [values, attr | write_flags(flags)], ctx(opts))
  end

  @doc """
  Increments the numeric value at `map_key` and returns the final value.
  """
  @spec increment(String.t(), term(), term(), opts()) :: t()
  def increment(bin_name, map_key, delta, opts \\ [])
      when is_binary(bin_name) do
    %{attr: attr} = map_policy(opts)
    CDT.map_modify_op(bin_name, @increment, [map_key, delta, attr], ctx(opts))
  end

  @doc """
  Decrements the numeric value at `map_key` and returns the final value.
  """
  @spec decrement(String.t(), term(), term(), opts()) :: t()
  def decrement(bin_name, map_key, delta, opts \\ [])
      when is_binary(bin_name) do
    %{attr: attr} = map_policy(opts)
    CDT.map_modify_op(bin_name, @decrement, [map_key, delta, attr], ctx(opts))
  end

  @doc """
  Removes all entries from the map.
  """
  @spec clear(String.t(), opts()) :: t()
  def clear(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.map_modify_op(bin_name, @clear, [], ctx(opts))
  end

  @doc """
  Removes the entry for `map_key`, returning data selected by `return_type:`.
  """
  @spec remove_by_key(String.t(), term(), opts()) :: t()
  def remove_by_key(bin_name, map_key, opts \\ []) when is_binary(bin_name) do
    CDT.map_modify_op(bin_name, @remove_by_key, [key_rt(opts), map_key], ctx(opts))
  end

  @doc """
  Removes entries matching any key in `keys`, returning selected data.
  """
  @spec remove_by_key_list(String.t(), list(), opts()) :: t()
  def remove_by_key_list(bin_name, keys, opts \\ []) when is_binary(bin_name) and is_list(keys) do
    CDT.map_modify_op(bin_name, @remove_by_key_list, [key_rt(opts), keys], ctx(opts))
  end

  @doc """
  Removes entries with keys in `[begin_key, end_key)`, returning selected data.

  Pass `nil` for an open range boundary.
  """
  @spec remove_by_key_range(String.t(), term(), term(), opts()) :: t()
  def remove_by_key_range(bin_name, begin_key, end_key, opts \\ []) when is_binary(bin_name) do
    CDT.map_modify_op(
      bin_name,
      @remove_by_key_range,
      [key_rt(opts), begin_key, end_key],
      ctx(opts)
    )
  end

  @doc """
  Removes entries nearest to `map_key` and greater by relative `index`.
  """
  @spec remove_by_key_rel_index_range(String.t(), term(), integer(), opts()) :: t()
  def remove_by_key_rel_index_range(bin_name, map_key, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.map_modify_op(
      bin_name,
      @remove_by_key_rel_index_range,
      [key_rt(opts), map_key, index],
      ctx(opts)
    )
  end

  @doc """
  Removes `count` entries nearest to `map_key` and greater by relative `index`.
  """
  @spec remove_by_key_rel_index_range_count(String.t(), term(), integer(), integer(), opts()) ::
          t()
  def remove_by_key_rel_index_range_count(bin_name, map_key, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.map_modify_op(
      bin_name,
      @remove_by_key_rel_index_range,
      [key_rt(opts), map_key, index, count],
      ctx(opts)
    )
  end

  @doc """
  Removes entries equal to `value`, returning data selected by `return_type:`.
  """
  @spec remove_by_value(String.t(), term(), opts()) :: t()
  def remove_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.map_modify_op(bin_name, @remove_by_value, [value_rt(opts), value], ctx(opts))
  end

  @doc """
  Removes entries matching any value in `values`, returning selected data.
  """
  @spec remove_by_value_list(String.t(), list(), opts()) :: t()
  def remove_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    CDT.map_modify_op(bin_name, @remove_by_value_list, [value_rt(opts), values], ctx(opts))
  end

  @doc """
  Removes entries with values in `[begin_value, end_value)`, returning selected data.

  Pass `nil` for an open range boundary.
  """
  @spec remove_by_value_range(String.t(), term(), term(), opts()) :: t()
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
  Removes entries nearest to `value` and greater by relative `rank`.
  """
  @spec remove_by_value_rel_rank_range(String.t(), term(), integer(), opts()) :: t()
  def remove_by_value_rel_rank_range(bin_name, value, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.map_modify_op(
      bin_name,
      @remove_by_value_rel_rank_range,
      [value_rt(opts), value, rank],
      ctx(opts)
    )
  end

  @doc """
  Removes `count` entries nearest to `value` and greater by relative `rank`.
  """
  @spec remove_by_value_rel_rank_range_count(String.t(), term(), integer(), integer(), opts()) ::
          t()
  def remove_by_value_rel_rank_range_count(bin_name, value, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.map_modify_op(
      bin_name,
      @remove_by_value_rel_rank_range,
      [value_rt(opts), value, rank, count],
      ctx(opts)
    )
  end

  @doc """
  Removes the entry at `index`, returning data selected by `return_type:`.
  """
  @spec remove_by_index(String.t(), integer(), opts()) :: t()
  def remove_by_index(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.map_modify_op(bin_name, @remove_by_index, [key_rt(opts), index], ctx(opts))
  end

  @doc """
  Removes entries from `index` through the end, returning selected data.
  """
  @spec remove_by_index_range_from(String.t(), integer(), opts()) :: t()
  def remove_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.map_modify_op(bin_name, @remove_by_index_range, [key_rt(opts), index], ctx(opts))
  end

  @doc """
  Removes `count` entries from `index`, returning selected data.
  """
  @spec remove_by_index_range(String.t(), integer(), integer(), opts()) :: t()
  def remove_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.map_modify_op(bin_name, @remove_by_index_range, [key_rt(opts), index, count], ctx(opts))
  end

  @doc """
  Removes the entry at `rank`, returning data selected by `return_type:`.
  """
  @spec remove_by_rank(String.t(), integer(), opts()) :: t()
  def remove_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    CDT.map_modify_op(bin_name, @remove_by_rank, [value_rt(opts), rank], ctx(opts))
  end

  @doc """
  Removes entries from `rank` through the highest rank, returning selected data.
  """
  @spec remove_by_rank_range_from(String.t(), integer(), opts()) :: t()
  def remove_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.map_modify_op(bin_name, @remove_by_rank_range, [value_rt(opts), rank], ctx(opts))
  end

  @doc """
  Removes `count` entries from `rank`, returning selected data.
  """
  @spec remove_by_rank_range(String.t(), integer(), integer(), opts()) :: t()
  def remove_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.map_modify_op(bin_name, @remove_by_rank_range, [value_rt(opts), rank, count], ctx(opts))
  end

  @doc """
  Returns the number of entries in the map.
  """
  @spec size(String.t(), opts()) :: t()
  def size(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.map_read_op(bin_name, @size, [], ctx(opts))
  end

  @doc """
  Returns the entry for `map_key`, selected by `return_type:`.
  """
  @spec get_by_key(String.t(), term(), opts()) :: t()
  def get_by_key(bin_name, map_key, opts \\ []) when is_binary(bin_name) do
    CDT.map_read_op(bin_name, @get_by_key, [value_rt(opts), map_key], ctx(opts))
  end

  @doc """
  Returns entries matching any key in `keys`, selected by `return_type:`.
  """
  @spec get_by_key_list(String.t(), list(), opts()) :: t()
  def get_by_key_list(bin_name, keys, opts \\ []) when is_binary(bin_name) and is_list(keys) do
    CDT.map_read_op(bin_name, @get_by_key_list, [key_value_rt(opts), keys], ctx(opts))
  end

  @doc """
  Returns entries with keys in `[begin_key, end_key)`, selected by `return_type:`.

  Pass `nil` for an open range boundary.
  """
  @spec get_by_key_range(String.t(), term(), term(), opts()) :: t()
  def get_by_key_range(bin_name, begin_key, end_key, opts \\ []) when is_binary(bin_name) do
    CDT.map_read_op(
      bin_name,
      @get_by_key_range,
      [key_value_rt(opts), begin_key, end_key],
      ctx(opts)
    )
  end

  @doc """
  Returns entries nearest to `map_key` and greater by relative `index`.
  """
  @spec get_by_key_rel_index_range(String.t(), term(), integer(), opts()) :: t()
  def get_by_key_rel_index_range(bin_name, map_key, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.map_read_op(
      bin_name,
      @get_by_key_rel_index_range,
      [key_value_rt(opts), map_key, index],
      ctx(opts)
    )
  end

  @doc """
  Returns `count` entries nearest to `map_key` and greater by relative `index`.
  """
  @spec get_by_key_rel_index_range_count(String.t(), term(), integer(), integer(), opts()) ::
          t()
  def get_by_key_rel_index_range_count(bin_name, map_key, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.map_read_op(
      bin_name,
      @get_by_key_rel_index_range,
      [key_value_rt(opts), map_key, index, count],
      ctx(opts)
    )
  end

  @doc """
  Returns entries equal to `value`, selected by `return_type:`.
  """
  @spec get_by_value(String.t(), term(), opts()) :: t()
  def get_by_value(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.map_read_op(bin_name, @get_by_value, [key_value_rt(opts), value], ctx(opts))
  end

  @doc """
  Returns entries matching any value in `values`, selected by `return_type:`.
  """
  @spec get_by_value_list(String.t(), list(), opts()) :: t()
  def get_by_value_list(bin_name, values, opts \\ [])
      when is_binary(bin_name) and is_list(values) do
    CDT.map_read_op(bin_name, @get_by_value_list, [key_value_rt(opts), values], ctx(opts))
  end

  @doc """
  Returns entries with values in `[begin_value, end_value)`, selected by `return_type:`.

  Pass `nil` for an open range boundary.
  """
  @spec get_by_value_range(String.t(), term(), term(), opts()) :: t()
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
  Returns entries nearest to `value` and greater by relative `rank`.
  """
  @spec get_by_value_rel_rank_range(String.t(), term(), integer(), opts()) :: t()
  def get_by_value_rel_rank_range(bin_name, value, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.map_read_op(
      bin_name,
      @get_by_value_rel_rank_range,
      [key_value_rt(opts), value, rank],
      ctx(opts)
    )
  end

  @doc """
  Returns `count` entries nearest to `value` and greater by relative `rank`.
  """
  @spec get_by_value_rel_rank_range_count(String.t(), term(), integer(), integer(), opts()) ::
          t()
  def get_by_value_rel_rank_range_count(bin_name, value, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.map_read_op(
      bin_name,
      @get_by_value_rel_rank_range,
      [key_value_rt(opts), value, rank, count],
      ctx(opts)
    )
  end

  @doc """
  Returns the entry at `index`, selected by `return_type:`.
  """
  @spec get_by_index(String.t(), integer(), opts()) :: t()
  def get_by_index(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.map_read_op(bin_name, @get_by_index, [key_value_rt(opts), index], ctx(opts))
  end

  @doc """
  Returns entries from `index` through the end, selected by `return_type:`.
  """
  @spec get_by_index_range_from(String.t(), integer(), opts()) :: t()
  def get_by_index_range_from(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.map_read_op(bin_name, @get_by_index_range, [key_value_rt(opts), index], ctx(opts))
  end

  @doc """
  Returns `count` entries from `index`, selected by `return_type:`.
  """
  @spec get_by_index_range(String.t(), integer(), integer(), opts()) :: t()
  def get_by_index_range(bin_name, index, count, opts \\ [])
      when is_binary(bin_name) and is_integer(index) and is_integer(count) do
    CDT.map_read_op(bin_name, @get_by_index_range, [key_value_rt(opts), index, count], ctx(opts))
  end

  @doc """
  Returns the entry at `rank`, selected by `return_type:`.
  """
  @spec get_by_rank(String.t(), integer(), opts()) :: t()
  def get_by_rank(bin_name, rank, opts \\ []) when is_binary(bin_name) and is_integer(rank) do
    CDT.map_read_op(bin_name, @get_by_rank, [key_value_rt(opts), rank], ctx(opts))
  end

  @doc """
  Returns entries from `rank` through the highest rank, selected by `return_type:`.
  """
  @spec get_by_rank_range_from(String.t(), integer(), opts()) :: t()
  def get_by_rank_range_from(bin_name, rank, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) do
    CDT.map_read_op(bin_name, @get_by_rank_range, [key_value_rt(opts), rank], ctx(opts))
  end

  @doc """
  Returns `count` entries from `rank`, selected by `return_type:`.
  """
  @spec get_by_rank_range(String.t(), integer(), integer(), opts()) :: t()
  def get_by_rank_range(bin_name, rank, count, opts \\ [])
      when is_binary(bin_name) and is_integer(rank) and is_integer(count) do
    CDT.map_read_op(bin_name, @get_by_rank_range, [key_value_rt(opts), rank, count], ctx(opts))
  end

  defp ctx(opts), do: Keyword.get(opts, :ctx)

  defp key_rt(opts),
    do: opts |> Keyword.get(:return_type, :key) |> PolicyInteger.map_return_type()

  defp key_value_rt(opts),
    do: opts |> Keyword.get(:return_type, :key_value) |> PolicyInteger.map_return_type()

  defp value_rt(opts),
    do: opts |> Keyword.get(:return_type, :value) |> PolicyInteger.map_return_type()

  defp order_attr(order, opts) do
    PolicyInteger.map_order_attr(order, Keyword.get(opts, :persist_index, false))
  end

  defp policy_attr(attributes, steps) when is_list(steps) and steps != [] do
    PolicyInteger.strip_persist_index(attributes)
  end

  defp policy_attr(attributes, _steps), do: attributes

  defp map_policy(opts), do: opts |> Keyword.get(:policy) |> PolicyInteger.map_policy()

  defp write_flags(0), do: []
  defp write_flags(flags), do: [flags]
end
