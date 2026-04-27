defmodule Aerospike.Exp.Map do
  @moduledoc """
  Map CDT expression helpers.
  """

  import Bitwise

  alias Aerospike.Exp
  alias Aerospike.Exp.Module
  alias Aerospike.Op.Map, as: MapOp

  @typedoc "Opaque server-side expression."
  @type t :: Exp.t()

  @put 67
  @put_items 68
  @increment 73
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

  @spec return_none() :: 0
  def return_none, do: MapOp.return_none()

  @spec return_index() :: 1
  def return_index, do: MapOp.return_index()

  @spec return_reverse_index() :: 2
  def return_reverse_index, do: MapOp.return_reverse_index()

  @spec return_rank() :: 3
  def return_rank, do: MapOp.return_rank()

  @spec return_reverse_rank() :: 4
  def return_reverse_rank, do: MapOp.return_reverse_rank()

  @spec return_count() :: 5
  def return_count, do: MapOp.return_count()

  @spec return_key() :: 6
  def return_key, do: MapOp.return_key()

  @spec return_value() :: 7
  def return_value, do: MapOp.return_value()

  @spec return_key_value() :: 8
  def return_key_value, do: MapOp.return_key_value()

  @spec return_exists() :: 13
  def return_exists, do: MapOp.return_exists()

  @spec return_unordered_map() :: 16
  def return_unordered_map, do: 16

  @spec return_ordered_map() :: 17
  def return_ordered_map, do: 17

  @spec return_inverted() :: 0x10000
  def return_inverted, do: @inverted

  @spec put(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def put(%Exp{} = bin, %Exp{} = key, %Exp{} = value, opts \\ []) do
    %{attr: attr, flags: flags} = map_policy(opts)
    modify(bin, @put, [key, value, attr | write_flags(flags)])
  end

  @spec put_items(Exp.t(), Exp.t(), keyword()) :: t()
  def put_items(%Exp{} = bin, %Exp{} = values, opts \\ []) do
    %{attr: attr, flags: flags} = map_policy(opts)
    modify(bin, @put_items, [values, attr | write_flags(flags)])
  end

  @spec increment(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def increment(%Exp{} = bin, %Exp{} = key, %Exp{} = delta, opts \\ []) do
    %{attr: attr} = map_policy(opts)
    modify(bin, @increment, [key, delta, attr])
  end

  @spec clear(Exp.t()) :: t()
  def clear(%Exp{} = bin), do: modify(bin, @clear, [])

  @spec remove_by_key(Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_key(%Exp{} = bin, %Exp{} = key, opts \\ []) do
    modify(bin, @remove_by_key, [key_rt(opts), key])
  end

  @spec remove_by_key_list(Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_key_list(%Exp{} = bin, %Exp{} = keys, opts \\ []) do
    modify(bin, @remove_by_key_list, [key_rt(opts), keys])
  end

  @spec remove_by_key_range(Exp.t(), Exp.t() | nil, Exp.t() | nil, keyword()) :: t()
  def remove_by_key_range(%Exp{} = bin, begin_key, end_key, opts \\ []) do
    modify(bin, @remove_by_key_range, range_args(key_rt(opts), begin_key, end_key))
  end

  @spec remove_by_key_rel_index_range(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_key_rel_index_range(%Exp{} = bin, %Exp{} = key, %Exp{} = index, opts \\ []) do
    modify(bin, @remove_by_key_rel_index_range, [key_rt(opts), key, index])
  end

  @spec remove_by_key_rel_index_range_count(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_key_rel_index_range_count(
        %Exp{} = bin,
        %Exp{} = key,
        %Exp{} = index,
        %Exp{} = count,
        opts \\ []
      ) do
    modify(bin, @remove_by_key_rel_index_range, [key_rt(opts), key, index, count])
  end

  @spec remove_by_value(Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_value(%Exp{} = bin, %Exp{} = value, opts \\ []) do
    modify(bin, @remove_by_value, [value_rt(opts), value])
  end

  @spec remove_by_value_list(Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_value_list(%Exp{} = bin, %Exp{} = values, opts \\ []) do
    modify(bin, @remove_by_value_list, [value_rt(opts), values])
  end

  @spec remove_by_value_range(Exp.t(), Exp.t() | nil, Exp.t() | nil, keyword()) :: t()
  def remove_by_value_range(%Exp{} = bin, begin_value, end_value, opts \\ []) do
    modify(bin, @remove_by_value_range, range_args(value_rt(opts), begin_value, end_value))
  end

  @spec remove_by_value_rel_rank_range(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_value_rel_rank_range(%Exp{} = bin, %Exp{} = value, %Exp{} = rank, opts \\ []) do
    modify(bin, @remove_by_value_rel_rank_range, [value_rt(opts), value, rank])
  end

  @spec remove_by_value_rel_rank_range_count(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) ::
          t()
  def remove_by_value_rel_rank_range_count(
        %Exp{} = bin,
        %Exp{} = value,
        %Exp{} = rank,
        %Exp{} = count,
        opts \\ []
      ) do
    modify(bin, @remove_by_value_rel_rank_range, [value_rt(opts), value, rank, count])
  end

  @spec remove_by_index(Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_index(%Exp{} = bin, %Exp{} = index, opts \\ []) do
    modify(bin, @remove_by_index, [key_rt(opts), index])
  end

  @spec remove_by_index_range_from(Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_index_range_from(%Exp{} = bin, %Exp{} = index, opts \\ []) do
    modify(bin, @remove_by_index_range, [key_rt(opts), index])
  end

  @spec remove_by_index_range(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_index_range(%Exp{} = bin, %Exp{} = index, %Exp{} = count, opts \\ []) do
    modify(bin, @remove_by_index_range, [key_rt(opts), index, count])
  end

  @spec remove_by_rank(Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_rank(%Exp{} = bin, %Exp{} = rank, opts \\ []) do
    modify(bin, @remove_by_rank, [value_rt(opts), rank])
  end

  @spec remove_by_rank_range_from(Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_rank_range_from(%Exp{} = bin, %Exp{} = rank, opts \\ []) do
    modify(bin, @remove_by_rank_range, [value_rt(opts), rank])
  end

  @spec remove_by_rank_range(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def remove_by_rank_range(%Exp{} = bin, %Exp{} = rank, %Exp{} = count, opts \\ []) do
    modify(bin, @remove_by_rank_range, [value_rt(opts), rank, count])
  end

  @spec size(Exp.t()) :: t()
  def size(%Exp{} = bin), do: read(bin, :int, @size, [])

  @spec get_by_key(Exp.t(), Exp.t(), Exp.exp_type(), keyword()) :: t()
  def get_by_key(%Exp{} = bin, %Exp{} = key, value_type, opts \\ []) do
    read(bin, single_return_type(opts, value_type), @get_by_key, [value_rt(opts), key])
  end

  @spec get_by_key_list(Exp.t(), Exp.t(), keyword()) :: t()
  def get_by_key_list(%Exp{} = bin, %Exp{} = keys, opts \\ []) do
    read(bin, map_return_type(opts), @get_by_key_list, [key_value_rt(opts), keys])
  end

  @spec get_by_key_range(Exp.t(), Exp.t() | nil, Exp.t() | nil, keyword()) :: t()
  def get_by_key_range(%Exp{} = bin, begin_key, end_key, opts \\ []) do
    read(
      bin,
      map_return_type(opts),
      @get_by_key_range,
      range_args(key_value_rt(opts), begin_key, end_key)
    )
  end

  @spec get_by_key_rel_index_range(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def get_by_key_rel_index_range(%Exp{} = bin, %Exp{} = key, %Exp{} = index, opts \\ []) do
    read(bin, map_return_type(opts), @get_by_key_rel_index_range, [key_value_rt(opts), key, index])
  end

  @spec get_by_key_rel_index_range_count(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def get_by_key_rel_index_range_count(
        %Exp{} = bin,
        %Exp{} = key,
        %Exp{} = index,
        %Exp{} = count,
        opts \\ []
      ) do
    read(bin, map_return_type(opts), @get_by_key_rel_index_range, [
      key_value_rt(opts),
      key,
      index,
      count
    ])
  end

  @spec get_by_value(Exp.t(), Exp.t(), keyword()) :: t()
  def get_by_value(%Exp{} = bin, %Exp{} = value, opts \\ []) do
    read(bin, map_return_type(opts), @get_by_value, [key_value_rt(opts), value])
  end

  @spec get_by_value_list(Exp.t(), Exp.t(), keyword()) :: t()
  def get_by_value_list(%Exp{} = bin, %Exp{} = values, opts \\ []) do
    read(bin, map_return_type(opts), @get_by_value_list, [key_value_rt(opts), values])
  end

  @spec get_by_value_range(Exp.t(), Exp.t() | nil, Exp.t() | nil, keyword()) :: t()
  def get_by_value_range(%Exp{} = bin, begin_value, end_value, opts \\ []) do
    read(
      bin,
      map_return_type(opts),
      @get_by_value_range,
      range_args(key_value_rt(opts), begin_value, end_value)
    )
  end

  @spec get_by_value_rel_rank_range(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def get_by_value_rel_rank_range(%Exp{} = bin, %Exp{} = value, %Exp{} = rank, opts \\ []) do
    read(bin, map_return_type(opts), @get_by_value_rel_rank_range, [
      key_value_rt(opts),
      value,
      rank
    ])
  end

  @spec get_by_value_rel_rank_range_count(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) ::
          t()
  def get_by_value_rel_rank_range_count(
        %Exp{} = bin,
        %Exp{} = value,
        %Exp{} = rank,
        %Exp{} = count,
        opts \\ []
      ) do
    read(
      bin,
      map_return_type(opts),
      @get_by_value_rel_rank_range,
      [key_value_rt(opts), value, rank, count]
    )
  end

  @spec get_by_index(Exp.t(), Exp.t(), Exp.exp_type(), keyword()) :: t()
  def get_by_index(%Exp{} = bin, %Exp{} = index, value_type, opts \\ []) do
    read(bin, single_return_type(opts, value_type), @get_by_index, [key_value_rt(opts), index])
  end

  @spec get_by_index_range_from(Exp.t(), Exp.t(), keyword()) :: t()
  def get_by_index_range_from(%Exp{} = bin, %Exp{} = index, opts \\ []) do
    read(bin, map_return_type(opts), @get_by_index_range, [key_value_rt(opts), index])
  end

  @spec get_by_index_range(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def get_by_index_range(%Exp{} = bin, %Exp{} = index, %Exp{} = count, opts \\ []) do
    read(bin, map_return_type(opts), @get_by_index_range, [key_value_rt(opts), index, count])
  end

  @spec get_by_rank(Exp.t(), Exp.t(), Exp.exp_type(), keyword()) :: t()
  def get_by_rank(%Exp{} = bin, %Exp{} = rank, value_type, opts \\ []) do
    read(bin, single_return_type(opts, value_type), @get_by_rank, [key_value_rt(opts), rank])
  end

  @spec get_by_rank_range_from(Exp.t(), Exp.t(), keyword()) :: t()
  def get_by_rank_range_from(%Exp{} = bin, %Exp{} = rank, opts \\ []) do
    read(bin, map_return_type(opts), @get_by_rank_range, [key_value_rt(opts), rank])
  end

  @spec get_by_rank_range(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def get_by_rank_range(%Exp{} = bin, %Exp{} = rank, %Exp{} = count, opts \\ []) do
    read(bin, map_return_type(opts), @get_by_rank_range, [key_value_rt(opts), rank, count])
  end

  defp read(bin, type, op_code, args), do: Module.cdt_read(bin, type, op_code, args)
  defp modify(bin, op_code, args), do: Module.cdt_modify(bin, :map, op_code, args, :map)
  defp key_rt(opts), do: Keyword.get(opts, :return_type, return_key())
  defp key_value_rt(opts), do: Keyword.get(opts, :return_type, return_key_value())
  defp value_rt(opts), do: Keyword.get(opts, :return_type, return_value())

  defp map_return_type(opts), do: map_return_type_from_rt(key_value_rt(opts))

  defp single_return_type(opts, value_type),
    do: single_return_type_from_rt(value_rt(opts), value_type)

  defp map_return_type_from_rt(return_type) do
    case return_type &&& bnot(@inverted) do
      value when value in [1, 2, 3, 4, 6, 7] -> :list
      5 -> :int
      value when value in [8, 16, 17] -> :map
      13 -> :bool
    end
  end

  defp single_return_type_from_rt(return_type, value_type) do
    case return_type &&& bnot(@inverted) do
      value when value in [1, 2, 3, 4, 5] -> :int
      6 -> :list
      7 -> value_type
      value when value in [8, 16, 17] -> :map
      13 -> :bool
    end
  end

  defp map_policy(opts), do: Map.merge(%{attr: 0, flags: 0}, Keyword.get(opts, :policy, %{}))
  defp write_flags(0), do: []
  defp write_flags(flags), do: [flags]
  defp range_args(return_type, begin_value, nil), do: [return_type, begin_value]
  defp range_args(return_type, begin_value, end_value), do: [return_type, begin_value, end_value]
end
