defmodule Aerospike.PolicyInteger do
  @moduledoc false

  import Bitwise

  @type raw_integer :: non_neg_integer() | {:raw, non_neg_integer()}
  @type flag_input :: atom() | [atom()] | raw_integer()
  @type enum_input :: atom() | raw_integer()
  @type policy_input :: map() | keyword() | nil

  @inverted 0x10000
  @persist_index 0x10
  @bit_signed 1

  @list_return_types %{
    none: 0,
    index: 1,
    reverse_index: 2,
    rank: 3,
    reverse_rank: 4,
    count: 5,
    value: 7,
    exists: 13,
    inverted: @inverted
  }

  @map_return_types Map.merge(@list_return_types, %{
                      key: 6,
                      key_value: 8,
                      unordered_map: 16,
                      ordered_map: 17
                    })

  @list_orders %{unordered: 0, ordered: 1}
  @map_orders %{unordered: 0, key_ordered: 1, key_value_ordered: 3}

  @list_write_flags %{
    default: 0,
    add_unique: 1,
    insert_bounded: 2,
    no_fail: 4,
    partial: 8
  }

  @write_flags %{
    default: 0,
    create_only: 1,
    update_only: 2,
    no_fail: 4,
    partial: 8
  }

  @hll_write_flags %{
    default: 0,
    create_only: 1,
    update_only: 2,
    no_fail: 4,
    allow_fold: 8
  }

  @list_sort_flags %{
    default: 0,
    descending: 1,
    drop_duplicates: 2
  }

  @bit_resize_flags %{
    default: 0,
    from_front: 1,
    grow_only: 2,
    shrink_only: 4
  }

  @bit_overflow_actions %{
    fail: 0,
    default: 0,
    saturate: 2,
    wrap: 4
  }

  @exp_read_flags %{
    default: 0,
    eval_no_fail: 16
  }

  @exp_write_flags %{
    default: 0,
    create_only: 1,
    update_only: 2,
    allow_delete: 4,
    policy_no_fail: 8,
    eval_no_fail: 16
  }

  @spec inverted() :: 0x10000
  def inverted, do: @inverted

  @spec persist_index() :: 0x10
  def persist_index, do: @persist_index

  @spec bit_signed_flag() :: 1
  def bit_signed_flag, do: @bit_signed

  @spec list_return_type(flag_input()) :: non_neg_integer()
  def list_return_type(value), do: flags(:list_return_type, value, @list_return_types)

  @spec map_return_type(flag_input()) :: non_neg_integer()
  def map_return_type(value), do: flags(:map_return_type, value, @map_return_types)

  @spec list_order(enum_input()) :: non_neg_integer()
  def list_order(value), do: enum(:list_order, value, @list_orders)

  @spec map_order(enum_input()) :: non_neg_integer()
  def map_order(value), do: enum(:map_order, value, @map_orders)

  @spec list_write_flags(flag_input()) :: non_neg_integer()
  def list_write_flags(value), do: flags(:list_write_flags, value, @list_write_flags)

  @spec map_write_flags(flag_input()) :: non_neg_integer()
  def map_write_flags(value), do: flags(:map_write_flags, value, @write_flags)

  @spec bit_write_flags(flag_input()) :: non_neg_integer()
  def bit_write_flags(value), do: flags(:bit_write_flags, value, @write_flags)

  @spec hll_write_flags(flag_input()) :: non_neg_integer()
  def hll_write_flags(value), do: flags(:hll_write_flags, value, @hll_write_flags)

  @spec list_sort_flags(flag_input()) :: non_neg_integer()
  def list_sort_flags(value), do: flags(:list_sort_flags, value, @list_sort_flags)

  @spec bit_resize_flags(flag_input()) :: non_neg_integer()
  def bit_resize_flags(value), do: flags(:bit_resize_flags, value, @bit_resize_flags)

  @spec bit_overflow_action(enum_input(), boolean()) :: non_neg_integer()
  def bit_overflow_action(value, signed \\ false)

  def bit_overflow_action(value, true),
    do: enum(:bit_overflow_action, value, @bit_overflow_actions) ||| @bit_signed

  def bit_overflow_action(value, false),
    do: enum(:bit_overflow_action, value, @bit_overflow_actions)

  @spec exp_read_flags(flag_input()) :: non_neg_integer()
  def exp_read_flags(value), do: flags(:exp_read_flags, value, @exp_read_flags)

  @spec exp_write_flags(flag_input()) :: non_neg_integer()
  def exp_write_flags(value), do: flags(:exp_write_flags, value, @exp_write_flags)

  @spec list_policy(policy_input()) :: %{order: non_neg_integer(), flags: non_neg_integer()}
  def list_policy(policy) do
    policy = policy_map(policy)

    %{
      order: list_order(Map.get(policy, :order, 0)),
      flags: list_write_flags(Map.get(policy, :flags, 0))
    }
  end

  @spec map_policy(policy_input()) :: %{attr: non_neg_integer(), flags: non_neg_integer()}
  def map_policy(policy) do
    policy = policy_map(policy)
    order = Map.get(policy, :order, Map.get(policy, :attr, 0))

    %{
      attr: map_order(order),
      flags: map_write_flags(Map.get(policy, :flags, 0))
    }
  end

  @spec list_order_attr(enum_input(), boolean()) :: non_neg_integer()
  def list_order_attr(order, false), do: list_order(order)
  def list_order_attr(order, true), do: list_order(order) ||| @persist_index

  @spec map_order_attr(enum_input(), boolean()) :: non_neg_integer()
  def map_order_attr(order, false), do: map_order(order)
  def map_order_attr(order, true), do: map_order(order) ||| @persist_index

  @spec strip_persist_index(non_neg_integer()) :: non_neg_integer()
  def strip_persist_index(value) when is_integer(value) and value >= 0,
    do: value &&& bnot(@persist_index)

  @spec list_create_flag(enum_input(), boolean()) :: 0x40 | 0x80 | 0xC0
  def list_create_flag(order, pad \\ false)

  def list_create_flag(order, pad) do
    case list_order(order) do
      0 when pad == true -> 0x80
      0 -> 0x40
      1 -> 0xC0
      value -> raise_invalid(:list_create_flag_order, value)
    end
  end

  @spec map_create_flag(enum_input()) :: 0x40 | 0x80 | 0xC0
  def map_create_flag(value)

  def map_create_flag(order) do
    case map_order(order) do
      0 -> 0x40
      1 -> 0x80
      3 -> 0xC0
      value -> raise_invalid(:map_create_flag_order, value)
    end
  end

  defp enum(_name, {:raw, value}, _values) when is_integer(value) and value >= 0, do: value
  defp enum(_name, value, _values) when is_integer(value) and value >= 0, do: value

  defp enum(name, atom, values) when is_atom(atom) do
    case Map.fetch(values, atom) do
      {:ok, value} -> value
      :error -> raise_unknown(name, atom)
    end
  end

  defp enum(name, value, _values), do: raise_invalid(name, value)

  defp flags(_name, {:raw, value}, _values) when is_integer(value) and value >= 0, do: value
  defp flags(_name, value, _values) when is_integer(value) and value >= 0, do: value

  defp flags(name, atoms, values) when is_list(atoms) do
    Enum.reduce(atoms, 0, fn atom, acc -> acc ||| enum(name, atom, values) end)
  end

  defp flags(name, value, values), do: enum(name, value, values)

  defp policy_map(nil), do: %{}
  defp policy_map(policy) when is_map(policy), do: policy
  defp policy_map(policy) when is_list(policy), do: Map.new(policy)
  defp policy_map(policy), do: raise_invalid(:policy, policy)

  defp raise_unknown(name, value) do
    raise ArgumentError,
          "unknown #{format_name(name)} #{inspect(value)}"
  end

  defp raise_invalid(name, value) do
    raise ArgumentError,
          "invalid #{format_name(name)} #{inspect(value)}"
  end

  defp format_name(name), do: name |> Atom.to_string() |> String.replace("_", " ")
end
