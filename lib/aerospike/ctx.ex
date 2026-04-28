defmodule Aerospike.Ctx do
  @moduledoc """
  Context path steps for nested CDT operations.

  Each helper returns one `{context_id, value}` pair that can be passed as
  part of a `ctx` list to CDT helpers.
  """

  alias Aerospike.Exp
  alias Aerospike.Protocol.CDT
  alias Aerospike.Protocol.MessagePack

  @typedoc "A single nested-CDT navigation step."
  @type step :: {non_neg_integer(), term()}

  @typedoc """
  Nested CDT context path.

  Pass this list as `ctx:` to CDT operation builders that support nested
  collection access.
  """
  @type t :: [step()]

  @typedoc """
  List order used when a context step creates a nested list.

  `:unordered` creates a normal list. `:ordered` creates an ordered list.
  """
  @type list_order :: :unordered | :ordered

  @typedoc """
  Map order used when a context step creates a nested map.

  `:unordered` keeps no ordering, `:key_ordered` orders entries by key, and
  `:key_value_ordered` orders by key and value.
  """
  @type map_order :: :unordered | :key_ordered | :key_value_ordered

  @expression 0x04
  @list_index 0x10
  @list_rank 0x11
  @list_value 0x13
  @map_index 0x20
  @map_rank 0x21
  @map_key 0x22
  @map_value 0x23

  @doc """
  Navigate to every child of the current collection.
  """
  @spec all_children() :: step()
  def all_children, do: all_children_with_filter(Exp.bool(true))

  @doc """
  Navigate to children of the current collection that match `expression`.

  The expression is evaluated with Aerospike's built-in loop variables, such as
  `Aerospike.Exp.int_loop_var(:value)`, bound to each candidate child.
  """
  @spec all_children_with_filter(Exp.t()) :: step()
  def all_children_with_filter(%Exp{} = expression), do: {@expression, expression}

  @doc "Navigate into a list by absolute index."
  @spec list_index(integer()) :: step()
  def list_index(index) when is_integer(index), do: {@list_index, index}

  @doc """
  Navigate into a list by index, creating a nested list when it is missing.

  `order` controls the created list order. When `pad` is `true` and `index` is
  past the current end of the list, nil elements are inserted before the new
  list.
  """
  @spec list_index_create(integer(), list_order(), boolean()) :: step()
  def list_index_create(index, order \\ :unordered, pad \\ false)
      when is_integer(index) and is_boolean(pad) do
    {Bitwise.bor(@list_index, list_order_flag(order, pad)), index}
  end

  @doc "Navigate into a list by rank."
  @spec list_rank(integer()) :: step()
  def list_rank(rank) when is_integer(rank), do: {@list_rank, rank}

  @doc "Navigate to the list element equal to `value`."
  @spec list_value(term()) :: step()
  def list_value(value), do: {@list_value, value}

  @doc "Navigate into a map by entry index."
  @spec map_index(integer()) :: step()
  def map_index(index) when is_integer(index), do: {@map_index, index}

  @doc "Navigate into a map by value rank."
  @spec map_rank(integer()) :: step()
  def map_rank(rank) when is_integer(rank), do: {@map_rank, rank}

  @doc "Navigate into the map entry for `key`."
  @spec map_key(term()) :: step()
  def map_key(key), do: {@map_key, key}

  @doc """
  Navigate into a map by key, creating a nested map when it is missing.
  """
  @spec map_key_create(term(), map_order()) :: step()
  def map_key_create(key, order \\ :unordered),
    do: {Bitwise.bor(@map_key, map_order_flag(order)), key}

  @doc "Navigate to the map entry whose value equals `value`."
  @spec map_value(term()) :: step()
  def map_value(value), do: {@map_value, value}

  @doc "Serializes a context path to MessagePack bytes."
  @spec to_bytes(t()) :: binary()
  def to_bytes(ctx) when is_list(ctx), do: CDT.encode_ctx(ctx)

  @doc "Deserializes MessagePack context bytes."
  @spec from_bytes(binary()) :: t()
  def from_bytes(bytes) when is_binary(bytes) do
    bytes
    |> MessagePack.unpack!()
    |> from_flat_terms()
  end

  @doc "Serializes a context path to Base64-encoded MessagePack bytes."
  @spec to_base64(t()) :: String.t()
  def to_base64(ctx) when is_list(ctx), do: ctx |> to_bytes() |> Base.encode64()

  @doc "Deserializes a Base64-encoded context path."
  @spec from_base64(String.t()) :: t()
  def from_base64(encoded) when is_binary(encoded) do
    encoded
    |> Base.decode64!()
    |> from_bytes()
  end

  defp list_order_flag(:ordered, _pad), do: 0xC0
  defp list_order_flag(:unordered, true), do: 0x80
  defp list_order_flag(:unordered, false), do: 0x40

  defp map_order_flag(:unordered), do: 0x40
  defp map_order_flag(:key_ordered), do: 0x80
  defp map_order_flag(:key_value_ordered), do: 0xC0

  defp from_flat_terms(terms) when is_list(terms), do: from_flat_terms(terms, [])

  defp from_flat_terms([], acc), do: Enum.reverse(acc)

  defp from_flat_terms([@expression, wire | rest], acc) when is_binary(wire) do
    from_flat_terms(rest, [{@expression, Exp.from_wire(wire)} | acc])
  end

  defp from_flat_terms([id, value | rest], acc) when is_integer(id) do
    from_flat_terms(rest, [{id, value} | acc])
  end

  defp from_flat_terms(_other, _acc),
    do: raise(ArgumentError, "context bytes must contain id/value pairs")
end
