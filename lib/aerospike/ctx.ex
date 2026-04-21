defmodule Aerospike.Ctx do
  @moduledoc """
  Context path steps for nested CDT operations.

  Each helper returns one `{context_id, value}` pair that can be passed as
  part of a `ctx` list to CDT helpers.
  """

  @typedoc "A single nested-CDT navigation step."
  @type step :: {non_neg_integer(), term()}

  @list_index 0x10
  @list_rank 0x11
  @list_value 0x13
  @map_index 0x20
  @map_rank 0x21
  @map_key 0x22
  @map_value 0x23

  @doc "Navigate into a list by absolute index."
  @spec list_index(integer()) :: step()
  def list_index(index) when is_integer(index), do: {@list_index, index}

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

  @doc "Navigate to the map entry whose value equals `value`."
  @spec map_value(term()) :: step()
  def map_value(value), do: {@map_value, value}
end
