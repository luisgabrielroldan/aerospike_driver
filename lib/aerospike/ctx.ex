defmodule Aerospike.Ctx do
  @moduledoc """
  Context path steps for nested Collection Data Type (CDT) operations.

  Aerospike stores lists and maps inside bins; a **context path** selects where a
  list or map operation applies when the structure is nested (e.g. a list inside
  a map). Pass a list of steps via the `ctx:` option on `Aerospike.Op.List`,
  `Aerospike.Op.Map`, or `Aerospike.Op.Bit` functions.

  Each step is a `{context_id, value}` pair. This module provides constructors so
  callers do not need to remember wire constants.

  ## Example

      alias Aerospike.Op.Map, as: MapOp

      ctx = [Aerospike.Ctx.map_key("profile"), Aerospike.Ctx.map_key("geo")]

      MyApp.Repo.operate(key, [
        MapOp.put("nested", "lat", 45.52, ctx: ctx)
      ])

  ## Related modules

  - `Aerospike.Op.List` — list CDT operations
  - `Aerospike.Op.Map` — map CDT operations
  - `MyApp.Repo.operate/2,3` — recommended application-facing execution path
  - `Aerospike.operate/4` — low-level atomic execution on one record
  """

  @typedoc "A single nested-CDT navigation step: `{wire_context_id, key_or_index_value}`."
  @type step :: {non_neg_integer(), term()}

  # IDs match `official_libs/aerospike-client-go/cdt_context.go` (not the older 0x18..0x1B range).

  @list_index 0x10
  @list_rank 0x11
  @list_value 0x13
  @map_index 0x20
  @map_rank 0x21
  @map_key 0x22
  @map_value 0x23

  @doc """
  Navigate into a list by absolute index (negative indexes count from the end).
  """
  @spec list_index(integer()) :: step()
  def list_index(index) when is_integer(index), do: {@list_index, index}

  @doc """
  Navigate into a list by rank (sorted order position).
  """
  @spec list_rank(integer()) :: step()
  def list_rank(rank) when is_integer(rank), do: {@list_rank, rank}

  @doc """
  Navigate to the list element equal to `value` (first match).
  """
  @spec list_value(term()) :: step()
  def list_value(value), do: {@list_value, value}

  @doc """
  Navigate into a map by entry index (unordered offset).
  """
  @spec map_index(integer()) :: step()
  def map_index(index) when is_integer(index), do: {@map_index, index}

  @doc """
  Navigate into a map by value rank.
  """
  @spec map_rank(integer()) :: step()
  def map_rank(rank) when is_integer(rank), do: {@map_rank, rank}

  @doc """
  Navigate into the map entry for `key` (string, integer, or blob `{:bytes, binary}`).
  """
  @spec map_key(term()) :: step()
  def map_key(key), do: {@map_key, key}

  @doc """
  Navigate to the map entry whose value equals `value`.
  """
  @spec map_value(term()) :: step()
  def map_value(value), do: {@map_value, value}
end
