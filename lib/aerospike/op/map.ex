defmodule Aerospike.Op.Map do
  @moduledoc """
  Map CDT operations for `Aerospike.operate/4`.
  """

  alias Aerospike.Protocol.CDT

  @opaque t :: Aerospike.Op.t()

  @put_items 68
  @increment 73
  @size 96
  @get_by_key 97

  @spec put_items(String.t(), map(), keyword()) :: t()
  def put_items(bin_name, values, opts \\ []) when is_binary(bin_name) and is_map(values) do
    CDT.map_modify_op(bin_name, @put_items, [values, 0], Keyword.get(opts, :ctx))
  end

  @spec increment(String.t(), term(), integer(), keyword()) :: t()
  def increment(bin_name, map_key, delta, opts \\ [])
      when is_binary(bin_name) and is_integer(delta) do
    CDT.map_modify_op(bin_name, @increment, [map_key, delta, 0], Keyword.get(opts, :ctx))
  end

  @spec size(String.t(), keyword()) :: t()
  def size(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.map_read_op(bin_name, @size, [], Keyword.get(opts, :ctx))
  end

  @spec get_by_key(String.t(), term(), keyword()) :: t()
  def get_by_key(bin_name, map_key, opts \\ []) when is_binary(bin_name) do
    return_type = Keyword.get(opts, :return_type, 7)
    CDT.map_read_op(bin_name, @get_by_key, [return_type, map_key], Keyword.get(opts, :ctx))
  end
end
