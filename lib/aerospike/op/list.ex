defmodule Aerospike.Op.List do
  @moduledoc """
  List CDT operations for `Aerospike.operate/4`.
  """

  alias Aerospike.Protocol.CDT

  @opaque t :: Aerospike.Op.t()

  @append 1
  @size 16
  @get 17

  @spec append(String.t(), term(), keyword()) :: t()
  def append(bin_name, value, opts \\ []) when is_binary(bin_name) do
    CDT.list_modify_op(bin_name, @append, [value], Keyword.get(opts, :ctx))
  end

  @spec size(String.t(), keyword()) :: t()
  def size(bin_name, opts \\ []) when is_binary(bin_name) do
    CDT.list_read_op(bin_name, @size, [], Keyword.get(opts, :ctx))
  end

  @spec get(String.t(), integer(), keyword()) :: t()
  def get(bin_name, index, opts \\ [])
      when is_binary(bin_name) and is_integer(index) do
    CDT.list_read_op(bin_name, @get, [index], Keyword.get(opts, :ctx))
  end
end
