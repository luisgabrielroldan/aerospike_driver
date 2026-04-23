defmodule Aerospike.Protocol.UdfArgs do
  @moduledoc false

  alias Aerospike.Protocol.MessagePack

  @spec pack!(list()) :: binary()
  def pack!(args) when is_list(args) do
    args
    |> Enum.map(&pack_arg/1)
    |> MessagePack.pack!()
  end

  @spec pack_arg(term()) :: term()
  def pack_arg(s) when is_binary(s), do: {:particle_string, s}
  def pack_arg({:bytes, b}) when is_binary(b), do: {:bytes, b}
  def pack_arg(nil), do: nil
  def pack_arg(true), do: true
  def pack_arg(false), do: false
  def pack_arg(n) when is_integer(n), do: n
  def pack_arg(f) when is_float(f), do: f
  def pack_arg(list) when is_list(list), do: Enum.map(list, &pack_arg/1)
  def pack_arg(%{} = map), do: Map.new(map, fn {k, v} -> {pack_arg(k), pack_arg(v)} end)
end
