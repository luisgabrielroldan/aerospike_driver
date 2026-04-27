defmodule Aerospike.Exp.Bit do
  @moduledoc """
  Bit expression helpers for blob expressions.
  """

  import Bitwise

  alias Aerospike.Exp
  alias Aerospike.Exp.Module

  @typedoc "Opaque server-side expression."
  @type t :: Exp.t()

  @resize 0
  @insert 1
  @remove 2
  @set 3
  @bw_or 4
  @bw_xor 5
  @bw_and 6
  @bw_not 7
  @lshift 8
  @rshift 9
  @add 10
  @subtract 11
  @set_int 12
  @get 50
  @count 51
  @lscan 52
  @rscan 53
  @get_int 54

  @signed_flag 1

  @spec resize(Exp.t(), Exp.t(), integer(), keyword()) :: t()
  def resize(%Exp{} = bin, %Exp{} = byte_size, resize_flags \\ 0, opts \\ []) do
    modify(bin, @resize, [byte_size, flags(opts), resize_flags])
  end

  @spec insert(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def insert(%Exp{} = bin, %Exp{} = byte_offset, %Exp{} = value, opts \\ []) do
    modify(bin, @insert, [byte_offset, value, flags(opts)])
  end

  @spec remove(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def remove(%Exp{} = bin, %Exp{} = byte_offset, %Exp{} = byte_size, opts \\ []) do
    modify(bin, @remove, [byte_offset, byte_size, flags(opts)])
  end

  @spec set(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def set(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @set, [bit_offset, bit_size, value, flags(opts)])
  end

  @spec bw_or(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def bw_or(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @bw_or, [bit_offset, bit_size, value, flags(opts)])
  end

  @spec bw_xor(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def bw_xor(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @bw_xor, [bit_offset, bit_size, value, flags(opts)])
  end

  @spec bw_and(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def bw_and(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @bw_and, [bit_offset, bit_size, value, flags(opts)])
  end

  @spec bw_not(Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def bw_not(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, opts \\ []) do
    modify(bin, @bw_not, [bit_offset, bit_size, flags(opts)])
  end

  @spec lshift(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def lshift(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = shift, opts \\ []) do
    modify(bin, @lshift, [bit_offset, bit_size, shift, flags(opts)])
  end

  @spec rshift(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def rshift(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = shift, opts \\ []) do
    modify(bin, @rshift, [bit_offset, bit_size, shift, flags(opts)])
  end

  @spec add(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def add(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @add, [bit_offset, bit_size, value, flags(opts), overflow_action(opts)])
  end

  @spec subtract(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def subtract(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @subtract, [bit_offset, bit_size, value, flags(opts), overflow_action(opts)])
  end

  @spec set_int(Exp.t(), Exp.t(), Exp.t(), Exp.t(), keyword()) :: t()
  def set_int(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @set_int, [bit_offset, bit_size, value, flags(opts)])
  end

  @spec get(Exp.t(), Exp.t(), Exp.t()) :: t()
  def get(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size) do
    read(bin, :blob, @get, [bit_offset, bit_size])
  end

  @spec count(Exp.t(), Exp.t(), Exp.t()) :: t()
  def count(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size) do
    read(bin, :int, @count, [bit_offset, bit_size])
  end

  @spec lscan(Exp.t(), Exp.t(), Exp.t(), Exp.t()) :: t()
  def lscan(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value) do
    read(bin, :int, @lscan, [bit_offset, bit_size, value])
  end

  @spec rscan(Exp.t(), Exp.t(), Exp.t(), Exp.t()) :: t()
  def rscan(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value) do
    read(bin, :int, @rscan, [bit_offset, bit_size, value])
  end

  @spec get_int(Exp.t(), Exp.t(), Exp.t(), boolean()) :: t()
  def get_int(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, signed \\ false) do
    args =
      if signed do
        [bit_offset, bit_size, @signed_flag]
      else
        [bit_offset, bit_size]
      end

    read(bin, :int, @get_int, args)
  end

  defp read(bin, type, op_code, args), do: Module.bit_read(bin, type, op_code, args)
  defp modify(bin, op_code, args), do: Module.bit_modify(bin, op_code, args)
  defp flags(opts), do: Keyword.get(opts, :flags, 0)

  defp overflow_action(opts) do
    action = Keyword.get(opts, :overflow_action, 0)

    if Keyword.get(opts, :signed, false) do
      action ||| @signed_flag
    else
      action
    end
  end
end
