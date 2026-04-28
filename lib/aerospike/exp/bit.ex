defmodule Aerospike.Exp.Bit do
  @moduledoc """
  Bit expression helpers for blob expressions.
  """

  alias Aerospike.Exp
  alias Aerospike.Exp.Module
  alias Aerospike.PolicyInteger

  @typedoc "Opaque server-side expression."
  @type t :: Exp.t()

  @typedoc """
  Bit expression modify options.

  Supported keys:

  * `:flags` - bit-operation write flags. Defaults to `:default`.
  * `:overflow_action` - overflow behavior for `add/5` and `subtract/5`.
  * `:signed` - when true, adds the signed flag to `:overflow_action`.
  """
  @type opts :: [
          flags: atom() | [atom()] | non_neg_integer() | {:raw, non_neg_integer()},
          overflow_action: atom() | non_neg_integer() | {:raw, non_neg_integer()},
          signed: boolean()
        ]

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

  @doc "Resizes the blob expression to `byte_size` bytes using `resize_flags`."
  @spec resize(
          Exp.t(),
          Exp.t(),
          atom() | [atom()] | non_neg_integer() | {:raw, non_neg_integer()},
          opts()
        ) :: t()
  def resize(%Exp{} = bin, %Exp{} = byte_size, resize_flags \\ :default, opts \\ []) do
    modify(bin, @resize, [byte_size, flags(opts), PolicyInteger.bit_resize_flags(resize_flags)])
  end

  @doc "Inserts a byte-string expression at `byte_offset`."
  @spec insert(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def insert(%Exp{} = bin, %Exp{} = byte_offset, %Exp{} = value, opts \\ []) do
    modify(bin, @insert, [byte_offset, value, flags(opts)])
  end

  @doc "Removes `byte_size` bytes starting at `byte_offset`."
  @spec remove(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def remove(%Exp{} = bin, %Exp{} = byte_offset, %Exp{} = byte_size, opts \\ []) do
    modify(bin, @remove, [byte_offset, byte_size, flags(opts)])
  end

  @doc "Overwrites a bit range with bits from `value`."
  @spec set(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def set(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @set, [bit_offset, bit_size, value, flags(opts)])
  end

  @doc "Applies bitwise OR of `value` into the selected bit range."
  @spec bw_or(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def bw_or(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @bw_or, [bit_offset, bit_size, value, flags(opts)])
  end

  @doc "Applies bitwise XOR of `value` into the selected bit range."
  @spec bw_xor(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def bw_xor(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @bw_xor, [bit_offset, bit_size, value, flags(opts)])
  end

  @doc "Applies bitwise AND of `value` into the selected bit range."
  @spec bw_and(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def bw_and(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @bw_and, [bit_offset, bit_size, value, flags(opts)])
  end

  @doc "Inverts the selected bit range."
  @spec bw_not(Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def bw_not(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, opts \\ []) do
    modify(bin, @bw_not, [bit_offset, bit_size, flags(opts)])
  end

  @doc "Left shifts the selected bit range by `shift` bits."
  @spec lshift(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def lshift(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = shift, opts \\ []) do
    modify(bin, @lshift, [bit_offset, bit_size, shift, flags(opts)])
  end

  @doc "Right shifts the selected bit range by `shift` bits."
  @spec rshift(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def rshift(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = shift, opts \\ []) do
    modify(bin, @rshift, [bit_offset, bit_size, shift, flags(opts)])
  end

  @doc """
  Adds `value` to a sub-integer in the bitmap.

  Supports `flags:`, `overflow_action:`, and `signed:`.
  """
  @spec add(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def add(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @add, [bit_offset, bit_size, value, flags(opts), overflow_action(opts)])
  end

  @doc """
  Subtracts `value` from a sub-integer in the bitmap.

  Supports `flags:`, `overflow_action:`, and `signed:`.
  """
  @spec subtract(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def subtract(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @subtract, [bit_offset, bit_size, value, flags(opts), overflow_action(opts)])
  end

  @doc "Writes integer `value` into the selected bit range."
  @spec set_int(Exp.t(), Exp.t(), Exp.t(), Exp.t(), opts()) :: t()
  def set_int(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value, opts \\ []) do
    modify(bin, @set_int, [bit_offset, bit_size, value, flags(opts)])
  end

  @doc "Returns the selected bits as a blob expression."
  @spec get(Exp.t(), Exp.t(), Exp.t()) :: t()
  def get(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size) do
    read(bin, :blob, @get, [bit_offset, bit_size])
  end

  @doc "Returns the count of bits set to `1` in the selected range."
  @spec count(Exp.t(), Exp.t(), Exp.t()) :: t()
  def count(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size) do
    read(bin, :int, @count, [bit_offset, bit_size])
  end

  @doc "Returns the offset of the first matching bit when scanning left to right."
  @spec lscan(Exp.t(), Exp.t(), Exp.t(), Exp.t()) :: t()
  def lscan(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value) do
    read(bin, :int, @lscan, [bit_offset, bit_size, value])
  end

  @doc "Returns the offset of the first matching bit when scanning right to left."
  @spec rscan(Exp.t(), Exp.t(), Exp.t(), Exp.t()) :: t()
  def rscan(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, %Exp{} = value) do
    read(bin, :int, @rscan, [bit_offset, bit_size, value])
  end

  @doc "Reads an integer from the selected bit range, optionally as signed."
  @spec get_int(Exp.t(), Exp.t(), Exp.t(), boolean()) :: t()
  def get_int(%Exp{} = bin, %Exp{} = bit_offset, %Exp{} = bit_size, signed \\ false) do
    args =
      if signed do
        [bit_offset, bit_size, PolicyInteger.bit_signed_flag()]
      else
        [bit_offset, bit_size]
      end

    read(bin, :int, @get_int, args)
  end

  defp read(bin, type, op_code, args), do: Module.bit_read(bin, type, op_code, args)
  defp modify(bin, op_code, args), do: Module.bit_modify(bin, op_code, args)
  defp flags(opts), do: opts |> Keyword.get(:flags, :default) |> PolicyInteger.bit_write_flags()

  defp overflow_action(opts),
    do:
      PolicyInteger.bit_overflow_action(
        Keyword.get(opts, :overflow_action, :default),
        signed?(opts)
      )

  defp signed?(opts), do: Keyword.get(opts, :signed, false) == true
end
