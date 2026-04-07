defmodule Aerospike.Op.Bit do
  import Bitwise

  @moduledoc """
  Bitwise operations on a byte-array bin for `Aerospike.operate/4`.

  Offsets are read left-to-right within the bin. **Negative** bit or byte offsets count
  backward from the end of the bitmap. Out-of-bounds offsets produce a parameter error.

  Offsets are in **bits** for `set/5`, `bw_or/5`, `bw_xor/5`, `bw_and/5`, `bw_not/4`,
  `lshift/5`, `rshift/5`, `add/5`, `subtract/5`, `get/4`, `count/4`, `lscan/5`, `rscan/5`,
  and `get_int/5`. `resize/4`, `insert/4`, and `remove/4` use **byte** offsets and sizes.

  Pass `ctx:` for nested bitmaps (see `Aerospike.Ctx`) and `flags:` for the server policy
  integer (default `0`).
  """

  alias Aerospike.Protocol.CDT

  @typedoc """
  Opaque bitwise operation for `Aerospike.operate/4`.

  Construct via `Aerospike.Op.Bit.*` builder functions and pass in an operation list.
  """
  @opaque t :: Aerospike.Op.t()

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

  defp ctx(opts), do: Keyword.get(opts, :ctx)
  defp flags(opts), do: Keyword.get(opts, :flags, 0)

  @doc """
  Resizes the bin to `byte_size` bytes using `resize_flags`.

  Server does not return a value. See Aerospike bit resize flags for `resize_flags` values.
  """
  @spec resize(String.t(), integer(), integer(), keyword()) :: t()
  def resize(bin_name, byte_size, resize_flags \\ 0, opts \\ [])
      when is_binary(bin_name) and is_integer(byte_size) and is_integer(resize_flags) do
    CDT.bit_modify_op(bin_name, @resize, [byte_size, flags(opts), resize_flags], ctx(opts))
  end

  @doc """
  Inserts `bytes` at `byte_offset` in the bin.

  Server does not return a value.
  """
  @spec insert(String.t(), integer(), binary(), keyword()) :: t()
  def insert(bin_name, byte_offset, bytes, opts \\ [])
      when is_binary(bin_name) and is_integer(byte_offset) and is_binary(bytes) do
    CDT.bit_modify_op(bin_name, @insert, [byte_offset, {:bytes, bytes}, flags(opts)], ctx(opts))
  end

  @doc """
  Removes `byte_size` bytes starting at `byte_offset`.

  Server does not return a value.
  """
  @spec remove(String.t(), integer(), integer(), keyword()) :: t()
  def remove(bin_name, byte_offset, byte_size, opts \\ [])
      when is_binary(bin_name) and is_integer(byte_offset) and is_integer(byte_size) do
    CDT.bit_modify_op(bin_name, @remove, [byte_offset, byte_size, flags(opts)], ctx(opts))
  end

  @doc """
  Overwrites the bit region `[bit_offset, bit_offset + bit_size)` with bits from `value`.

  Server does not return a value.
  """
  @spec set(String.t(), integer(), integer(), binary(), keyword()) :: t()
  def set(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_binary(value) do
    CDT.bit_modify_op(
      bin_name,
      @set,
      [bit_offset, bit_size, {:bytes, value}, flags(opts)],
      ctx(opts)
    )
  end

  @doc "Bitwise OR of `value` into the region `[bit_offset, bit_offset + bit_size)`."
  @spec bw_or(String.t(), integer(), integer(), binary(), keyword()) :: t()
  def bw_or(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_binary(value) do
    CDT.bit_modify_op(
      bin_name,
      @bw_or,
      [bit_offset, bit_size, {:bytes, value}, flags(opts)],
      ctx(opts)
    )
  end

  @doc """
  Bitwise XOR of `value` into `[bit_offset, bit_offset + bit_size)`.

  Server does not return a value.
  """
  @spec bw_xor(String.t(), integer(), integer(), binary(), keyword()) :: t()
  def bw_xor(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_binary(value) do
    CDT.bit_modify_op(
      bin_name,
      @bw_xor,
      [bit_offset, bit_size, {:bytes, value}, flags(opts)],
      ctx(opts)
    )
  end

  @doc """
  Bitwise AND of `value` into `[bit_offset, bit_offset + bit_size)`.

  Server does not return a value.
  """
  @spec bw_and(String.t(), integer(), integer(), binary(), keyword()) :: t()
  def bw_and(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_binary(value) do
    CDT.bit_modify_op(
      bin_name,
      @bw_and,
      [bit_offset, bit_size, {:bytes, value}, flags(opts)],
      ctx(opts)
    )
  end

  @doc """
  Bitwise NOT (invert) for `[bit_offset, bit_offset + bit_size)`.

  Server does not return a value.
  """
  @spec bw_not(String.t(), integer(), integer(), keyword()) :: t()
  def bw_not(bin_name, bit_offset, bit_size, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) do
    CDT.bit_modify_op(bin_name, @bw_not, [bit_offset, bit_size, flags(opts)], ctx(opts))
  end

  @doc """
  Left-shifts the bit region `[bit_offset, bit_offset + bit_size)` by `shift` bits.

  Server does not return a value.
  """
  @spec lshift(String.t(), integer(), integer(), integer(), keyword()) :: t()
  def lshift(bin_name, bit_offset, bit_size, shift, opts \\ [])
      when is_binary(bin_name) and is_integer(shift) do
    CDT.bit_modify_op(bin_name, @lshift, [bit_offset, bit_size, shift, flags(opts)], ctx(opts))
  end

  @doc """
  Right-shifts the bit region `[bit_offset, bit_offset + bit_size)` by `shift` bits.

  Server does not return a value.
  """
  @spec rshift(String.t(), integer(), integer(), integer(), keyword()) :: t()
  def rshift(bin_name, bit_offset, bit_size, shift, opts \\ [])
      when is_binary(bin_name) and is_integer(shift) do
    CDT.bit_modify_op(bin_name, @rshift, [bit_offset, bit_size, shift, flags(opts)], ctx(opts))
  end

  @doc """
  Add an integer to a sub-integer in the bitmap (`bit_size` ≤ 64).

  Options: `:signed` (default `false`), `:overflow_action` (default `0`, OR with signed flag when `:signed` is true).
  """
  @spec add(String.t(), integer(), integer(), integer(), keyword()) :: t()
  def add(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(value) do
    signed = Keyword.get(opts, :signed, false)
    overflow = Keyword.get(opts, :overflow_action, 0)
    action = if signed, do: overflow ||| @signed_flag, else: overflow

    CDT.bit_modify_op(
      bin_name,
      @add,
      [bit_offset, bit_size, value, flags(opts), action],
      ctx(opts)
    )
  end

  @doc "Subtract — same options as `add/5`."
  @spec subtract(String.t(), integer(), integer(), integer(), keyword()) :: t()
  def subtract(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(value) do
    signed = Keyword.get(opts, :signed, false)
    overflow = Keyword.get(opts, :overflow_action, 0)
    action = if signed, do: overflow ||| @signed_flag, else: overflow

    CDT.bit_modify_op(
      bin_name,
      @subtract,
      [bit_offset, bit_size, value, flags(opts), action],
      ctx(opts)
    )
  end

  @doc """
  Writes integer `value` into `[bit_offset, bit_offset + bit_size)` (`bit_size` ≤ 64).

  Server does not return a value.
  """
  @spec set_int(String.t(), integer(), integer(), integer(), keyword()) :: t()
  def set_int(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(value) do
    CDT.bit_modify_op(bin_name, @set_int, [bit_offset, bit_size, value, flags(opts)], ctx(opts))
  end

  @doc """
  Returns `bit_size` bits starting at `bit_offset` as a binary.
  """
  @spec get(String.t(), integer(), integer(), keyword()) :: t()
  def get(bin_name, bit_offset, bit_size, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) do
    CDT.bit_read_op(bin_name, @get, [bit_offset, bit_size], ctx(opts))
  end

  @doc """
  Returns the count of bits set to `1` in `[bit_offset, bit_offset + bit_size)`.
  """
  @spec count(String.t(), integer(), integer(), keyword()) :: t()
  def count(bin_name, bit_offset, bit_size, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) do
    CDT.bit_read_op(bin_name, @count, [bit_offset, bit_size], ctx(opts))
  end

  @doc """
  Returns the bit offset of the first `value` bit (`true` = 1, `false` = 0) in the region,
  scanning left to right from `bit_offset` for `bit_size` bits.
  """
  @spec lscan(String.t(), integer(), integer(), boolean(), keyword()) :: t()
  def lscan(bin_name, bit_offset, bit_size, value, opts \\ []) when is_binary(bin_name) do
    CDT.bit_read_op(bin_name, @lscan, [bit_offset, bit_size, value], ctx(opts))
  end

  @doc """
  Returns the bit offset of the last `value` bit in the region, scanning right to left.
  """
  @spec rscan(String.t(), integer(), integer(), boolean(), keyword()) :: t()
  def rscan(bin_name, bit_offset, bit_size, value, opts \\ []) when is_binary(bin_name) do
    CDT.bit_read_op(bin_name, @rscan, [bit_offset, bit_size, value], ctx(opts))
  end

  @doc """
  Reads an integer from `[bit_offset, bit_offset + bit_size)` (`bit_size` ≤ 64).

  Pass `true` for `signed` to interpret the field as signed.
  """
  @spec get_int(String.t(), integer(), integer(), boolean(), keyword()) :: t()
  def get_int(bin_name, bit_offset, bit_size, signed \\ false, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) do
    args =
      if signed do
        [bit_offset, bit_size, @signed_flag]
      else
        [bit_offset, bit_size]
      end

    CDT.bit_read_op(bin_name, @get_int, args, ctx(opts))
  end
end
