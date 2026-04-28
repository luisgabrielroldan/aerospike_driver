defmodule Aerospike.Op.Bit do
  @moduledoc """
  Bit operations on a byte-array bin for `Aerospike.operate/4`.

  Bit operations require an Aerospike blob bin. When seeding a bit bin through
  this client, write an explicit blob such as `{:blob, <<0>>}` because plain
  Elixir binaries are encoded as Aerospike strings.

  Offsets are read left to right within the bin. Negative bit or byte offsets
  count backward from the end of the bitmap. Out-of-bounds offsets produce a
  server parameter error.

  Offsets are in bits for `set/5`, `bw_or/5`, `bw_xor/5`, `bw_and/5`,
  `bw_not/4`, `lshift/5`, `rshift/5`, `add/5`, `subtract/5`, `set_int/5`,
  `get/4`, `count/4`, `lscan/5`, `rscan/5`, and `get_int/5`. `resize/4`,
  `insert/4`, and `remove/4` use byte offsets and sizes.

  Pass `ctx:` for nested bitmaps and `flags:` for write policy flags.
  """

  alias Aerospike.PolicyInteger
  alias Aerospike.Protocol.CDT

  @typedoc """
  Opaque bit operation for `Aerospike.operate/4`.
  """
  @opaque t :: Aerospike.Op.t()

  @typedoc """
  Bit operation write policy flags.

  Accepts `:default`, `:create_only`, `:update_only`, `:no_fail`, `:partial`,
  or a list of those atoms. Compatibility callers may pass a non-negative
  integer; use `{:raw, integer}` when deliberately sending an unnamed server
  value.
  """
  @type flags :: atom() | [atom()] | non_neg_integer() | {:raw, non_neg_integer()}

  @typedoc """
  Overflow action for bit add/subtract operations.

  Accepts `:fail`, `:saturate`, or `:wrap`. Compatibility callers may pass a
  non-negative integer; use `{:raw, integer}` when deliberately sending an
  unnamed server value. `signed: true` adds the signed flag before encoding.
  """
  @type overflow_action :: atom() | non_neg_integer() | {:raw, non_neg_integer()}

  @typedoc """
  Resize flags for bit resize operations.

  Accepts `:default`, `:from_front`, `:grow_only`, `:shrink_only`, or a list
  of those atoms. Compatibility callers may pass a non-negative integer; use
  `{:raw, integer}` when deliberately sending an unnamed server value.
  """
  @type resize_flags :: atom() | [atom()] | non_neg_integer() | {:raw, non_neg_integer()}

  @typedoc """
  Common bit operation options.

  Supported keys:

  * `:ctx` - nested CDT context path from `Aerospike.Ctx`.
  * `:flags` - bit-operation write flags. Defaults to `:default`.
  * `:overflow_action` - overflow behavior for `add/5` and `subtract/5`.
  * `:signed` - when true, adds the signed flag to `:overflow_action`.
  """
  @type opts :: [
          ctx: Aerospike.Ctx.t(),
          flags: flags(),
          overflow_action: overflow_action(),
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

  @doc "Use default bit write behavior."
  @spec write_default() :: 0
  def write_default, do: PolicyInteger.bit_write_flags(:default)

  @doc "Only create the bit bin when it does not already exist."
  @spec write_create_only() :: 1
  def write_create_only, do: PolicyInteger.bit_write_flags(:create_only)

  @doc "Only update the bit bin when it already exists."
  @spec write_update_only() :: 2
  def write_update_only, do: PolicyInteger.bit_write_flags(:update_only)

  @doc "Do not fail the command when a bit operation is denied by write flags."
  @spec write_no_fail() :: 4
  def write_no_fail, do: PolicyInteger.bit_write_flags(:no_fail)

  @doc "Commit other valid operations when this bit operation is denied by write flags."
  @spec write_partial() :: 8
  def write_partial, do: PolicyInteger.bit_write_flags(:partial)

  @doc "Fail bit add/subtract on overflow or underflow."
  @spec overflow_fail() :: 0
  def overflow_fail, do: PolicyInteger.bit_overflow_action(:fail)

  @doc "Saturate bit add/subtract to min/max on overflow or underflow."
  @spec overflow_saturate() :: 2
  def overflow_saturate, do: PolicyInteger.bit_overflow_action(:saturate)

  @doc "Wrap bit add/subtract on overflow or underflow."
  @spec overflow_wrap() :: 4
  def overflow_wrap, do: PolicyInteger.bit_overflow_action(:wrap)

  @doc """
  Resizes the bin to `byte_size` bytes using `resize_flags`.
  """
  @spec resize(String.t(), integer(), resize_flags(), opts()) :: t()
  def resize(bin_name, byte_size, resize_flags \\ :default, opts \\ [])
      when is_binary(bin_name) and is_integer(byte_size) and is_list(opts) do
    CDT.bit_modify_op(
      bin_name,
      @resize,
      [byte_size, flags(opts), PolicyInteger.bit_resize_flags(resize_flags)],
      ctx(opts)
    )
  end

  @doc """
  Inserts `bytes` at `byte_offset` in the bin.
  """
  @spec insert(String.t(), integer(), binary(), opts()) :: t()
  def insert(bin_name, byte_offset, bytes, opts \\ [])
      when is_binary(bin_name) and is_integer(byte_offset) and is_binary(bytes) and
             is_list(opts) do
    CDT.bit_modify_op(bin_name, @insert, [byte_offset, {:bytes, bytes}, flags(opts)], ctx(opts))
  end

  @doc """
  Removes `byte_size` bytes starting at `byte_offset`.
  """
  @spec remove(String.t(), integer(), integer(), opts()) :: t()
  def remove(bin_name, byte_offset, byte_size, opts \\ [])
      when is_binary(bin_name) and is_integer(byte_offset) and is_integer(byte_size) and
             is_list(opts) do
    CDT.bit_modify_op(bin_name, @remove, [byte_offset, byte_size, flags(opts)], ctx(opts))
  end

  @doc """
  Overwrites the bit region `[bit_offset, bit_offset + bit_size)` with bits from `value`.
  """
  @spec set(String.t(), integer(), integer(), binary(), opts()) :: t()
  def set(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_binary(value) and is_list(opts) do
    CDT.bit_modify_op(
      bin_name,
      @set,
      [bit_offset, bit_size, {:bytes, value}, flags(opts)],
      ctx(opts)
    )
  end

  @doc """
  Applies bitwise OR of `value` into `[bit_offset, bit_offset + bit_size)`.
  """
  @spec bw_or(String.t(), integer(), integer(), binary(), opts()) :: t()
  def bw_or(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_binary(value) and is_list(opts) do
    CDT.bit_modify_op(
      bin_name,
      @bw_or,
      [bit_offset, bit_size, {:bytes, value}, flags(opts)],
      ctx(opts)
    )
  end

  @doc """
  Applies bitwise XOR of `value` into `[bit_offset, bit_offset + bit_size)`.
  """
  @spec bw_xor(String.t(), integer(), integer(), binary(), opts()) :: t()
  def bw_xor(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_binary(value) and is_list(opts) do
    CDT.bit_modify_op(
      bin_name,
      @bw_xor,
      [bit_offset, bit_size, {:bytes, value}, flags(opts)],
      ctx(opts)
    )
  end

  @doc """
  Applies bitwise AND of `value` into `[bit_offset, bit_offset + bit_size)`.
  """
  @spec bw_and(String.t(), integer(), integer(), binary(), opts()) :: t()
  def bw_and(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_binary(value) and is_list(opts) do
    CDT.bit_modify_op(
      bin_name,
      @bw_and,
      [bit_offset, bit_size, {:bytes, value}, flags(opts)],
      ctx(opts)
    )
  end

  @doc """
  Inverts `[bit_offset, bit_offset + bit_size)`.
  """
  @spec bw_not(String.t(), integer(), integer(), opts()) :: t()
  def bw_not(bin_name, bit_offset, bit_size, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_list(opts) do
    CDT.bit_modify_op(bin_name, @bw_not, [bit_offset, bit_size, flags(opts)], ctx(opts))
  end

  @doc """
  Left shifts `[bit_offset, bit_offset + bit_size)` by `shift` bits.
  """
  @spec lshift(String.t(), integer(), integer(), integer(), opts()) :: t()
  def lshift(bin_name, bit_offset, bit_size, shift, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_integer(shift) and is_list(opts) do
    CDT.bit_modify_op(bin_name, @lshift, [bit_offset, bit_size, shift, flags(opts)], ctx(opts))
  end

  @doc """
  Right shifts `[bit_offset, bit_offset + bit_size)` by `shift` bits.
  """
  @spec rshift(String.t(), integer(), integer(), integer(), opts()) :: t()
  def rshift(bin_name, bit_offset, bit_size, shift, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_integer(shift) and is_list(opts) do
    CDT.bit_modify_op(bin_name, @rshift, [bit_offset, bit_size, shift, flags(opts)], ctx(opts))
  end

  @doc """
  Adds `value` to a sub-integer in the bitmap.

  Supports `flags:`, `overflow_action:`, and `signed:`. Use `signed: true` to
  OR the signed flag into `overflow_action:`.
  """
  @spec add(String.t(), integer(), integer(), integer(), opts()) :: t()
  def add(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_integer(value) and is_list(opts) do
    CDT.bit_modify_op(
      bin_name,
      @add,
      [bit_offset, bit_size, value, flags(opts), overflow_action(opts)],
      ctx(opts)
    )
  end

  @doc """
  Subtracts `value` from a sub-integer in the bitmap.

  Supports `flags:`, `overflow_action:`, and `signed:`. Use `signed: true` to
  OR the signed flag into `overflow_action:`.
  """
  @spec subtract(String.t(), integer(), integer(), integer(), opts()) :: t()
  def subtract(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_integer(value) and is_list(opts) do
    CDT.bit_modify_op(
      bin_name,
      @subtract,
      [bit_offset, bit_size, value, flags(opts), overflow_action(opts)],
      ctx(opts)
    )
  end

  @doc """
  Writes integer `value` into `[bit_offset, bit_offset + bit_size)`.
  """
  @spec set_int(String.t(), integer(), integer(), integer(), opts()) :: t()
  def set_int(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_integer(value) and is_list(opts) do
    CDT.bit_modify_op(bin_name, @set_int, [bit_offset, bit_size, value, flags(opts)], ctx(opts))
  end

  @doc """
  Returns `bit_size` bits starting at `bit_offset` as a binary.
  """
  @spec get(String.t(), integer(), integer(), opts()) :: t()
  def get(bin_name, bit_offset, bit_size, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_list(opts) do
    CDT.bit_read_op(bin_name, @get, [bit_offset, bit_size], ctx(opts))
  end

  @doc """
  Returns the count of bits set to `1` in `[bit_offset, bit_offset + bit_size)`.
  """
  @spec count(String.t(), integer(), integer(), opts()) :: t()
  def count(bin_name, bit_offset, bit_size, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_list(opts) do
    CDT.bit_read_op(bin_name, @count, [bit_offset, bit_size], ctx(opts))
  end

  @doc """
  Returns the offset of the first bit matching `value` when scanning left to right.
  """
  @spec lscan(String.t(), integer(), integer(), boolean(), opts()) :: t()
  def lscan(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_boolean(value) and is_list(opts) do
    CDT.bit_read_op(bin_name, @lscan, [bit_offset, bit_size, value], ctx(opts))
  end

  @doc """
  Returns the offset of the first bit matching `value` when scanning right to left.
  """
  @spec rscan(String.t(), integer(), integer(), boolean(), opts()) :: t()
  def rscan(bin_name, bit_offset, bit_size, value, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_boolean(value) and is_list(opts) do
    CDT.bit_read_op(bin_name, @rscan, [bit_offset, bit_size, value], ctx(opts))
  end

  @doc """
  Reads an integer from `[bit_offset, bit_offset + bit_size)`.

  Pass `true` for `signed` to interpret the field as signed.
  """
  @spec get_int(String.t(), integer(), integer(), boolean(), opts()) :: t()
  def get_int(bin_name, bit_offset, bit_size, signed \\ false, opts \\ [])
      when is_binary(bin_name) and is_integer(bit_offset) and is_integer(bit_size) and
             is_boolean(signed) and is_list(opts) do
    args =
      if signed do
        [bit_offset, bit_size, PolicyInteger.bit_signed_flag()]
      else
        [bit_offset, bit_size]
      end

    CDT.bit_read_op(bin_name, @get_int, args, ctx(opts))
  end

  defp ctx(opts), do: Keyword.get(opts, :ctx)
  defp flags(opts), do: opts |> Keyword.get(:flags, :default) |> PolicyInteger.bit_write_flags()

  defp overflow_action(opts),
    do:
      PolicyInteger.bit_overflow_action(
        Keyword.get(opts, :overflow_action, :default),
        signed?(opts)
      )

  defp signed?(opts), do: Keyword.get(opts, :signed, false) == true
end
