defmodule Aerospike.Exp do
  @moduledoc """
  Server-side expression builder.

  Expressions are composable values used by Aerospike server features such as
  filter expressions. Each builder returns an `%Aerospike.Exp{}` struct
  containing pre-encoded expression wire bytes.

      alias Aerospike.Exp

      adult =
        Exp.and_([
          Exp.gte(Exp.int_bin("age"), Exp.val(18)),
          Exp.lt(Exp.int_bin("age"), Exp.val(65))
        ])

      active = Exp.eq(Exp.str_bin("status"), Exp.val("active"))
      expression = Exp.and_([adult, active])

  `val/1` maps Elixir values to literal expressions:

  | Elixir term | Expression builder |
  |-------------|--------------------|
  | `integer()` | `int/1` |
  | `float()` | `float/1` |
  | `binary()` | `str/1` |
  | `boolean()` | `bool/1` |
  | `nil` | `nil_/0` |

  Binaries passed to `val/1` or `str/1` are encoded as MessagePack strings.
  Use `blob/1` when the expression value must use MessagePack binary format.
  """

  import Kernel, except: [abs: 1, ceil: 1, floor: 1]

  @enforce_keys [:wire]
  defstruct [:wire]

  @typedoc """
  Opaque server-side expression.

  The `wire` field contains encoded Aerospike expression bytes.
  """
  @type t :: %__MODULE__{wire: binary()}

  alias Aerospike.Protocol.Exp, as: Encoder

  @particle_types %{
    null: 0,
    integer: 1,
    float: 2,
    string: 3,
    blob: 4,
    digest: 6,
    bool: 17,
    hll: 18,
    map: 19,
    list: 20,
    ldt: 21,
    geojson: 23
  }

  @regex_flags %{
    none: 0,
    extended: 1,
    icase: 2,
    nosub: 4,
    newline: 8
  }

  @loop_var_parts %{
    map_key: 0,
    value: 1,
    index: 2
  }

  @type exp_type :: nil | :bool | :int | :string | :list | :map | :blob | :float | :geo | :hll
  @type particle_type ::
          :null
          | :integer
          | :float
          | :string
          | :blob
          | :digest
          | :bool
          | :hll
          | :map
          | :list
          | :ldt
          | :geojson
  @type regex_flag :: :none | :extended | :icase | :nosub | :newline
  @type loop_var_part :: :map_key | :value | :index

  @doc """
  Wraps pre-encoded expression bytes.

  This is a low-level escape hatch for expressions built outside this module.
  The binary is not validated as a complete Aerospike expression.
  """
  @spec from_wire(binary()) :: t()
  def from_wire(wire) when is_binary(wire), do: %__MODULE__{wire: wire}

  @doc """
  Encodes an expression's wire bytes as Base64.

  Empty expressions return `{:error, :empty}` because Aerospike server APIs that
  accept expressions require a non-empty expression payload.
  """
  @spec base64(t()) :: {:ok, String.t()} | {:error, :empty}
  def base64(%__MODULE__{wire: ""}), do: {:error, :empty}
  def base64(%__MODULE__{wire: wire}) when is_binary(wire), do: {:ok, Base.encode64(wire)}

  @doc "Integer literal expression."
  @spec int(integer()) :: t()
  def int(value) when is_integer(value), do: encode(%{val: value})

  @doc "Float literal expression."
  @spec float(float()) :: t()
  def float(value) when is_float(value), do: encode(%{val: value})

  @doc "String literal expression encoded as a MessagePack string."
  @spec str(binary()) :: t()
  def str(value) when is_binary(value), do: encode(%{val: {:string, value}})

  @doc "Boolean literal expression."
  @spec bool(boolean()) :: t()
  def bool(value) when is_boolean(value), do: encode(%{val: value})

  @doc "Nil literal expression."
  @spec nil_() :: t()
  def nil_, do: encode(%{val: nil})

  @doc "Blob literal expression encoded as MessagePack binary data."
  @spec blob(binary()) :: t()
  def blob(value) when is_binary(value), do: encode(%{val: {:blob, value}})

  @doc "GeoJSON literal expression."
  @spec geo(binary()) :: t()
  def geo(value) when is_binary(value), do: encode(%{val: {:geo, value}})

  @doc "List literal expression."
  @spec list(list()) :: t()
  def list(values) when is_list(values), do: encode(%{val: {:list, values}})

  @doc "Map literal expression."
  @spec map(map()) :: t()
  def map(values) when is_map(values), do: encode(%{val: {:map, values}})

  @doc "Infinity value for CDT range expressions."
  @spec infinity() :: t()
  def infinity, do: encode(%{val: :infinity})

  @doc "Wildcard value for CDT expressions."
  @spec wildcard() :: t()
  def wildcard, do: encode(%{val: :wildcard})

  @doc """
  Builds a literal expression from an Elixir value.

  Binaries are treated as strings. Use `blob/1` explicitly for raw binary
  semantics.
  """
  @spec val(integer() | float() | binary() | boolean() | nil | list() | map()) :: t()
  def val(value) when is_integer(value), do: int(value)
  def val(value) when is_float(value), do: float(value)
  def val(value) when is_binary(value), do: str(value)
  def val(value) when is_boolean(value), do: bool(value)
  def val(nil), do: nil_()
  def val(value) when is_list(value), do: list(value)
  def val(value) when is_map(value), do: map(value)

  @doc "Record key expression of the specified expression type."
  @spec key(exp_type()) :: t()
  def key(type), do: encode(%{cmd: :key, type: type})

  @doc "Reads an integer bin from the current record."
  @spec int_bin(String.t()) :: t()
  def int_bin(name) when is_binary(name), do: bin(name, :int)

  @doc "Reads a float bin from the current record."
  @spec float_bin(String.t()) :: t()
  def float_bin(name) when is_binary(name), do: bin(name, :float)

  @doc "Reads a string bin from the current record."
  @spec str_bin(String.t()) :: t()
  def str_bin(name) when is_binary(name), do: bin(name, :string)

  @doc "Reads a boolean bin from the current record."
  @spec bool_bin(String.t()) :: t()
  def bool_bin(name) when is_binary(name), do: bin(name, :bool)

  @doc "Reads a blob bin from the current record."
  @spec blob_bin(String.t()) :: t()
  def blob_bin(name) when is_binary(name), do: bin(name, :blob)

  @doc "Reads a geospatial bin from the current record."
  @spec geo_bin(String.t()) :: t()
  def geo_bin(name) when is_binary(name), do: bin(name, :geo)

  @doc "Reads a list bin from the current record."
  @spec list_bin(String.t()) :: t()
  def list_bin(name) when is_binary(name), do: bin(name, :list)

  @doc "Reads a map bin from the current record."
  @spec map_bin(String.t()) :: t()
  def map_bin(name) when is_binary(name), do: bin(name, :map)

  @doc "Reads an HLL bin from the current record."
  @spec hll_bin(String.t()) :: t()
  def hll_bin(name) when is_binary(name), do: bin(name, :hll)

  @doc "True when the named bin exists in the current record."
  @spec bin_exists(String.t()) :: t()
  def bin_exists(name) when is_binary(name), do: ne(bin_type(name), int(particle_type(:null)))

  @doc "Reads the named bin's integer particle type."
  @spec bin_type(String.t()) :: t()
  def bin_type(name) when is_binary(name), do: encode(%{cmd: :bin_type, val: name})

  @doc "Record time-to-live in seconds."
  @spec ttl() :: t()
  def ttl, do: encode(%{cmd: :ttl})

  @doc "Record expiration time as an absolute server timestamp."
  @spec void_time() :: t()
  def void_time, do: encode(%{cmd: :void_time})

  @doc "Record last-update timestamp."
  @spec last_update() :: t()
  def last_update, do: encode(%{cmd: :last_update})

  @doc "Milliseconds since the record was last updated."
  @spec since_update() :: t()
  def since_update, do: encode(%{cmd: :since_update})

  @doc "True when the record has a stored user key."
  @spec key_exists() :: t()
  def key_exists, do: encode(%{cmd: :key_exists})

  @doc "Record set name."
  @spec set_name() :: t()
  def set_name, do: encode(%{cmd: :set_name})

  @doc "True when the record is a tombstone."
  @spec tombstone?() :: t()
  def tombstone?, do: encode(%{cmd: :is_tombstone})

  @doc "Record size in bytes on storage device."
  @spec record_size() :: t()
  def record_size, do: encode(%{cmd: :record_size})

  @doc "Record digest modulo expression."
  @spec digest_modulo(integer()) :: t()
  def digest_modulo(value) when is_integer(value), do: encode(%{cmd: :digest_modulo, val: value})

  @doc "Integer particle type value returned by `bin_type/1`."
  @spec particle_type(particle_type()) :: non_neg_integer()
  def particle_type(name) when is_map_key(@particle_types, name),
    do: Map.fetch!(@particle_types, name)

  @doc "Integer regular expression flag value for `regex_compare/3`."
  @spec regex_flag(regex_flag()) :: non_neg_integer()
  def regex_flag(name) when is_map_key(@regex_flags, name), do: Map.fetch!(@regex_flags, name)

  @doc "Combines regular expression flags for `regex_compare/3`."
  @spec regex_flags([regex_flag()]) :: non_neg_integer()
  def regex_flags(flags) when is_list(flags) do
    Enum.reduce(flags, 0, fn flag, acc -> Bitwise.bor(acc, regex_flag(flag)) end)
  end

  @doc "Integer loop-variable part value for typed loop-variable builders."
  @spec loop_var_part(loop_var_part()) :: non_neg_integer()
  def loop_var_part(name) when is_map_key(@loop_var_parts, name),
    do: Map.fetch!(@loop_var_parts, name)

  @doc "Regular expression comparison against a string expression."
  @spec regex_compare(String.t(), non_neg_integer(), t()) :: t()
  def regex_compare(regex, flags, %__MODULE__{} = expression)
      when is_binary(regex) and is_integer(flags) and flags >= 0 do
    encode(%{cmd: :regex, val: {regex, flags}, exps: expression_nodes([expression])})
  end

  @doc "Geospatial comparison."
  @spec geo_compare(t(), t()) :: t()
  def geo_compare(%__MODULE__{} = left, %__MODULE__{} = right) do
    encode(%{cmd: :geo_compare, exps: expression_nodes([left, right])})
  end

  @doc "Equal comparison."
  @spec eq(t(), t()) :: t()
  def eq(%__MODULE__{} = left, %__MODULE__{} = right), do: compare(:eq, left, right)

  @doc "Not-equal comparison."
  @spec ne(t(), t()) :: t()
  def ne(%__MODULE__{} = left, %__MODULE__{} = right), do: compare(:ne, left, right)

  @doc "Greater-than comparison."
  @spec gt(t(), t()) :: t()
  def gt(%__MODULE__{} = left, %__MODULE__{} = right), do: compare(:gt, left, right)

  @doc "Greater-than-or-equal comparison."
  @spec gte(t(), t()) :: t()
  def gte(%__MODULE__{} = left, %__MODULE__{} = right), do: compare(:gte, left, right)

  @doc "Less-than comparison."
  @spec lt(t(), t()) :: t()
  def lt(%__MODULE__{} = left, %__MODULE__{} = right), do: compare(:lt, left, right)

  @doc "Less-than-or-equal comparison."
  @spec lte(t(), t()) :: t()
  def lte(%__MODULE__{} = left, %__MODULE__{} = right), do: compare(:lte, left, right)

  @doc """
  Logical AND over two or more expressions.

  The function name has a trailing underscore because `and` is an Elixir
  reserved word.
  """
  @spec and_([t(), ...]) :: t()
  def and_(expressions) when is_list(expressions) and length(expressions) >= 2 do
    encode(%{cmd: :and_, exps: expression_nodes(expressions)})
  end

  @doc """
  Logical OR over two or more expressions.

  The function name has a trailing underscore because `or` is an Elixir
  reserved word.
  """
  @spec or_([t(), ...]) :: t()
  def or_(expressions) when is_list(expressions) and length(expressions) >= 2 do
    encode(%{cmd: :or_, exps: expression_nodes(expressions)})
  end

  @doc """
  Logical NOT of an expression.

  The function name has a trailing underscore because `not` is an Elixir
  reserved word.
  """
  @spec not_(t()) :: t()
  def not_(%__MODULE__{} = expression) do
    encode(%{cmd: :not_, exps: expression_nodes([expression])})
  end

  @doc "Logical exclusive-or over two or more expressions."
  @spec exclusive([t(), ...]) :: t()
  def exclusive(expressions) when is_list(expressions) and length(expressions) >= 2 do
    encode(%{cmd: :exclusive, exps: expression_nodes(expressions)})
  end

  @doc "Numeric addition over one or more expressions."
  @spec add([t(), ...]) :: t()
  def add(expressions), do: variadic(:add, expressions)

  @doc "Numeric subtraction over one or more expressions."
  @spec sub([t(), ...]) :: t()
  def sub(expressions), do: variadic(:sub, expressions)

  @doc "Numeric multiplication over one or more expressions."
  @spec mul([t(), ...]) :: t()
  def mul(expressions), do: variadic(:mul, expressions)

  @doc "Numeric division over one or more expressions."
  @spec div_([t(), ...]) :: t()
  def div_(expressions), do: variadic(:div_, expressions)

  @doc "Numeric power expression."
  @spec pow(t(), t()) :: t()
  def pow(%__MODULE__{} = base, %__MODULE__{} = exponent), do: compare(:pow, base, exponent)

  @doc "Numeric logarithm expression."
  @spec log(t(), t()) :: t()
  def log(%__MODULE__{} = number, %__MODULE__{} = base), do: compare(:log, number, base)

  @doc "Integer modulo expression."
  @spec mod(t(), t()) :: t()
  def mod(%__MODULE__{} = numerator, %__MODULE__{} = denominator),
    do: compare(:mod, numerator, denominator)

  @doc "Absolute value expression."
  @spec abs(t()) :: t()
  def abs(%__MODULE__{} = expression), do: unary(:abs, expression)

  @doc "Floor expression."
  @spec floor(t()) :: t()
  def floor(%__MODULE__{} = expression), do: unary(:floor, expression)

  @doc "Ceiling expression."
  @spec ceil(t()) :: t()
  def ceil(%__MODULE__{} = expression), do: unary(:ceil, expression)

  @doc "Converts a numeric expression to an integer."
  @spec to_int(t()) :: t()
  def to_int(%__MODULE__{} = expression), do: unary(:to_int, expression)

  @doc "Converts a numeric expression to a float."
  @spec to_float(t()) :: t()
  def to_float(%__MODULE__{} = expression), do: unary(:to_float, expression)

  @doc "Integer bitwise AND over two or more expressions."
  @spec int_and([t(), ...]) :: t()
  def int_and(expressions) when is_list(expressions) and length(expressions) >= 2,
    do: variadic(:int_and, expressions)

  @doc "Integer bitwise OR over two or more expressions."
  @spec int_or([t(), ...]) :: t()
  def int_or(expressions) when is_list(expressions) and length(expressions) >= 2,
    do: variadic(:int_or, expressions)

  @doc "Integer bitwise XOR over two or more expressions."
  @spec int_xor([t(), ...]) :: t()
  def int_xor(expressions) when is_list(expressions) and length(expressions) >= 2,
    do: variadic(:int_xor, expressions)

  @doc "Integer bitwise NOT expression."
  @spec int_not(t()) :: t()
  def int_not(%__MODULE__{} = expression), do: unary(:int_not, expression)

  @doc "Integer left-shift expression."
  @spec int_lshift(t(), t()) :: t()
  def int_lshift(%__MODULE__{} = value, %__MODULE__{} = shift),
    do: compare(:int_lshift, value, shift)

  @doc "Integer logical right-shift expression."
  @spec int_rshift(t(), t()) :: t()
  def int_rshift(%__MODULE__{} = value, %__MODULE__{} = shift),
    do: compare(:int_rshift, value, shift)

  @doc "Integer arithmetic right-shift expression."
  @spec int_arshift(t(), t()) :: t()
  def int_arshift(%__MODULE__{} = value, %__MODULE__{} = shift),
    do: compare(:int_arshift, value, shift)

  @doc "Count of set bits in an integer expression."
  @spec int_count(t()) :: t()
  def int_count(%__MODULE__{} = expression), do: unary(:int_count, expression)

  @doc "Scan integer bits from left to right for a search bit."
  @spec int_lscan(t(), t()) :: t()
  def int_lscan(%__MODULE__{} = value, %__MODULE__{} = search),
    do: compare(:int_lscan, value, search)

  @doc "Scan integer bits from right to left for a search bit."
  @spec int_rscan(t(), t()) :: t()
  def int_rscan(%__MODULE__{} = value, %__MODULE__{} = search),
    do: compare(:int_rscan, value, search)

  @doc "Minimum value over one or more expressions."
  @spec min([t(), ...]) :: t()
  def min(expressions), do: variadic(:min, expressions)

  @doc "Maximum value over one or more expressions."
  @spec max([t(), ...]) :: t()
  def max(expressions), do: variadic(:max, expressions)

  @doc "Conditionally selects an action expression."
  @spec cond_([t(), ...]) :: t()
  def cond_(expressions) when is_list(expressions) and length(expressions) >= 3 do
    encode(%{cmd: :cond, exps: expression_nodes(expressions)})
  end

  @doc "Defines variables and evaluates a scoped expression."
  @spec let([t(), ...]) :: t()
  def let(expressions) when is_list(expressions) and length(expressions) >= 2 do
    encode(%{cmd: :let, exps: expression_nodes(expressions)})
  end

  @doc "Assigns a variable for use inside `let/1`."
  @spec def_(String.t(), t()) :: t()
  def def_(name, %__MODULE__{} = expression) when is_binary(name) do
    encode(%{cmd: :def, val: name, exps: expression_nodes([expression])})
  end

  @doc "Reads a variable defined by `let/1`."
  @spec var(String.t()) :: t()
  def var(name) when is_binary(name), do: encode(%{cmd: :var, val: name})

  @doc "Reads a built-in loop variable with the specified expression type."
  @spec loop_var(exp_type(), loop_var_part()) :: t()
  def loop_var(type, part), do: encode(%{cmd: :loop_var, type: type, val: loop_var_part(part)})

  @doc "Reads a nil built-in loop variable."
  @spec nil_loop_var(loop_var_part()) :: t()
  def nil_loop_var(part), do: loop_var(nil, part)

  @doc "Reads a boolean built-in loop variable."
  @spec bool_loop_var(loop_var_part()) :: t()
  def bool_loop_var(part), do: loop_var(:bool, part)

  @doc "Reads an integer built-in loop variable."
  @spec int_loop_var(loop_var_part()) :: t()
  def int_loop_var(part), do: loop_var(:int, part)

  @doc "Reads a float built-in loop variable."
  @spec float_loop_var(loop_var_part()) :: t()
  def float_loop_var(part), do: loop_var(:float, part)

  @doc "Reads a string built-in loop variable."
  @spec str_loop_var(loop_var_part()) :: t()
  def str_loop_var(part), do: loop_var(:string, part)

  @doc "Reads a blob built-in loop variable."
  @spec blob_loop_var(loop_var_part()) :: t()
  def blob_loop_var(part), do: loop_var(:blob, part)

  @doc "Reads a list built-in loop variable."
  @spec list_loop_var(loop_var_part()) :: t()
  def list_loop_var(part), do: loop_var(:list, part)

  @doc "Reads a map built-in loop variable."
  @spec map_loop_var(loop_var_part()) :: t()
  def map_loop_var(part), do: loop_var(:map, part)

  @doc "Reads a geospatial built-in loop variable."
  @spec geo_loop_var(loop_var_part()) :: t()
  def geo_loop_var(part), do: loop_var(:geo, part)

  @doc "Reads an HLL built-in loop variable."
  @spec hll_loop_var(loop_var_part()) :: t()
  def hll_loop_var(part), do: loop_var(:hll, part)

  @doc "Unknown expression value."
  @spec unknown() :: t()
  def unknown, do: encode(%{cmd: :unknown})

  @doc "Result-remove expression value."
  @spec remove_result() :: t()
  def remove_result, do: encode(%{cmd: :remove_result})

  defp bin(name, type), do: encode(%{cmd: :bin, val: name, type: type})

  defp unary(operator, %__MODULE__{} = expression) do
    encode(%{cmd: operator, exps: expression_nodes([expression])})
  end

  defp variadic(operator, [_ | _] = expressions) do
    encode(%{cmd: operator, exps: expression_nodes(expressions)})
  end

  defp compare(operator, %__MODULE__{} = left, %__MODULE__{} = right) do
    encode(%{cmd: operator, exps: expression_nodes([left, right])})
  end

  defp expression_nodes(expressions) do
    Enum.map(expressions, fn %__MODULE__{wire: wire} -> %{bytes: wire} end)
  end

  defp encode(node), do: %__MODULE__{wire: Encoder.encode(node)}
end
