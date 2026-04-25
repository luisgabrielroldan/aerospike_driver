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

  @enforce_keys [:wire]
  defstruct [:wire]

  @typedoc """
  Opaque server-side expression.

  The `wire` field contains encoded Aerospike expression bytes.
  """
  @type t :: %__MODULE__{wire: binary()}

  alias Aerospike.Protocol.Exp, as: Encoder

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

  @doc """
  Builds a literal expression from an Elixir value.

  Binaries are treated as strings. Use `blob/1` explicitly for raw binary
  semantics.
  """
  @spec val(integer() | float() | binary() | boolean() | nil) :: t()
  def val(value) when is_integer(value), do: int(value)
  def val(value) when is_float(value), do: float(value)
  def val(value) when is_binary(value), do: str(value)
  def val(value) when is_boolean(value), do: bool(value)
  def val(nil), do: nil_()

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

  @doc "Record time-to-live in seconds."
  @spec ttl() :: t()
  def ttl, do: encode(%{cmd: :ttl})

  @doc "Record expiration time as an absolute server timestamp."
  @spec void_time() :: t()
  def void_time, do: encode(%{cmd: :void_time})

  @doc "Record last-update timestamp."
  @spec last_update() :: t()
  def last_update, do: encode(%{cmd: :last_update})

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

  defp bin(name, type), do: encode(%{cmd: :bin, val: name, type: type})

  defp compare(operator, %__MODULE__{} = left, %__MODULE__{} = right) do
    encode(%{cmd: operator, exps: expression_nodes([left, right])})
  end

  defp expression_nodes(expressions) do
    Enum.map(expressions, fn %__MODULE__{wire: wire} -> %{bytes: wire} end)
  end

  defp encode(node), do: %__MODULE__{wire: Encoder.encode(node)}
end
