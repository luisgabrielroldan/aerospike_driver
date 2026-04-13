defmodule Aerospike.Exp do
  @moduledoc """
  Server-side filter expressions for policies (`:filter` on reads, batches,
  scans, queries).

  Expressions are built as composable typed values using the builder functions in this module.
  Each function returns an `%Aerospike.Exp{}` struct containing the expression's pre-encoded
  wire bytes. Expressions compose by passing `%Exp{}` values as arguments to other builders.

  ## Usage

      alias Aerospike.Exp

      # Simple comparison
      expr = Exp.gt(Exp.int_bin("age"), Exp.int(21))

      # Convenience: Exp.val/1 infers the type from the Elixir term
      expr = Exp.gt(Exp.int_bin("age"), Exp.val(21))

      # Boolean composition
      expr = Exp.and_([
        Exp.gte(Exp.int_bin("age"), Exp.val(18)),
        Exp.lt(Exp.int_bin("age"), Exp.val(65)),
        Exp.eq(Exp.str_bin("status"), Exp.val("active"))
      ])

      # Negation
      expr = Exp.not_(Exp.eq(Exp.str_bin("status"), Exp.val("banned")))

      # Record metadata
      expr = Exp.gt(Exp.ttl(), Exp.int(3600))

      # Use as a filter on a CRUD operation
      MyApp.Repo.get(key, filter: expr)

  ## `Exp.val/1` — type-inferring convenience

  `Exp.val/1` maps Elixir terms to their Aerospike expression type:

  | Elixir term | Expression type |
  |-------------|----------------|
  | `integer()` | `Exp.int/1` |
  | `float()`   | `Exp.float/1` |
  | `binary()`  | `Exp.str/1` |
  | `boolean()` | `Exp.bool/1` |
  | `nil`       | `Exp.nil_/0` |

  All binaries are treated as strings. Use `Exp.blob/1` explicitly for raw binary semantics.

  ## Related

  - `Aerospike.Op.Exp` — expression operations inside `operate/4`
  - `MyApp.Repo.get/2`, `MyApp.Repo.stream!/2`, `MyApp.Repo.all/2` — recommended
    application-facing `:filter` usage
  - `Aerospike.get/3`, `Aerospike.stream!/3`, `Aerospike.all/3` — low-level
    facade for the same `:filter` option
  """

  @enforce_keys [:wire]
  defstruct [:wire]

  @type t :: %__MODULE__{wire: binary()}

  alias Aerospike.Protocol.Exp, as: ExpEncoder

  @doc """
  Wraps pre-encoded filter expression bytes.

  The binary must match the Aerospike filter expression wire layout. Prefer the typed
  builder functions (`Exp.int_bin/1`, `Exp.gt/2`, etc.) over this low-level escape hatch.

  ## Examples

      iex> e = Aerospike.Exp.from_wire(<<1, 2>>)
      iex> e.wire
      <<1, 2>>

  """
  @spec from_wire(binary()) :: t()
  def from_wire(wire) when is_binary(wire), do: %__MODULE__{wire: wire}

  @doc false
  @spec base64(t()) :: {:ok, String.t()} | {:error, :empty}
  def base64(%__MODULE__{wire: ""}), do: {:error, :empty}
  def base64(%__MODULE__{wire: wire}) when is_binary(wire), do: {:ok, Base.encode64(wire)}

  # ---------------------------------------------------------------------------
  # Literal values
  # ---------------------------------------------------------------------------

  @doc "Integer literal expression."
  @spec int(integer()) :: t()
  def int(n) when is_integer(n), do: %__MODULE__{wire: ExpEncoder.encode(%{val: n})}

  @doc "Float literal expression."
  @spec float(float()) :: t()
  def float(f) when is_float(f), do: %__MODULE__{wire: ExpEncoder.encode(%{val: f})}

  @doc "String literal expression (plain UTF-8, no particle-type prefix on the wire)."
  @spec str(binary()) :: t()
  def str(s) when is_binary(s), do: %__MODULE__{wire: ExpEncoder.encode(%{val: {:string, s}})}

  @doc "Boolean literal expression."
  @spec bool(boolean()) :: t()
  def bool(b) when is_boolean(b), do: %__MODULE__{wire: ExpEncoder.encode(%{val: b})}

  @doc "Nil literal expression."
  @spec nil_() :: t()
  def nil_, do: %__MODULE__{wire: ExpEncoder.encode(%{val: nil})}

  @doc "Blob (raw binary) literal expression."
  @spec blob(binary()) :: t()
  def blob(b) when is_binary(b), do: %__MODULE__{wire: ExpEncoder.encode(%{val: {:blob, b}})}

  @doc """
  Type-inferring literal convenience. Maps Elixir terms to their typed expression constructor.

  Binaries are always mapped to `str/1`. Use `blob/1` explicitly for raw binary semantics.
  """
  @spec val(integer() | float() | binary() | boolean() | nil) :: t()
  def val(n) when is_integer(n), do: int(n)
  def val(f) when is_float(f), do: float(f)
  def val(s) when is_binary(s), do: str(s)
  def val(b) when is_boolean(b), do: bool(b)
  def val(nil), do: nil_()

  # ---------------------------------------------------------------------------
  # Bin reads
  # ---------------------------------------------------------------------------

  @doc "Read an integer bin from the current record."
  @spec int_bin(String.t()) :: t()
  def int_bin(name) when is_binary(name),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :bin, val: name, type: :int})}

  @doc "Read a float bin from the current record."
  @spec float_bin(String.t()) :: t()
  def float_bin(name) when is_binary(name),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :bin, val: name, type: :float})}

  @doc "Read a string bin from the current record."
  @spec str_bin(String.t()) :: t()
  def str_bin(name) when is_binary(name),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :bin, val: name, type: :string})}

  @doc "Read a boolean bin from the current record."
  @spec bool_bin(String.t()) :: t()
  def bool_bin(name) when is_binary(name),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :bin, val: name, type: :bool})}

  @doc "Read a blob bin from the current record."
  @spec blob_bin(String.t()) :: t()
  def blob_bin(name) when is_binary(name),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :bin, val: name, type: :blob})}

  @doc "Read a geo bin from the current record."
  @spec geo_bin(String.t()) :: t()
  def geo_bin(name) when is_binary(name),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :bin, val: name, type: :geo})}

  # ---------------------------------------------------------------------------
  # Record metadata
  # ---------------------------------------------------------------------------

  @doc "Record TTL (time-to-live in seconds)."
  @spec ttl() :: t()
  def ttl, do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :ttl})}

  @doc "Record void time (absolute expiration timestamp)."
  @spec void_time() :: t()
  def void_time, do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :void_time})}

  @doc "Record last-update timestamp."
  @spec last_update() :: t()
  def last_update, do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :last_update})}

  @doc "True if a key value was stored with the record."
  @spec key_exists() :: t()
  def key_exists, do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :key_exists})}

  @doc "The set name of the record."
  @spec set_name() :: t()
  def set_name, do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :set_name})}

  @doc "Server-side expression that evaluates to true when the record is a tombstone."
  @spec tombstone?() :: t()
  def tombstone?, do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :is_tombstone})}

  @doc "Record size in bytes on storage device."
  @spec record_size() :: t()
  def record_size, do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :record_size})}

  # ---------------------------------------------------------------------------
  # Comparisons
  # ---------------------------------------------------------------------------

  @doc "Equal comparison: `left == right`."
  @spec eq(t(), t()) :: t()
  def eq(%__MODULE__{wire: l}, %__MODULE__{wire: r}),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :eq, exps: [%{bytes: l}, %{bytes: r}]})}

  @doc "Not-equal comparison: `left != right`."
  @spec ne(t(), t()) :: t()
  def ne(%__MODULE__{wire: l}, %__MODULE__{wire: r}),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :ne, exps: [%{bytes: l}, %{bytes: r}]})}

  @doc "Greater-than comparison: `left > right`."
  @spec gt(t(), t()) :: t()
  def gt(%__MODULE__{wire: l}, %__MODULE__{wire: r}),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :gt, exps: [%{bytes: l}, %{bytes: r}]})}

  @doc "Greater-than-or-equal comparison: `left >= right`."
  @spec gte(t(), t()) :: t()
  def gte(%__MODULE__{wire: l}, %__MODULE__{wire: r}),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :gte, exps: [%{bytes: l}, %{bytes: r}]})}

  @doc "Less-than comparison: `left < right`."
  @spec lt(t(), t()) :: t()
  def lt(%__MODULE__{wire: l}, %__MODULE__{wire: r}),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :lt, exps: [%{bytes: l}, %{bytes: r}]})}

  @doc "Less-than-or-equal comparison: `left <= right`."
  @spec lte(t(), t()) :: t()
  def lte(%__MODULE__{wire: l}, %__MODULE__{wire: r}),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :lte, exps: [%{bytes: l}, %{bytes: r}]})}

  # ---------------------------------------------------------------------------
  # Boolean combinators
  # ---------------------------------------------------------------------------

  @doc """
  Logical AND over a list of expressions. Requires at least two elements.

  The name uses a trailing underscore because `and` is an Elixir reserved word.
  """
  @spec and_([t(), ...]) :: t()
  def and_(exps) when is_list(exps) and length(exps) >= 2 do
    nodes = Enum.map(exps, fn %__MODULE__{wire: w} -> %{bytes: w} end)
    %__MODULE__{wire: ExpEncoder.encode(%{cmd: :and_, exps: nodes})}
  end

  @doc """
  Logical OR over a list of expressions. Requires at least two elements.

  The name uses a trailing underscore because `or` is an Elixir reserved word.
  """
  @spec or_([t(), ...]) :: t()
  def or_(exps) when is_list(exps) and length(exps) >= 2 do
    nodes = Enum.map(exps, fn %__MODULE__{wire: w} -> %{bytes: w} end)
    %__MODULE__{wire: ExpEncoder.encode(%{cmd: :or_, exps: nodes})}
  end

  @doc """
  Logical NOT of an expression.

  The name uses a trailing underscore because `not` is an Elixir reserved word.
  """
  @spec not_(t()) :: t()
  def not_(%__MODULE__{wire: w}),
    do: %__MODULE__{wire: ExpEncoder.encode(%{cmd: :not_, exps: [%{bytes: w}]})}
end
