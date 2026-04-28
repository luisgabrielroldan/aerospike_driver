defmodule Aerospike.Op.Exp do
  @moduledoc """
  Expression operations for `Aerospike.operate/4`.

  Expression operations evaluate an `Aerospike.Exp` on the server. Reads return
  the expression result under the requested response bin name; writes store the
  result into the target bin.

      Aerospike.operate(cluster, key, [
        Aerospike.Op.Exp.read("projected", Aerospike.Exp.int_bin("count")),
        Aerospike.Op.Exp.write("computed", Aerospike.Exp.int(99))
      ])

  This module accepts the expression builders exposed by `Aerospike.Exp`; it
  does not accept arbitrary encoded payloads.
  """

  alias Aerospike.Error
  alias Aerospike.Exp
  alias Aerospike.PolicyInteger
  alias Aerospike.Protocol.AsmMsg.Operation

  @typedoc """
  Opaque expression operation for `Aerospike.operate/4`.
  """
  @opaque t :: Aerospike.Op.t()

  @typedoc """
  Options accepted by expression operate builders.

  Supported key:

  * `:flags` - expression read or write flags. Defaults to `:default`.
  """
  @type flags :: atom() | [atom()] | non_neg_integer() | {:raw, non_neg_integer()}
  @type opts :: [flags: flags()]

  @doc """
  Reads the result of a server-side expression into `bin_name`.

  The optional `:flags` value accepts `:default`, `:eval_no_fail`, or a list
  of those atoms. Compatibility callers may pass a non-negative integer; use
  `{:raw, integer}` when deliberately sending an unnamed server value.

      Aerospike.Op.Exp.read("projected", Aerospike.Exp.int_bin("count"))
  """
  @spec read(String.t() | atom(), Exp.t(), opts()) :: t()
  def read(bin_name, %Exp{} = expression, opts \\ []) do
    case Operation.exp_read(
           normalize_bin_name(bin_name),
           expression,
           read_flags(opts)
         ) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end

  @doc """
  Writes the result of a server-side expression to `bin_name`.

  The optional `:flags` value accepts `:default`, `:create_only`,
  `:update_only`, `:allow_delete`, `:policy_no_fail`, `:eval_no_fail`, or a
  list of those atoms. Compatibility callers may pass a non-negative integer;
  use `{:raw, integer}` when deliberately sending an unnamed server value.

      Aerospike.Op.Exp.write("computed", Aerospike.Exp.int(99))
  """
  @spec write(String.t() | atom(), Exp.t(), opts()) :: t()
  def write(bin_name, %Exp{} = expression, opts \\ []) do
    case Operation.exp_modify(
           normalize_bin_name(bin_name),
           expression,
           write_flags(opts)
         ) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end

  defp read_flags(opts),
    do: opts |> Keyword.get(:flags, :default) |> PolicyInteger.exp_read_flags()

  defp write_flags(opts),
    do: opts |> Keyword.get(:flags, :default) |> PolicyInteger.exp_write_flags()

  defp normalize_bin_name(bin_name) when is_atom(bin_name), do: Atom.to_string(bin_name)
  defp normalize_bin_name(bin_name), do: bin_name
end
