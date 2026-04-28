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
  alias Aerospike.Protocol.AsmMsg.Operation

  @typedoc """
  Opaque expression operation for `Aerospike.operate/4`.
  """
  @opaque t :: Aerospike.Op.t()

  @typedoc """
  Options accepted by expression operate builders.

  Supported key:

  * `:flags` - raw server expression read/write flags integer. Defaults to `0`.
  """
  @type opts :: [flags: non_neg_integer()]

  @doc """
  Reads the result of a server-side expression into `bin_name`.

  The optional `:flags` value is the expression read-flags integer
  (`ExpReadFlags` in the official clients), used for server-side expression
  evaluation behavior such as no-fail reads.

      Aerospike.Op.Exp.read("projected", Aerospike.Exp.int_bin("count"))
  """
  @spec read(String.t() | atom(), Exp.t(), opts()) :: t()
  def read(bin_name, %Exp{} = expression, opts \\ []) do
    case Operation.exp_read(
           normalize_bin_name(bin_name),
           expression,
           Keyword.get(opts, :flags, 0)
         ) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end

  @doc """
  Writes the result of a server-side expression to `bin_name`.

  The optional `:flags` value is the expression write-flags integer
  (`ExpWriteFlags` in the official clients), used for server-side expression
  write behavior such as create-only, update-only, allow-delete, and policy
  no-fail.

      Aerospike.Op.Exp.write("computed", Aerospike.Exp.int(99))
  """
  @spec write(String.t() | atom(), Exp.t(), opts()) :: t()
  def write(bin_name, %Exp{} = expression, opts \\ []) do
    case Operation.exp_modify(
           normalize_bin_name(bin_name),
           expression,
           Keyword.get(opts, :flags, 0)
         ) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end

  defp normalize_bin_name(bin_name) when is_atom(bin_name), do: Atom.to_string(bin_name)
  defp normalize_bin_name(bin_name), do: bin_name
end
