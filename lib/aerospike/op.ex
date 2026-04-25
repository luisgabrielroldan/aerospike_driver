defmodule Aerospike.Op do
  @moduledoc """
  Primitive record operations for `Aerospike.operate/4`.

  See also `Aerospike.Op.Bit`, `Aerospike.Op.Exp`, `Aerospike.Op.HLL`,
  `Aerospike.Op.List`, and `Aerospike.Op.Map` for collection and expression
  operations.
  """

  alias Aerospike.Error
  alias Aerospike.Protocol.AsmMsg.Operation

  @typedoc "Opaque wire operation."
  @opaque t :: Operation.t()

  defp normalize_bin_name(bin_name) when is_atom(bin_name), do: Atom.to_string(bin_name)
  defp normalize_bin_name(bin_name), do: bin_name

  @spec put(String.t() | atom(), term()) :: t()
  def put(bin_name, value) do
    case Operation.write(normalize_bin_name(bin_name), value) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end

  @spec get(String.t() | atom()) :: t()
  def get(bin_name) do
    Operation.read(normalize_bin_name(bin_name))
  end

  @spec get_header() :: t()
  def get_header do
    %Operation{
      op_type: Operation.op_read(),
      particle_type: Operation.particle_null(),
      bin_name: "",
      data: <<>>,
      read_header: true
    }
  end

  @spec add(String.t() | atom(), integer() | float()) :: t()
  def add(bin_name, delta) do
    case Operation.add(normalize_bin_name(bin_name), delta) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end

  @spec append(String.t() | atom(), String.t()) :: t()
  def append(bin_name, suffix) do
    case Operation.append(normalize_bin_name(bin_name), suffix) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end

  @spec prepend(String.t() | atom(), String.t()) :: t()
  def prepend(bin_name, prefix) do
    case Operation.prepend(normalize_bin_name(bin_name), prefix) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end
end
