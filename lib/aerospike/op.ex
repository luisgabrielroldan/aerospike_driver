defmodule Aerospike.Op do
  @moduledoc """
  Primitive record operations for `Aerospike.operate/4`.

  These builders create the simple bin operations used by the server's
  operate command: write a bin, read a bin, read only record metadata,
  increment a numeric bin, and append or prepend string data. Use the CDT
  modules for collection bins and `Aerospike.Op.Exp` for expression-backed
  operate calls.

      key = Aerospike.key("test", "users", "user:1")

      {:ok, record} =
        Aerospike.operate(cluster, key, [
          Aerospike.Op.add("login_count", 1),
          Aerospike.Op.put("last_seen", "2026-04-27"),
          Aerospike.Op.get("login_count")
        ])

  Read operations return data in the `%Aerospike.Record{}` returned by
  `Aerospike.operate/4`; write operations affect the record on the server and
  only return data when the server operation itself produces a result.

  See also `Aerospike.Op.Bit`, `Aerospike.Op.Exp`, `Aerospike.Op.HLL`,
  `Aerospike.Op.List`, and `Aerospike.Op.Map` for collection and expression
  operations.
  """

  alias Aerospike.Error
  alias Aerospike.Protocol.AsmMsg.Operation

  @typedoc "Opaque wire operation."
  @opaque t :: Operation.t()

  @typedoc """
  Bin name accepted by primitive operate builders.

  Atom bin names are converted to strings before the operation is encoded.
  """
  @type bin_name :: String.t() | atom()

  defp normalize_bin_name(bin_name) when is_atom(bin_name), do: Atom.to_string(bin_name)
  defp normalize_bin_name(bin_name), do: bin_name

  @doc """
  Writes `value` to `bin_name`.

  The value must be encodable by the Aerospike particle encoder. Atom bin names
  are converted to strings.
  """
  @spec put(bin_name(), term()) :: t()
  def put(bin_name, value) do
    case Operation.write(normalize_bin_name(bin_name), value) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end

  @doc """
  Reads `bin_name` from the record.

  The returned operation projects the requested bin into the operate response.
  """
  @spec get(bin_name()) :: t()
  def get(bin_name) do
    Operation.read(normalize_bin_name(bin_name))
  end

  @doc """
  Reads only record metadata.

  This operation asks the server for the record generation and TTL without
  returning bin data.
  """
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

  @doc """
  Adds `delta` to a numeric bin.

  The server creates the bin when needed and returns an error if the existing
  value is not numeric.
  """
  @spec add(bin_name(), integer() | float()) :: t()
  def add(bin_name, delta) do
    case Operation.add(normalize_bin_name(bin_name), delta) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end

  @doc """
  Appends `suffix` to a string bin.
  """
  @spec append(bin_name(), String.t()) :: t()
  def append(bin_name, suffix) do
    case Operation.append(normalize_bin_name(bin_name), suffix) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end

  @doc """
  Prepends `prefix` to a string bin.
  """
  @spec prepend(bin_name(), String.t()) :: t()
  def prepend(bin_name, prefix) do
    case Operation.prepend(normalize_bin_name(bin_name), prefix) do
      {:ok, op} -> op
      {:error, %Error{} = err} -> raise ArgumentError, err.message
    end
  end
end
