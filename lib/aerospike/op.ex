defmodule Aerospike.Op do
  @moduledoc """
  Primitive record operations for use with `Aerospike.operate/4`.

  Each function returns an opaque operation value (`t/0`) representing a single
  step in an atomic multi-operation command. Compose them in a list:

  ```elixir
  import Aerospike.Op

  {:ok, record} =
    MyApp.Repo.operate(key, [
      put("status", "premium"),
      add("login_count", 1),
      get("login_count")
    ])
  ```

  For **collection data types** (list, map, bit, HyperLogLog), see `Aerospike.Op.List`,
  `Aerospike.Op.Map`, `Aerospike.Op.Bit`, and `Aerospike.Op.HLL`. Nested CDT paths use
  `Aerospike.Ctx`.

  ## Related

  - `MyApp.Repo.operate/2,3` — recommended application-facing execution path
  - `Aerospike.operate/4` — low-level atomic execution on one record
  - `Aerospike.Record` — result type containing `bins`, `generation`, and `ttl`
  """

  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value

  @typedoc "Opaque wire operation; do not pattern-match on the struct fields."
  @opaque t :: Operation.t()

  @doc """
  Writes a bin value (same semantics as a single-bin `put`).
  """
  @spec put(String.t(), term()) :: t()
  def put(bin_name, value) when is_binary(bin_name) do
    {pt, data} = Value.encode_value(value)

    %Operation{
      op_type: Operation.op_write(),
      particle_type: pt,
      bin_name: bin_name,
      data: data
    }
  end

  @doc """
  Reads a single bin by name.
  """
  @spec get(String.t()) :: t()
  def get(bin_name) when is_binary(bin_name), do: Operation.read(bin_name)

  @doc """
  Reads record metadata (generation and TTL) without bin values.

  Combine with write operations to fetch the new generation after an update.
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
  Atomically adds `delta` to an integer or float bin.
  """
  @spec add(String.t(), integer() | float()) :: t()
  def add(bin_name, delta) when is_binary(bin_name) and (is_integer(delta) or is_float(delta)),
    do: Operation.add(bin_name, delta)

  @doc """
  Appends UTF-8 bytes to an existing string bin.
  """
  @spec append(String.t(), String.t()) :: t()
  def append(bin_name, suffix) when is_binary(bin_name) and is_binary(suffix),
    do: Operation.append(bin_name, suffix)

  @doc """
  Prepends UTF-8 bytes to an existing string bin.
  """
  @spec prepend(String.t(), String.t()) :: t()
  def prepend(bin_name, prefix) when is_binary(bin_name) and is_binary(prefix),
    do: Operation.prepend(bin_name, prefix)
end
