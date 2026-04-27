defmodule Aerospike.Batch do
  @moduledoc """
  Constructors for heterogeneous `Aerospike.batch_operate/3` requests.

  Each constructor returns an opaque batch entry value. Pass a list of entries
  to `Aerospike.batch_operate/3` to execute reads, writes, deletes, operations,
  and record UDF calls in one batch request. Results are returned as
  `%Aerospike.BatchResult{}` values in the same order as the input entries.

  The current public surface intentionally keeps batch policy narrow. Per-entry
  write policies are not exposed; `Aerospike.batch_operate/3` currently accepts
  only the batch-level `:timeout` option.
  """

  alias Aerospike.Key
  alias Aerospike.Record

  defmodule Read do
    @moduledoc "Batch read entry built by `Aerospike.Batch.read/1`."

    @enforce_keys [:key]
    defstruct [:key]

    @typedoc "Batch read entry targeting one key."
    @type t :: %__MODULE__{key: Key.t()}
  end

  defmodule Put do
    @moduledoc "Batch put entry built by `Aerospike.Batch.put/2`."

    @enforce_keys [:key, :bins]
    defstruct [:key, :bins]

    @typedoc "Batch put entry with bins to write for one key."
    @type t :: %__MODULE__{key: Key.t(), bins: Record.bins_input()}
  end

  defmodule Delete do
    @moduledoc "Batch delete entry built by `Aerospike.Batch.delete/1`."

    @enforce_keys [:key]
    defstruct [:key]

    @typedoc "Batch delete entry targeting one key."
    @type t :: %__MODULE__{key: Key.t()}
  end

  defmodule Operate do
    @moduledoc "Batch operate entry built by `Aerospike.Batch.operate/2`."

    @enforce_keys [:key, :operations]
    defstruct [:key, :operations]

    @typedoc "Batch operate entry with record operations for one key."
    @type t :: %__MODULE__{key: Key.t(), operations: [Aerospike.Op.t()]}
  end

  defmodule UDF do
    @moduledoc "Batch record-UDF entry built by `Aerospike.Batch.udf/4`."

    @enforce_keys [:key, :package, :function, :args]
    defstruct [:key, :package, :function, :args]

    @typedoc "Batch record-UDF entry targeting one key."
    @type t :: %__MODULE__{
            key: Key.t(),
            package: String.t(),
            function: String.t(),
            args: list()
          }
  end

  @typedoc """
  One heterogeneous batch entry for `Aerospike.batch_operate/3`.
  """
  @type t :: Read.t() | Put.t() | Delete.t() | Operate.t() | UDF.t()

  @doc """
  Returns the key targeted by a batch entry.
  """
  @spec key(t()) :: Key.t()
  def key(%Read{key: key}), do: key
  def key(%Put{key: key}), do: key
  def key(%Delete{key: key}), do: key
  def key(%Operate{key: key}), do: key
  def key(%UDF{key: key}), do: key

  @doc """
  Builds a full-record read entry.

  Accepts `%Aerospike.Key{}` values or `{namespace, set, user_key}` tuples.
  """
  @spec read(Key.key_input()) :: Read.t()
  def read(key), do: %Read{key: Key.coerce!(key)}

  @doc """
  Builds a put entry from a non-empty bin map.

  Bin names may be atoms or strings. Values follow the same supported value
  subset as `Aerospike.put/4`.
  """
  @spec put(Key.key_input(), Record.bins_input()) :: Put.t()
  def put(key, bins) when is_map(bins), do: %Put{key: Key.coerce!(key), bins: bins}

  @doc """
  Builds a delete entry.
  """
  @spec delete(Key.key_input()) :: Delete.t()
  def delete(key), do: %Delete{key: Key.coerce!(key)}

  @doc """
  Builds an operate entry from `Aerospike.Op` operations.
  """
  @spec operate(Key.key_input(), [Aerospike.Op.t()]) :: Operate.t()
  def operate(key, operations) when is_list(operations) do
    %Operate{key: Key.coerce!(key), operations: operations}
  end

  @doc """
  Builds a record-UDF entry.
  """
  @spec udf(Key.key_input(), String.t(), String.t()) :: UDF.t()
  @spec udf(Key.key_input(), String.t(), String.t(), list()) :: UDF.t()
  def udf(key, package, function, args \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) do
    %UDF{key: Key.coerce!(key), package: package, function: function, args: args}
  end
end
